"""Domain classes for store metadata.

Converted data is stored in chunked datastores (e.g. Zarr, Icechunk) enabling
subselection across any dimension of data, provided it is chunked appropriately.

This module provides an abstract `TensorStore` base holding the backend-agnostic
business logic, and concrete implementations for specific storage engines.

TODO: 2024-11-20 This module wants refactoring into smaller testable components.
"""

import abc
import dataclasses
import datetime as dt
import logging
import os
import pathlib
import shutil
from typing import Any, ClassVar

import icechunk
import numpy as np
import numpy.typing
import pandas as pd
import xarray as xr
import zarr
import zarr.abc.store
import zarr.storage
from returns.result import Failure, ResultE, Success

from .coordinates import NWPDimensionCoordinateMap
from .parameters import Parameter
from .postprocess import PostProcessOptions

log = logging.getLogger("nwp-consumer")


@dataclasses.dataclass(slots=True)
class ParameterScanResult:
    """Container for the results of a scan of a parameter's values."""

    mean: float
    """The mean value of the parameter's data."""
    is_valid: bool
    """Whether the parameter's data values are valid.

    This is determined according to the parameter's limits and threshold.
    See `entities.parameters.Parameter`.
    """
    has_nulls: bool
    """Whether the parameter's data contains null values."""


@dataclasses.dataclass(slots=True)
class TensorStore(abc.ABC):
    """Abstract store class for multidimensional data.

    Each store instance has defined coordinates for the data,
    and is capable of handling parallel, region-based updates.

    This base class holds the backend-agnostic business logic (validation,
    region writing, scanning). Backend-specific persistence is defined by the
    abstract methods, which concrete subclasses (e.g. `ZarrTensorStore`)
    implement for their particular storage engine.

    Writes are grouped into transactions: `begin_transaction` opens one,
    `write_to_region` writes into it, and `commit` or `rollback` closes it.
    Backends that support it (e.g. `IcechunkTensorStore`) make the transaction's
    writes visible to readers atomically on commit.
    """

    name: str
    """Identifier for the store and the data within."""

    path: str
    """The path to the store."""

    coordinate_map: NWPDimensionCoordinateMap
    """The coordinates of the store."""

    size_kb: int
    """The size of the data written to the store by this instance in kilobytes."""

    encoding: dict[str, Any]
    """The encoding passed to the storage backend whilst writing."""

    created: bool
    """Whether the store was created by this instance, as opposed to already existing.

    Stores that already existed may hold data written by other runs,
    so they must not be deleted in response to a failure in this one.
    """

    monthly: ClassVar[bool] = False
    """Whether each store holds a whole month of init times.

    Monthly stores are written to by many runs, each adding init times, which
    requires transactions. Otherwise, each store holds only the init times of
    the run that creates it.
    """

    # --- Abstract backend-specific methods --- #
    @classmethod
    @abc.abstractmethod
    def initialize_empty_store(
        cls,
        model: str,
        repository: str,
        coords: NWPDimensionCoordinateMap,
        chunks: dict[str, int],
    ) -> ResultE["TensorStore"]:
        """Initialize a store for a given init time.

        This method writes a blank dataarray to disk based on the input coordinates,
        which define the dimension labels and tick values of the output dataset object.

        .. note: If a store already exists at the expected path,
           it is checked for consistency with the input coordinates and used if valid.

        The dataarray is 'blank' because only its metadata is written; this enables
        region-based writing of new data to the store, which further allows for
        parallel write processes.

        There is a gotcha: regional writes can never be done in parallel to the same chunk,
        so writes must always be done at the chunk level or higher (as a chunk is an
        individual file in the store). To this effect, chunks are chosen to cover as small
        a unit of data as could reasonably be expected to be provided by an NWP source:

        - Raw data files may not contain the full grid of data, hence a chunk of size equal
          to a quarter the length of the grid dimension (lat/lon/x/y axes) is used.
        - Raw data files may contain as little as one step for a single parameter, so a chunk
          size of 1 is used along the step dimension.
        - As above for the init_time dimension.

        Args:
            model: The name of the model providing the tensor data.
                   This is also used as the name of the tensor.
            repository: The name of the repository providing the tensor data.
            coords: The coordinates of the store.
            chunks: The chunk sizes for the store.

        Returns:
            A new instance of a TensorStore subclass.

        See Also:
            - https://docs.xarray.dev/en/stable/user-guide/io.html#appending-to-existing-zarr-stores
            - https://docs.xarray.dev/en/stable/user-guide/io.html#distributed-writes
        """
        pass

    @abc.abstractmethod
    def _open_dataarray(self) -> xr.DataArray:
        """Open the underlying store as an xarray DataArray.

        Concrete implementations return a read view of the store data,
        used by the shared business logic methods below. The view includes
        the uncommitted writes of the open transaction, if there is one.
        """
        pass

    @abc.abstractmethod
    def begin_transaction(self) -> ResultE[None]:
        """Open a transaction for subsequent writes to the store."""
        pass

    @abc.abstractmethod
    def commit(self, message: str) -> ResultE[None]:
        """Commit the writes of the open transaction, closing it.

        Args:
            message: A description of the changes made in the transaction.
        """
        pass

    @abc.abstractmethod
    def rollback(self) -> ResultE[None]:
        """Undo the uncommitted writes made by this instance.

        Closes the open transaction, if there is one.
        """
        pass

    @abc.abstractmethod
    def _write_region_data(self, da: xr.DataArray, region: dict[str, slice]) -> None:
        """Persist a dataarray to a region of the store.

        Implementations should raise on failure; the shared `write_to_region`
        wraps this call and translates exceptions into a `Failure`.
        """
        pass

    @abc.abstractmethod
    def update_attrs(self, attrs: dict[str, str]) -> ResultE[str]:
        """Update the attributes of the store with the given dictionary."""
        pass

    @abc.abstractmethod
    def delete_store(self) -> ResultE[None]:
        """Delete the store."""
        pass

    # --- Business logic methods --- #
    @staticmethod
    def _existing_coordinate_map(
        path: str,
        store_da: xr.DataArray,
        coords: NWPDimensionCoordinateMap,
    ) -> ResultE[NWPDimensionCoordinateMap]:
        """Get the coordinates of an existing store, checking they cover the input coords.

        The store's own coordinates are returned rather than the input coordinates,
        so that regions are determined relative to the full extent of the store.
        """
        coordinate_map_result = NWPDimensionCoordinateMap.from_xarray(store_da)
        if isinstance(coordinate_map_result, Failure):
            return Failure(
                OSError(
                    f"Error reading coordinates of existing store at '{path}' "
                    f"(possible corruption): {coordinate_map_result}",
                ),
            )
        store_coords = coordinate_map_result.unwrap()
        region_result = store_coords.determine_region(inner=coords)
        if isinstance(region_result, Failure):
            return Failure(
                OSError(
                    f"Existing store at '{path}' does not cover the requested coordinates. "
                    "Delete the existing store or move it to a new location, "
                    "or choose a new location for the new store via ZARRDIR. "
                    f"Error context: {region_result.failure()}",
                ),
            )
        return Success(store_coords)

    def _select_init_times(
        self,
        store_da: xr.DataArray,
        init_times: list[dt.datetime],
    ) -> ResultE[xr.DataArray]:
        """Select the given init times from the store's dataarray."""
        unknown_its = [it for it in init_times if it not in self.coordinate_map.init_time]
        if len(unknown_its) > 0:
            return Failure(
                KeyError(
                    f"Init times {unknown_its} are not present in store at '{self.path}'. "
                    f"Store init times: {self.coordinate_map.init_time}.",
                ),
            )
        return Success(
            store_da.sel(
                init_time=[
                    np.datetime64(it.astimezone(dt.UTC).replace(tzinfo=None), "ns")
                    for it in init_times
                ],
            ),
        )

    def write_to_region(
        self,
        da: xr.DataArray,
        region: dict[str, slice] | None = None,
    ) -> ResultE[int]:
        """Write partial data to the store.

        The optional region is a dictionary which maps dimension labels to slices.
        These define the region in the store to write to.

        If the region dict is empty or not provided, the region is determined
        via the 'determine_region' method.

        This function should be thread safe, so a check is performed on the region
        to ensure that it can be safely written to in parallel, i.e. that it covers
        an integer number of chunks.

        Args:
            da: The data to write to the store.
            region: The region to write to.

        Returns:
            An indicator of a successful store write containing the number of bytes written.

        See Also:
            - https://docs.xarray.dev/en/stable/user-guide/io.html#distributed-writes
        """
        # Attempt to determine the region if missing
        if region is None or region == {}:
            region_result = NWPDimensionCoordinateMap.from_xarray(da).bind(
                self.coordinate_map.determine_region,
            )
            if isinstance(region_result, Failure):
                return region_result
            region = region_result.unwrap()

        # For each dimensional slice defining the region, check the slice represents an
        # integer number of chunks along that dimension.
        # * This is to ensure that the data can safely be written in parallel.
        # * The start and and of each slice should be divisible by the chunk size.
        chunksizes = self._open_dataarray().chunksizes
        for dim, slc in region.items():
            chunk_size = chunksizes.get(dim, (1,))[0]
            # TODO: Determine if this should return a full failure object
            if slc.start % chunk_size != 0 or slc.stop % chunk_size != 0:
                log.warning(
                    f"Determined region of raw data to be written for dimension '{dim}'"
                    f"does not align with chunk boundaries of the store. "
                    f"Dimension '{dim}' has a chunk size of {chunk_size}, "
                    "but the data to be written for this dimension starts at chunk "
                    f"{slc.start / chunk_size:.2f} (index {slc.start}) and ends at chunk "
                    f"{slc.stop / chunk_size:.2f} (index {slc.stop}). "
                    "As such, this region cannot be safely written in parallel. "
                    "Ensure the chunking is granular enough to cover the raw data region.",
                )

        # Perform the regional write
        try:
            self._write_region_data(da=da, region=region)
        except Exception as e:
            return Failure(
                OSError(
                    f"Error writing to region of store: {e}",
                ),
            )

        # Calculate the number of bytes written
        nbytes: int = da.nbytes
        del da
        self.size_kb += nbytes // 1024
        return Success(nbytes)

    @staticmethod
    def _has_nans(store_da: xr.DataArray) -> ResultE[bool]:
        """Check the store for NaN values."""
        # I don't like having environ calls buried like this - needs refactoring to the top
        nans_in_image_threshold: float = float(os.getenv("ALLOWED_NAN_PERCENTAGE", "0.05"))
        images_failing_nan_check_threshold: float = float(
            os.getenv("ALLOWED_VALIDATION_FAILURE_PERCENTAGE", "0.02"),
        )

        def _calc_null_percentage(data: np.typing.NDArray[np.float32]) -> float:
            nulls = np.isnan(data)
            if 0 in data.shape:
                log.warning(
                    "Validation region has 0 area, check input slices correspond"
                    "to coordinate values in the dataset",
                )
                return 1.0
            return float(nulls.sum() / np.prod(nulls.shape))

        if "latitude" in store_da.dims:
            spatial_dims: list[str] = ["latitude", "longitude"]
        elif "x_osgb" in store_da.dims:
            spatial_dims = ["x_osgb", "y_osgb"]
        elif "x_laea" in store_da.dims:
            spatial_dims = ["x_laea", "y_laea"]
        else:
            return Failure(
                ValueError(
                    "Store does not have expected spatial dimensions. "
                    "Expected: ['latitude', 'longitude'], ['x_osgb', 'y_osgb'], "
                    "['x_laea', 'y_laea']. Got: {store_da.dims}.",
                ),
            )

        result = xr.apply_ufunc(
            _calc_null_percentage,
            store_da,
            input_core_dims=[spatial_dims],
            vectorize=True,
            dask="parallelized",
        )

        failed_image_count: int = (result > nans_in_image_threshold).sum().values
        total_image_count: int = result.size
        failed_image_percentage: float = failed_image_count / total_image_count
        if failed_image_percentage > images_failing_nan_check_threshold:
            log.warning(
                f"Dataset failed validation. "
                f"{failed_image_percentage:.2%} of images have greater than "
                f"{int(nans_in_image_threshold * 100)}% null values"
                f"({failed_image_count}/{total_image_count})",
            )
            return Success(True)
        log.info(
            f"{failed_image_count}/{total_image_count} "
            f"({failed_image_percentage:.2%}) of images have greater than "
            f"{int(nans_in_image_threshold * 100)}% null values",
        )
        return Success(False)

    def validate_store(self, init_times: list[dt.datetime]) -> ResultE[None]:
        """Validate the data written to the store for the given init times.

        Only the incoming init times are checked, so that the completeness of the
        newly written data is validated independently of the rest of the store
        (which may legitimately be missing data, e.g. future init times).

        Args:
            init_times: The init times written to the store that should be validated.

        Returns:
            A bool indicating the result of the validation.
        """
        log.debug(f"Validating init times {init_times} in store at '{self.path}'")
        select_result = self._select_init_times(
            store_da=self._open_dataarray(),
            init_times=init_times,
        )
        if isinstance(select_result, Failure):
            return select_result
        store_da: xr.DataArray = select_result.unwrap()

        # Check for NaNs
        has_nans_result = self._has_nans(store_da=store_da)
        if isinstance(has_nans_result, Failure):
            return has_nans_result
        else:
            if has_nans_result.unwrap():
                return Failure(
                    ValueError(
                        "Store contains NaN values. "
                        "Check the source data for missing values and reprocess the data.",
                    ),
                )

        # TODO: Use consistency checks instead
        """
        # Consistency check on the coordinates of the store
        coords_result = NWPDimensionCoordinateMap.from_xarray(store_da)
        match coords_result:
            case Failure(e):
                return Failure(e)
            case Success(coords):
                if coords != self.coordinate_map:
                    return Failure(ValueError(
                        "Coordinate consistency check failed: "
                        "Store coordinates do not match expected coordinates. "
                        f"Expected: {self.coordinate_map}. Got: {coords}.",
                    ))

        # Validity check on the parameters of the store
        for param in self.coordinate_map.variable:
            scan_result: ResultE[ParameterScanResult] = self.scan_parameter_values(p=param)
            match scan_result:
                case Failure(e):
                    return Failure(e)
                case Success(scan):
                    log.debug(f"Scanned parameter {param.name}: {scan.__repr__()}")
                    if not scan.is_valid or scan.has_nulls:
                        return Failure(ValueError("Parameter validation failed."))
        """

        return Success(None)

    def scan_parameter_values(self, p: Parameter) -> ResultE[ParameterScanResult]:
        """Scan the values of a parameter in the store.

        Extracts data from the values of the given parameter in the store.
        This reads the data from the store, so note that this can be an
        expensive operation for large datasets.

        Args:
            p: The name of the parameter to scan.

        Returns:
            A ParameterScanResult object.
        """
        if p not in self.coordinate_map.variable:
            return Failure(
                KeyError(
                    "Parameter scan failed: "
                    f"Cannot validate unknown parameter: {p.name}. "
                    "Ensure the parameter has been renamed to match the entities "
                    "parameters defined in `entities.parameters` if desired, or "
                    "add the parameter to the entities parameters if it is new. "
                    f"Store parameters: {[p.name for p in self.coordinate_map.variable]}.",
                ),
            )
        store_da: xr.DataArray = self._open_dataarray()

        # Calculating the mean of a dataarray returns another dataarray, so it
        # must be converted to a numpy array via `values`. Even though it is a
        # single number in this case, type checkers don't know that, so the
        # second call to `mean()` helps to reassure them its a float.
        mean = store_da.mean().values.mean()

        return Success(
            ParameterScanResult(
                mean=mean,
                is_valid=True,
                has_nulls=False,
            ),
        )

    def postprocess(self, options: PostProcessOptions) -> ResultE[str]:
        """Post-process the store.

        This creates a new store, as many of the postprocess options require
        modifications to the underlying file structure of the store.
        """
        # TODO: Implement postprocessing options
        if options.requires_postprocessing():
            log.info("Applying postprocessing options to store %s", self.name)

            if options.validate:
                log.warning("Validation not yet implemented in efficient manner. Skipping option.")

            log.debug("Postprocessing complete for store %s", self.name)
            return Success(self.path)

        else:
            return Success(self.path)

    def missing_times(
        self,
        init_times: list[dt.datetime] | None = None,
    ) -> ResultE[list[dt.datetime]]:
        """Find the missing init_time in the store.

        A "missing init_time" is determined by the values corresponding
        to the first two coordinate values of each dimension: if all are
        NaN or None values then the time is considered missing.

        Args:
            init_times: The init times to check. Defaults to all init times in the store.
        """
        try:
            store_da: xr.DataArray = self._open_dataarray()
        except Exception as e:
            return Failure(
                OSError(
                    "Cannot determine missing times in store due to "
                    f"error reading '{self.path}': {e}",
                ),
            )
        if init_times is not None:
            select_result = self._select_init_times(store_da=store_da, init_times=init_times)
            if isinstance(select_result, Failure):
                return select_result
            store_da = select_result.unwrap()
        missing_times: list[dt.datetime] = []
        for it in store_da.coords["init_time"].values:
            if (
                store_da.sel(init_time=it)
                .isel({d: slice(0, 2) for d in self.coordinate_map.dims if d != "init_time"})
                .isnull()
                .all()
                .values
            ):
                missing_times.append(pd.Timestamp(it).to_pydatetime().replace(tzinfo=dt.UTC))
        if len(missing_times) > 0:
            log.debug(
                f"NaNs in init times '{missing_times}' suggest they are missing, "
                f"will redownload",
            )
        return Success(missing_times)

    @staticmethod
    def gen_store_filename(coords: NWPDimensionCoordinateMap, extension: str = ".zarr") -> str:
        """Create a filename for the store.

        If the store only covers a single init_time, the filename is the init time.
        Else, if it covers multiple init_times, the filename is the range of init times.
        """
        store_range: str = coords.init_time[0].strftime("%Y%m%d%H")
        if len(coords.init_time) > 1:
            store_range = f"{coords.init_time[0]:%Y%m%d%H}-{coords.init_time[-1]:%Y%m%d%H}"

        return store_range + extension


@dataclasses.dataclass(slots=True)
class ZarrTensorStore(TensorStore):
    """A `TensorStore` backed by a Zarr store (local directory or S3)."""

    @classmethod
    def initialize_empty_store(
        cls,
        model: str,
        repository: str,
        coords: NWPDimensionCoordinateMap,
        chunks: dict[str, int],
    ) -> ResultE["TensorStore"]:
        """Initialize a blank Zarr store from the input coordinates.

        See `TensorStore.initialize_empty_store` for the interface contract.
        """
        if not isinstance(coords.init_time, list) or len(coords.init_time) == 0:
            return Failure(
                ValueError(
                    "Cannot initialize store with 'init_time' dimension coordinates not "
                    "specified via a populated list. Check instantiation of "
                    "NWPDimensionCoordinateMap passed to this function. "
                    f"Got: {coords.init_time} (not a list, or empty).",
                ),
            )

        zarrdir = os.getenv("ZARRDIR", f"~/.local/cache/nwp/{repository}/{model}/data")
        store: zarr.abc.store.Store
        path: str
        filename: str = cls.gen_store_filename(coords=coords)
        try:
            if zarrdir.startswith("s3"):
                store_result = cls._create_zarrstore_s3(zarrdir, filename)
                store, path = store_result.unwrap()
            else:
                path = pathlib.Path("/".join((zarrdir, filename))).expanduser().as_posix()
                store = zarr.storage.LocalStore(path)
        except Exception as e:
            return Failure(
                OSError(
                    f"Unable to create Local Store at dir '{zarrdir}'. "
                    "Ensure ZARRDIR environment variable is specified correctly. "
                    f"Error context: {e}",
                ),
            )

        # Write the coordinates to a skeleton Zarr store
        # * 'compute=False' enables only saving metadata
        # * 'mode="w-"' fails if it finds an existing store

        da: xr.DataArray = coords.as_zeroed_dataarray(name=model, chunks=chunks)
        encoding = {
            "init_time": {"units": "nanoseconds since 1970-01-01"},
            "step": {"units": "hours"},
        }
        try:
            _ = da.to_zarr(
                store=store,
                compute=False,
                mode="w-",
                consolidated=False,
                zarr_format=3,
                encoding=encoding,
            )
            log.info("Created blank zarr store at '%s'", path)
            # Ensure the store is readable
            store_da = xr.open_dataarray(path, engine="zarr", consolidated=False)
        except FileExistsError:
            store_da = xr.open_dataarray(path, engine="zarr", consolidated=False)
            if store_da.name != da.name:  # TODO: Also check for equality of coordinates
                return Failure(
                    OSError(
                        f"Existing store at '{path}' is for a different model. "
                        "Delete the existing store or move it to a new location, "
                        "or choose a new location for the new store via ZARRDIR.",
                    ),
                )
            existing_coords_result = cls._existing_coordinate_map(path, store_da, coords)
            if isinstance(existing_coords_result, Failure):
                return existing_coords_result
            log.info(f"Using existing store at '{path}'")
            return Success(
                cls(
                    name=model,
                    path=path,
                    coordinate_map=existing_coords_result.unwrap(),
                    size_kb=0,
                    encoding=encoding,
                    created=False,
                ),
            )
        except Exception as e:
            return Failure(
                OSError(
                    f"Failed writing blank store to '{path}': {e}",
                ),
            )

        # Check the resultant array's coordinates can be converted back
        coordinate_map_result = NWPDimensionCoordinateMap.from_xarray(store_da)
        if isinstance(coordinate_map_result, Failure):
            return Failure(
                OSError(
                    f"Error reading back coordinates of initialized store "
                    f"from path '{path}' (possible corruption): {coordinate_map_result}",
                ),
            )

        return Success(
            cls(
                name=model,
                path=path,
                coordinate_map=coordinate_map_result.unwrap(),
                size_kb=0,
                encoding=encoding,
                created=True,
            ),
        )

    def _open_dataarray(self) -> xr.DataArray:
        return xr.open_dataarray(self.path, engine="zarr", consolidated=False)

    def _write_region_data(self, da: xr.DataArray, region: dict[str, slice]) -> None:
        da.to_zarr(store=self.path, region=region, consolidated=False)

    def begin_transaction(self) -> ResultE[None]:
        """Open a transaction for subsequent writes to the store.

        Zarr has no transactions, so writes are visible as soon as they are made.
        """
        return Success(None)

    def commit(self, message: str) -> ResultE[None]:
        """Commit the writes of the open transaction.

        Zarr writes are already persisted, so there is nothing to commit.
        """
        log.debug(f"Committed to zarr store at '{self.path}': {message}")
        return Success(None)

    def rollback(self) -> ResultE[None]:
        """Undo the writes made by this instance.

        Zarr writes cannot be undone individually, so the store is deleted if it was
        created by this instance. Pre-existing stores may hold data written by other
        runs, so they are left in place, along with any partially written data.
        """
        if not self.created:
            log.warning(
                f"Leaving pre-existing zarr store at '{self.path}' in place on rollback. "
                "It may contain partially written data.",
            )
            return Success(None)
        return self.delete_store()

    def update_attrs(self, attrs: dict[str, str]) -> ResultE[str]:
        """Update the attributes of the store with the given dictionary."""
        group: zarr.Group = zarr.open_group(self.path)
        group.attrs.update(attrs)
        return Success(self.path)

    def delete_store(self) -> ResultE[None]:
        """Delete the store."""
        if self.path.startswith("s3://"):
            import s3fs

            try:
                fs = s3fs.S3FileSystem(
                    anon=False,
                    client_kwargs={
                        "region_name": os.getenv("AWS_REGION", "eu-west-1"),
                        "endpoint_url": os.getenv("AWS_ENDPOINT_URL", None),
                    },
                )
                fs.rm(self.path, recursive=True)
            except Exception as e:
                return Failure(
                    OSError(
                        f"Unable to delete S3 store at path '{self.path}'."
                        "Ensure AWS credentials are correct and discoverable by botocore. "
                        f"Error context: {e}",
                    ),
                )
        else:
            try:
                shutil.rmtree(self.path)
            except Exception as e:
                return Failure(
                    OSError(
                        f"Unable to delete store at path '{self.path}'. " f"Error context: {e}",
                    ),
                )
        log.info("Deleted zarr store at '%s'", self.path)
        return Success(None)

    @staticmethod
    def _create_zarrstore_s3(
        s3_folder: str,
        filename: str,
    ) -> ResultE[tuple[zarr.abc.store.Store, str]]:
        """Create a zarr store backed by S3.

        Authentication with S3 is done via botocore's credential discovery.

        Returns:
            A tuple containing the zarr store and the path to the store,
            in a result object indicating success or failure.

        See Also:
          - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials
        """
        import s3fs

        if not s3_folder.startswith("s3://"):
            return Failure(
                ValueError(
                    "S3 folder path must start with 's3://'. " f"Got: {s3_folder}",
                ),
            )
        log.debug("Attempting AWS connection using credential discovery")
        try:
            fs = s3fs.S3FileSystem(
                anon=False,
                asynchronous=True,
                client_kwargs={
                    "region_name": os.getenv("AWS_REGION", "eu-west-1"),
                    "endpoint_url": os.getenv("AWS_ENDPOINT_URL", None),
                },
            )
            path = s3_folder + "/" + filename
            key = path.removeprefix("s3://")
            store = zarr.storage.FsspecStore(fs, path=key)
        except Exception as e:
            return Failure(
                OSError(
                    f"Unable to create S3 store for path '{s3_folder}/{filename}'. "
                    "Ensure ZARRDIR environment variable is specified correctly, "
                    "and AWS credentials are discoverable by botocore. "
                    f"Error context: {e}",
                ),
            )
        return Success((store, path))


ICECHUNK_BRANCH = "main"
"""The branch of the Icechunk repository written to and read from."""

ICECHUNK_CONFIG = icechunk.RepositoryConfig(
    manifest=icechunk.ManifestConfig(
        splitting=icechunk.ManifestSplittingConfig.from_dict(
            {
                icechunk.ManifestSplitCondition.AnyArray(): {
                    icechunk.ManifestSplitDimCondition.DimensionName("^init_time$"): 1,
                },
            },
        ),
    ),
)
"""The configuration of the Icechunk repositories.

Manifests are split to cover a single chunk along the init_time dimension, which is
one init time with the default chunking. As each commit writes a single init time,
it then writes one small manifest, rather than rewriting one covering the month.
"""

ICECHUNK_GC_GRACE_PERIOD = dt.timedelta(days=1)
"""How long unreferenced objects are kept for before garbage collection deletes them.

The chunks written by a transaction are unreferenced until it is committed, so any
transaction must be committed within this period, or its chunks may be deleted.
"""


@dataclasses.dataclass(slots=True)
class IcechunkTensorStore(TensorStore):
    """A `TensorStore` backed by an Icechunk repository (local filesystem or S3).

    Icechunk is a transactional storage engine for Zarr: writes happen inside a
    session and are only visible once committed. Each transaction is a writable
    session on the ``main`` branch, so readers never see partially written data.
    As the consumer writes regions sequentially (see
    `services.ConsumerService._fold_dataarrays_generator`), no distributed
    session merging is required.

    Each store holds a month of init times, and is named for that month
    (e.g. ``2026-02.icechunk``). Many writers can add init times to the same
    store concurrently: commits are rebased onto each other unless they write
    to the same chunks.
    """

    monthly: ClassVar[bool] = True

    repo: icechunk.Repository
    """The `icechunk.Repository` backing the store."""

    session: icechunk.Session | None = None
    """The writable session of the open transaction, if there is one."""

    @classmethod
    def initialize_empty_store(
        cls,
        model: str,
        repository: str,
        coords: NWPDimensionCoordinateMap,
        chunks: dict[str, int],
    ) -> ResultE["TensorStore"]:
        """Initialize a blank Icechunk store from the input coordinates.

        The input coordinates must not span more than one month of init times.

        Several writers may initialize the same store at once (e.g. concurrent runs
        for different init times in the same month). Whichever commits the blank
        store first creates it, and the others use it as an existing store.

        See `TensorStore.initialize_empty_store` for the interface contract.
        """
        if not isinstance(coords.init_time, list) or len(coords.init_time) == 0:
            return Failure(
                ValueError(
                    "Cannot initialize store with 'init_time' dimension coordinates not "
                    "specified via a populated list. Check instantiation of "
                    "NWPDimensionCoordinateMap passed to this function. "
                    f"Got: {coords.init_time} (not a list, or empty).",
                ),
            )
        months = sorted({f"{it:%Y-%m}" for it in coords.init_time})
        if len(months) > 1:
            return Failure(
                ValueError(
                    "Cannot initialize icechunk store with init times spanning more than "
                    f"one month, as each store holds a single month. Got months: {months}.",
                ),
            )

        icdir = os.getenv("ZARRDIR", f"~/.local/cache/nwp/{repository}/{model}/data")
        filename: str = cls.gen_store_filename(coords=coords)
        try:
            storage, path = cls._create_icechunk_storage(icdir, filename)
            repo = cls._open_or_create_repository(storage)
        except Exception as e:
            return Failure(
                OSError(
                    f"Unable to open or create Icechunk repository at dir '{icdir}'. "
                    "Ensure ZARRDIR environment variable is specified correctly. "
                    f"Error context: {e}",
                ),
            )

        da: xr.DataArray = coords.as_zeroed_dataarray(name=model, chunks=chunks)
        encoding = {
            "init_time": {"units": "nanoseconds since 1970-01-01"},
            "step": {"units": "hours"},
        }

        # Reuse an existing store if the repository already holds this model's data.
        # * The check is made in the same session as the skeleton is written in, so a
        #   store created concurrently by another writer causes a conflict on commit
        session = repo.writable_session(ICECHUNK_BRANCH)
        existing_result = cls._open_existing_dataarray(session=session, name=model)
        if isinstance(existing_result, Success):
            return cls._from_existing_dataarray(
                store_da=existing_result.unwrap(),
                path=path,
                repo=repo,
                coords=coords,
                encoding=encoding,
            )

        # Write the coordinates to a skeleton store
        # * 'compute=False' enables only saving metadata
        # * 'consolidated=False' as Icechunk organizes metadata itself
        try:
            _ = da.to_zarr(
                session.store,
                compute=False,
                mode="w-",
                consolidated=False,
                encoding=encoding,
            )
            session.commit(f"Initialize empty store for {model}")
            log.info("Created blank icechunk store at '%s'", path)
        except icechunk.ConflictError:
            log.info(f"Store at '{path}' was created concurrently by another writer")
            existing_result = cls._open_existing_dataarray(
                session=repo.readonly_session(ICECHUNK_BRANCH),
                name=model,
            )
            if isinstance(existing_result, Failure):
                return Failure(
                    OSError(
                        f"Failed reading store at '{path}' created concurrently by "
                        f"another writer: {existing_result.failure()}",
                    ),
                )
            return cls._from_existing_dataarray(
                store_da=existing_result.unwrap(),
                path=path,
                repo=repo,
                coords=coords,
                encoding=encoding,
            )
        except Exception as e:
            return Failure(
                OSError(
                    f"Failed writing blank store to '{path}': {e}",
                ),
            )

        store_da = xr.open_zarr(
            repo.readonly_session(ICECHUNK_BRANCH).store,
            consolidated=False,
        )[model]
        coordinate_map_result = NWPDimensionCoordinateMap.from_xarray(store_da)
        if isinstance(coordinate_map_result, Failure):
            return Failure(
                OSError(
                    f"Error reading back coordinates of initialized store "
                    f"from path '{path}' (possible corruption): {coordinate_map_result}",
                ),
            )

        return Success(
            cls(
                name=model,
                path=path,
                coordinate_map=coordinate_map_result.unwrap(),
                size_kb=0,
                encoding=encoding,
                created=True,
                repo=repo,
            ),
        )

    def _open_dataarray(self) -> xr.DataArray:
        session = self.session or self.repo.readonly_session(ICECHUNK_BRANCH)
        return xr.open_zarr(session.store, consolidated=False)[self.name]

    def _write_region_data(self, da: xr.DataArray, region: dict[str, slice]) -> None:
        if self.session is None:
            raise RuntimeError(
                "No open transaction to write to. Call 'begin_transaction' first.",
            )
        da.to_zarr(self.session.store, region=region, consolidated=False)

    def begin_transaction(self) -> ResultE[None]:
        """Open a writable session on the branch for subsequent writes."""
        if self.session is not None:
            return Failure(
                RuntimeError(
                    f"Cannot begin transaction on store at '{self.path}': "
                    "a transaction is already open.",
                ),
            )
        try:
            self.session = self.repo.writable_session(ICECHUNK_BRANCH)
        except Exception as e:
            return Failure(
                OSError(
                    f"Unable to open writable session on store at '{self.path}': {e}",
                ),
            )
        return Success(None)

    def commit(self, message: str) -> ResultE[None]:
        """Commit the open session, making its writes visible to readers.

        If other writers have committed since the session was opened, the session is
        rebased onto their commits, unless they wrote to the same chunks.
        """
        if self.session is None:
            return Failure(
                RuntimeError(
                    f"Cannot commit to store at '{self.path}': no transaction is open.",
                ),
            )
        if not self.session.has_uncommitted_changes:
            log.info(f"No changes to commit to icechunk store at '{self.path}'")
            self.session = None
            return Success(None)
        try:
            snapshot_id = self.session.commit(message, rebase_with=icechunk.ConflictDetector())
        except icechunk.RebaseFailedError as e:
            return Failure(
                OSError(
                    f"Failed to commit to icechunk store at '{self.path}': another writer "
                    "has concurrently committed data to the same chunks of "
                    f"{sorted({c.path for c in e.conflicts})}.",
                ),
            )
        except Exception as e:
            return Failure(
                OSError(
                    f"Failed to commit to icechunk store at '{self.path}': {e}",
                ),
            )
        self.session = None
        log.info(f"Committed snapshot '{snapshot_id}' to '{self.path}': {message}")
        return Success(None)

    def rollback(self) -> ResultE[None]:
        """Discard the open session, if there is one.

        The discarded writes were never visible to readers. Any chunks they
        uploaded are unreferenced, and are removed by Icechunk's garbage collection.
        """
        if self.session is not None:
            self.session.discard_changes()
            self.session = None
            log.info(f"Discarded uncommitted changes to icechunk store at '{self.path}'")
        return Success(None)

    def update_attrs(self, attrs: dict[str, str]) -> ResultE[str]:
        """Update the attributes of the store with the given dictionary.

        If a transaction is open, the update is made within it. Otherwise,
        the update is committed immediately.
        """
        session = self.session or self.repo.writable_session(ICECHUNK_BRANCH)
        group: zarr.Group = zarr.open_group(session.store)
        group.attrs.update(attrs)
        if self.session is None:
            session.commit("Update attributes", rebase_with=icechunk.ConflictDetector())
        return Success(self.path)

    def delete_store(self) -> ResultE[None]:
        """Delete the store.

        Icechunk exposes no destructive delete of its underlying storage, so the
        backing files/objects are removed directly.
        """
        if self.path.startswith("s3://"):
            import s3fs

            try:
                fs = s3fs.S3FileSystem(
                    anon=False,
                    client_kwargs={
                        "region_name": os.getenv("AWS_REGION", "eu-west-1"),
                        "endpoint_url": os.getenv("AWS_ENDPOINT_URL", None),
                    },
                )
                fs.rm(self.path, recursive=True)
            except Exception as e:
                return Failure(
                    OSError(
                        f"Unable to delete S3 store at path '{self.path}'."
                        "Ensure AWS credentials are correct and discoverable by botocore. "
                        f"Error context: {e}",
                    ),
                )
        else:
            try:
                shutil.rmtree(self.path)
            except Exception as e:
                return Failure(
                    OSError(
                        f"Unable to delete store at path '{self.path}'. " f"Error context: {e}",
                    ),
                )
        log.info("Deleted icechunk store at '%s'", self.path)
        return Success(None)

    @classmethod
    def maintain(
        cls,
        model: str,
        repository: str,
        month: dt.date,
        expire_snapshots_older_than: dt.timedelta | None = None,
    ) -> ResultE[str]:
        """Delete unneeded objects from the store holding the given month.

        Objects not referenced by any snapshot, such as the chunks written by
        transactions that were rolled back, are deleted once they are older than
        `ICECHUNK_GC_GRACE_PERIOD`. The data in the store is never affected.

        Args:
            model: The name of the model providing the tensor data.
            repository: The name of the repository providing the tensor data.
            month: The month of the store to maintain.
            expire_snapshots_older_than: If set, snapshots older than this are first
                expired from the store's history, so that objects only they reference
                (e.g. overwritten data) are also deleted.

        Returns:
            The path to the maintained store.
        """
        icdir = os.getenv("ZARRDIR", f"~/.local/cache/nwp/{repository}/{model}/data")
        filename: str = cls._month_filename(month)
        try:
            storage, path = cls._create_icechunk_storage(icdir, filename)
            if not icechunk.Repository.exists(storage):
                return Failure(
                    FileNotFoundError(f"No icechunk store exists at '{path}' to maintain."),
                )
            repo = icechunk.Repository.open(storage, config=ICECHUNK_CONFIG)

            now = dt.datetime.now(tz=dt.UTC)
            if expire_snapshots_older_than is not None:
                expired = repo.expire_snapshots(older_than=now - expire_snapshots_older_than)
                log.info(f"Expired {len(expired)} snapshots from '{path}'")
            summary = repo.garbage_collect(
                delete_object_older_than=now - ICECHUNK_GC_GRACE_PERIOD,
            )
        except Exception as e:
            return Failure(
                OSError(
                    f"Failed to maintain icechunk store at '{icdir}/{filename}': {e}",
                ),
            )
        log.info(
            f"Deleted {summary.chunks_deleted} chunks, {summary.manifests_deleted} "
            f"manifests and {summary.snapshots_deleted} snapshots "
            f"({summary.bytes_deleted // 1024**2} MB) from '{path}'",
        )
        return Success(path)

    @staticmethod
    def _open_existing_dataarray(
        session: icechunk.Session,
        name: str,
    ) -> ResultE[xr.DataArray]:
        """Return the named dataarray from the session's snapshot, if present."""
        try:
            ds = xr.open_zarr(session.store, consolidated=False)
        except Exception as e:
            return Failure(e)
        if name not in ds.data_vars:
            return Failure(KeyError(name))
        return Success(ds[name])

    @classmethod
    def _from_existing_dataarray(
        cls,
        store_da: xr.DataArray,
        path: str,
        repo: icechunk.Repository,
        coords: NWPDimensionCoordinateMap,
        encoding: dict[str, Any],
    ) -> ResultE["TensorStore"]:
        """Create an instance for the existing store holding the given dataarray."""
        existing_coords_result = cls._existing_coordinate_map(path, store_da, coords)
        if isinstance(existing_coords_result, Failure):
            return existing_coords_result
        log.info(f"Using existing store at '{path}'")
        return Success(
            cls(
                name=str(store_da.name),
                path=path,
                coordinate_map=existing_coords_result.unwrap(),
                size_kb=0,
                encoding=encoding,
                created=False,
                repo=repo,
            ),
        )

    @staticmethod
    def _open_or_create_repository(storage: icechunk.Storage) -> icechunk.Repository:
        """Open the repository in the given storage, creating it if it doesn't exist.

        If another writer creates the repository concurrently, it is opened instead.
        """
        try:
            return icechunk.Repository.open_or_create(storage, config=ICECHUNK_CONFIG)
        except icechunk.IcechunkError:
            if not icechunk.Repository.exists(storage):
                raise
            log.info("Icechunk repository was created concurrently by another writer")
            return icechunk.Repository.open(storage, config=ICECHUNK_CONFIG)

    @staticmethod
    def gen_store_filename(
        coords: NWPDimensionCoordinateMap,
        extension: str = ".icechunk",
    ) -> str:
        """Create a filename for the store.

        Each store holds a single month of init times, so is named for that month.
        """
        return IcechunkTensorStore._month_filename(coords.init_time[0], extension=extension)

    @staticmethod
    def _month_filename(month: dt.date, extension: str = ".icechunk") -> str:
        """Create the filename of the store holding the given month."""
        return f"{month:%Y-%m}{extension}"

    @staticmethod
    def _create_icechunk_storage(icdir: str, filename: str) -> tuple[icechunk.Storage, str]:
        """Create Icechunk storage config for the given directory and filename.

        Returns a tuple of the `icechunk.Storage` and the path to the store.
        """
        path = "/".join((icdir, filename))
        if icdir.startswith("s3"):
            bucket, _, prefix = path.removeprefix("s3://").partition("/")
            endpoint = os.getenv("AWS_ENDPOINT_URL", None)
            storage = icechunk.s3_storage(
                bucket=bucket,
                prefix=prefix,
                region=os.getenv("AWS_REGION", "eu-west-1"),
                endpoint_url=endpoint,
                allow_http=bool(endpoint and endpoint.startswith("http://")),
                from_env=True,
            )
            return storage, path
        local_path = pathlib.Path(path).expanduser().as_posix()
        return icechunk.local_filesystem_storage(local_path), local_path
