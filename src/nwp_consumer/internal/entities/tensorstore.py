"""Domain classes for store metadata.

Converted data is stored in Zarr stores, which are chunked datastores
enabling subselection across any dimension of data, provided it is
chunked appropriately.

This module provides a class for storing metadata about a Zarr store.

TODO: 2024-11-20 This module wants refactoring into smaller testable components.
"""

import abc
import dataclasses
import datetime as dt
import logging
import os
import pathlib
import shutil
from collections.abc import Mapping, MutableMapping
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr
import zarr
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
    """Store class for multidimensional data.

    This class is used to store data in a Zarr store.
    Each store instance has defined coordinates for the data,
    and is capable of handling parallel, region-based updates.
    """

    name: str
    """Identifier for the store and the data within."""

    path: str
    """The path to the store."""

    coordinate_map: NWPDimensionCoordinateMap
    """The coordinates of the store."""

    size_kb: int
    """The size of the store in kilobytes."""

    encoding: dict[str, Any]
    """The encoding passed to Zarr whilst writing."""

    @classmethod
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

        The dataarray is 'blank' because it is written via::

            dataarray.to_zarr("<example>.zarr", compute=False)

        which writes the metadata alone.
        The utility of this is to enable region-based writing of new data to the store,
        which further allows for parallel write processes.

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
            An indicator of a successful store write containing the number of bytes written.

        See Also:
            - https://docs.xarray.dev/en/stable/user-guide/io.html#appending-to-existing-zarr-stores
            - https://docs.xarray.dev/en/stable/user-guide/io.html#distributed-writes

        Returns:
            A new instance of the TensorStore class.
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
        store: zarr.storage.Store
        path: str
        filename: str = TensorStore.gen_store_filename(coords=coords)
        try:
            if zarrdir.startswith("s3"):
                store_result = cls._create_zarrstore_s3(zarrdir, filename)
                store, path = store_result.unwrap()  # Can do this as exceptions are caught
            else:
                path = pathlib.Path("/".join((zarrdir, filename))).expanduser().as_posix()
                store = zarr.storage.DirectoryStore(path)
        except Exception as e:
            return Failure(
                OSError(
                    f"Unable to create Directory Store at dir '{zarrdir}'. "
                    "Ensure ZARRDIR environment variable is specified correctly. "
                    f"Error context: {e}",
                ),
            )

        # Write the coordinates to a skeleton Zarr store
        # * 'compute=False' enables only saving metadata
        # * 'mode="w-"' fails if it finds an existing store

        da: xr.DataArray = coords.as_zeroed_dataarray(name=model, chunks=chunks)
        encoding = {
            model: {"write_empty_chunks": False},
            "init_time": {"units": "nanoseconds since 1970-01-01"},
            "step": {"units": "hours"},
        }
        try:
            _ = da.to_zarr(
                store=store,
                compute=False,
                mode="w-",
                consolidated=True,
                encoding=encoding,
            )
            log.info("Created blank zarr store at '%s'", path)
            # Ensure the store is readable
            store_da = xr.open_dataarray(store, engine="zarr")
        except zarr.errors.ContainsGroupError:
            store_da = xr.open_dataarray(store, engine="zarr")
            if store_da.name != da.name:  # TODO: Also check for equality of coordinates
                return Failure(
                    OSError(
                        f"Existing store at '{path}' is for a different model. "
                        "Delete the existing store or move it to a new location, "
                        "or choose a new location for the new store via ZARRDIR.",
                    ),
                )
            log.info(f"Using existing store at '{path}'")
            return Success(
                cls(
                    name=model,
                    path=path,
                    coordinate_map=coords,
                    size_kb=store_da.nbytes // 1024,
                    encoding=encoding,
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
            ),
        )

    # def from_existing_store(
    #    model: str,
    #    repository: str,
    #    expected_coords: NWPDimensionCoordinateMap,
    # ) -> ResultE["TensorStore"]:
    #    """Create a TensorStore instance from an existing store."""
    #    pass # TODO

    #    for dim in store_da.dims:
    #        if dim not in da.dims:
    #            return Failure(
    #                ValueError(
    #                    "Cannot use existing store due to mismatched coordinates. "
    #                    f"Dimension '{dim}' in existing store not found in new store. "
    #                    "Use 'overwrite_existing=True' or move the existing store at "
    #                    f"'{store}' to a new location. ",
    #                ),
    #            )
    #        if not np.array_equal(store_da.coords[dim].values, da.coords[dim].values):
    #            return Failure(
    #                ValueError(
    #                    "Cannot use existing store due to mismatched coordinates. "
    #                    f"Dimension '{dim}' in existing store has different coordinate "
    #                    "values from specified. "
    #                    "Use 'overwrite_existing=True' or move the existing store at "
    #                    f"'{store}' to a new location.",
    #                ),
    #            )

    # --- Business logic methods --- #
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
        chunksizes: Mapping[Any, tuple[int, ...]] = xr.open_dataarray(
            self.path,
            engine="zarr",
        ).chunksizes
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
            da.to_zarr(store=self.path, region=region, consolidated=True)
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

    def validate_store(self) -> ResultE[None]:
        """Validate the store.

        This method checks the store for the presence of all expected parameters.

        Returns:
            A bool indicating the result of the validation.
        """
        log.debug(f"Validating store at '{self.path}'")
        store_da: xr.DataArray = xr.open_dataarray(self.path, engine="zarr")

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
        store_da: xr.DataArray = xr.open_dataarray(self.path, engine="zarr")

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

    def update_attrs(self, attrs: dict[str, str]) -> ResultE[str]:
        """Update the attributes of the store.

        This method updates the attributes of the store with the given dictionary.
        """
        group: zarr.Group = zarr.open_group(self.path)
        group.attrs.update(attrs)
        zarr.consolidate_metadata(self.path)
        return Success(self.path)

    def missing_times(self) -> ResultE[list[dt.datetime]]:
        """Find the missing init_time in the store.

        A "missing init_time" is determined by the values corresponding
        to the first two coordinate values of each dimension: if all are
        NaN or None values then the time is considered missing.
        """
        try:
            store_da: xr.DataArray = xr.open_dataarray(self.path, engine="zarr")
        except Exception as e:
            return Failure(
                OSError(
                    "Cannot determine missing times in store due to "
                    f"error reading '{self.path}': {e}",
                ),
            )
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
    def _create_zarrstore_s3(s3_folder: str, filename: str) -> ResultE[tuple[MutableMapping, str]]:  # type: ignore
        """Create a mutable mapping to an S3 store.

        Authentication with S3 is done via botocore's credential discovery.

        Returns:
            A tuple containing the store mapping and the path to the store,
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
                client_kwargs={
                    "region_name": os.getenv("AWS_REGION", "eu-west-1"),
                    "endpoint_url": os.getenv("AWS_ENDPOINT_URL", None),
                },
            )
            path = s3_folder + "/" + filename
            fs.mkdirs(path=path, exist_ok=True)
            store = s3fs.mapping.S3Map(path, fs, check=False, create=True)
        except Exception as e:
            return Failure(
                OSError(
                    f"Unable to create file mapping for path '{path}'. "
                    "Ensure ZARRDIR environment variable is specified correctly, "
                    "and AWS credentials are discoverable by botocore. "
                    f"Error context: {e}",
                ),
            )
        return Success((store, path))

    @staticmethod
    def gen_store_filename(coords: NWPDimensionCoordinateMap) -> str:
        """Create a filename for the store.

        If the store only covers a single init_time, the filename is the init time.
        Else, if it covers multiple init_times, the filename is the range of init times.
        The extension is '.zarr'.
        """
        store_range: str = coords.init_time[0].strftime("%Y%m%d%H")
        if len(coords.init_time) > 1:
            store_range = f"{coords.init_time[0]:%Y%m%d%H}-{coords.init_time[-1]:%Y%m%d%H}"

        return store_range + ".zarr"
