import io
import os

from src.nwp_consumer import internal

import pathlib
import xarray as xr
from ocf_blosc2 import Blosc2
import numpy as np
from io import BytesIO


class LocalFSClient(internal.StorageInterface):
    """Client for local filesystem."""

    __rawGribDir: pathlib.Path
    __zarrDir: pathlib.Path

    def __init__(self):

        self.__rawGribDir = pathlib.Path(os.getenv(
            "RAW_GRIB_DIR_PATH",
            (pathlib.Path(__file__).parents[5].resolve() / "downloads").as_posix()
        ))
        self.__zarrDir = pathlib.Path(os.getenv(
            "ZARR_DIR_PATH",
            (pathlib.Path(__file__).parents[5].resolve() / "downloads").as_posix()
        ))

    def exists(self, filepath: pathlib.Path) -> bool:
        """Check if the given path exists."""

        match filepath.suffix:
            case ".grib": return (self.__rawGribDir / filepath).exists()
            case ".zarr": return (self.__zarrDir / filepath).exists()
            case _: raise NotImplementedError(f"Unknown file extension: {filepath.suffix}")

    def open(self, path: pathlib.Path) -> io.BufferedWriter:
        """Open a file, returning a writeable file-like object."""

        match path.suffix:
            case ".grib":
                path = self.__rawGribDir / path
            case ".zarr":
                path = self.__zarrDir / path
            case _:
                raise NotImplementedError(f"Unknown file extension: {path.suffix}")

        # Create the path to the file if the folders do not exist
        path.parent.mkdir(parents=True, exist_ok=True)

        return path.open("wb")

    def saveDataset(self, dataset: xr.Dataset, filepath: pathlib.Path) -> None:
        """Store the given dataset as zarr."""

        path = self.__zarrDir / filepath

        chunkedDataset = _createChunkedDaskDataset(dataset)
        del dataset

        # Ensure the zarr path doesn't already exist
        if self.exists(path):
            raise ValueError(f"Zarr path already exists: {path}")

        # Create new Zarr store.
        to_zarr_kwargs = dict(
            encoding={
                "init_time": {"units": "nanoseconds since 1970-01-01"},
                "UKV": {
                    "compressor": Blosc2(cname="zstd", clevel=5),
                },
            },
        )

        chunkedDataset["UKV"] = chunkedDataset.astype(np.float16)["UKV"]
        chunkedDataset.to_zarr(filepath, **to_zarr_kwargs)
        del chunkedDataset

    def appendDataset(self, dataset: xr.Dataset, filepath: pathlib.Path) -> None:
        """Append the given dataset to the existing Zarr store."""

        path = self.__zarrDir / filepath

        # Ensure the zarr path already exists
        if not self.exists(path):
            raise ValueError(f"Error appending dataset to path {path}: path does not exist")

        # Append to existing Zarr store.
        to_zarr_kwargs = dict(
            append_dim="init_time",
        )

        chunkedDataset = _createChunkedDaskDataset(dataset)
        del dataset

        chunkedDataset["UKV"] = chunkedDataset.astype(np.float16)["UKV"]
        chunkedDataset.to_zarr(filepath, **to_zarr_kwargs)

        del chunkedDataset


def _createChunkedDaskDataset(ds: xr.Dataset) -> xr.Dataset:
    """Create a chunked Dask Dataset from the input multi-variate Dataset.

    Converts the input multivariate DataSet (with different DataArrays for
    each NWP variable) to a single DataArray with a `variable` dimension.
    This allows each Zarr chunk to hold multiple variables (useful for loading
    many/all variables at once from disk)."""

    # Create single-variate dataarray from dataset, with new "variable" dimension
    da = ds.to_array(dim="variable", name="UKV")
    del ds

    return (
        da.to_dataset().chunk(
            {
                "init_time": 1,
                "step_time": 1,
                "variable": -1,
            })
    )