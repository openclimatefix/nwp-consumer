import datetime as dt
import pathlib

import numpy as np
import xarray as xr
from ocf_blosc2 import Blosc2

from nwp_consumer import internal


class LocalFSClient(internal.StorageInterface):
    """Client for local filesystem."""

    # Location to save raw (Usually GRIB) files
    __rawDir: pathlib.Path

    # Location to save Zarr files
    __zarrDir: pathlib.Path

    def __init__(self, rawDir: str, zarrDir: str, createDirs: bool = False):
        """Create a new LocalFSClient."""
        rawPath: pathlib.Path = pathlib.Path(rawDir)
        zarrPath: pathlib.Path = pathlib.Path(zarrDir)

        if createDirs:
            # If the directories do not exist, create them
            rawPath.mkdir(parents=True, exist_ok=True)
            zarrPath.mkdir(parents=True, exist_ok=True)

        self.__rawDir = rawPath
        self.__zarrDir = zarrPath

    def existsInRawDir(self, fileName: str, initTime: dt.datetime) -> bool:
        """Check if a file exists in the raw directory."""
        path = pathlib.Path(
            f"{self.__rawDir}/{initTime.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING)}/{fileName}")
        return path.exists()

    def writeBytesToRawDir(self, fileName: str, initTime: dt.datetime, data: bytes) -> pathlib.Path:
        """Write the given bytes to the raw directory."""
        path = pathlib.Path(f"{self.__rawDir}/{initTime.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING)}/{fileName}")

        # Create the path to the file if the folders do not exist
        path.parent.mkdir(parents=True, exist_ok=True)

        path.write_bytes(data)
        return path

    def listInitTimesInRawDir(self) -> list[dt.datetime]:
        """List all initTimes in the raw directory."""
        # List all the YYYY/MM/DD/INITTIME folders in the raw directory
        files = [f.relative_to(self.__rawDir) for f in self.__rawDir.glob('*/*/*/*') if f.is_dir()]

        # Get the set of initTimes from the file paths
        initTimes = set([
            dt.datetime.strptime(f.as_posix(), internal.RAW_FOLDER_PATTERN_FMT_STRING).replace(tzinfo=None) for f in files
        ])

        return sorted(initTimes)

    def readBytesForInitTime(self, initTime: dt.datetime) -> tuple[dt.datetime, list[bytes]]:
        """Read all files from the raw dir as bytes for the given init time."""
        initTimeDirPath = pathlib.Path(f"{self.__rawDir}/{initTime.strftime(internal.RAW_FOLDER_PATTERN_FMT_STRING)}")

        if not initTimeDirPath.exists():
            raise FileNotFoundError(f"Folder does not exist for init time {initTime} at {initTimeDirPath.as_posix()}")

        paths: list[pathlib.Path] = list(initTimeDirPath.iterdir())

        # TODO: Filter unwanted filenames

        # Read all files as bytes
        fileByteList: list[bytes] = [path.read_bytes() for path in paths]
        return initTime, fileByteList

    def existsInZarrDir(self, fileName: str, initTime: dt.datetime) -> bool:
        """Check if a file exists in the zarr directory."""
        path = pathlib.Path(f"{self.__zarrDir}/{fileName}")
        return path.exists()

    def writeDatasetToZarrDir(self, fileName: str, initTime: dt.datetime, data: xr.Dataset) -> pathlib.Path:
        """Write the given Dataset to the zarr directory."""
        path = pathlib.Path(f"{self.__zarrDir}/{fileName}")

        # Ensure the zarr path doesn't already exist
        if self.existsInZarrDir(fileName=fileName, initTime=initTime):
            raise FileExistsError(f"Zarr path already exists: {path}")

        # Create a chunked Dask Dataset from the input multi-variate Dataset.
        # *  Converts the input multivariate DataSet (with different DataArrays for
        #     each NWP variable) to a single DataArray with a `variable` dimension.
        # * This allows each Zarr chunk to hold multiple variables (useful for loading
        #     many/all variables at once from disk).

        # Create single-variate dataarray from dataset, with new "variable" dimension
        da = data \
            .to_array(dim="variable", name="UKV") \
            .compute()
        del data

        # Convert back to dataset, order dimensions, and chunk
        chunkedDataset = da.to_dataset() \
            .transpose("init_time", "step", "variable", "y", "x") \
            .sortby("step") \
            .sortby("variable") \
            .chunk({
                "init_time": 1,
                "step": 1,
                "variable": -1,
                "y": len(da.y) // 2,
                "x": len(da.x) // 2,
            }).compute()
        del da

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
        chunkedDataset.to_zarr(path, **to_zarr_kwargs)
        del chunkedDataset
        return path
