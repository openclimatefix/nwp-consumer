import datetime as dt
import pathlib

import structlog
import xarray as xr

from src.nwp_consumer import internal

log = structlog.stdlib.get_logger()


def combineSingleParamGRIBsAsOCFDataset(
        client: internal.FetcherInterface, parameterFilePaths: list[pathlib.Path], initTime: dt.datetime) -> xr.Dataset:
    """Combines many single-parameter GRIB files as a single xarray dataset."""
    # Instantiate a list to hold the DataArrays for each parameter
    parameterDataArrays: list[xr.DataArray] = []
    datasetName: str = initTime.strftime("%Y%m%dT%H%M")

    log.debug(
        f"Creating Dataset {datasetName} from {len(parameterFilePaths)} files",
        dataset=datasetName,
        files=[path.as_posix() for path in parameterFilePaths]
    )

    # For each parameter file, load test_integration as a DataArray and add test_integration to the list
    for filePath in parameterFilePaths:
        try:
            parameterDataArray = client.loadSingleParameterGRIBAsOCFDataArray(path=filePath, initTime=initTime)
            parameterDataArrays.append(parameterDataArray)
        except Exception as e:
            raise Exception(f"Error loading file {filePath.as_posix()} as DataArray: {e}")

        log.debug(
            f"Merging DataArray from parameter {filePath.stem} into dataset {datasetName}",
            parameterFile=filePath.as_posix(), dataset=datasetName
        )

    # Merge the DataArrays into a single Dataset
    dataset = xr.merge(
        parameterDataArrays,
        compat='identical', combine_attrs='drop_conflicts'
    )

    del parameterDataArrays

    return dataset
