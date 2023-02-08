from src.nwp_consumer import internal

import datetime as dt
import pathlib
import xarray as xr
import numpy as np

class DummyClient(internal.ClientInterface):
    def getDatasetForInitTime(self, initTime: dt.datetime) -> xr.Dataset:
        return xr.Dataset(
            data_vars={
                "vis": (["step", "x", "y"], np.random.rand(3, 100, 100)),
                "t": (["step", "x", "y"], np.random.rand(3, 100, 100)),
            },
            coords={
                "init_time": np.datetime64(initTime),
                "step": (["step"], np.arange(3)),
                "x": (["x"], np.arange(100)),
                "y": (["y"], np.arange(100)),
            }
        )

    def loadSingleParameterGRIBAsOCFDataArray(self, path: pathlib.Path, initTime: dt.datetime) -> xr.DataArray:
        return xr.DataArray()
