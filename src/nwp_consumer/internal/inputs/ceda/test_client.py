import datetime as dt
import pathlib
import unittest.mock

import numpy as np
import xarray as xr

from ._models import CEDAFileInfo
from .client import (
    Client,
    _isWantedFile,
    _reshapeTo2DGrid,
)

# --------- Test setup --------- #

testClient = Client(ftpPassword="", ftpUsername="")


# --------- Client methods --------- #

class TestClient_ListRawFilesForInitTime(unittest.TestCase):

    def test_listsFilesCorrectly(self) -> None:
        pass


class TestClient_FetchRawFileBytes(unittest.TestCase):

    def test_fetchesFileCorrectly(self) -> None:
        pass


class TestClient_MapTemp(unittest.TestCase):

    def test_convertsWholesale1FileCorrectly(self) -> None:
        wholesalePath: pathlib.Path = pathlib.Path(__file__).parent / "test_wholesale1.grib"

        out = testClient.mapTemp(p=wholesalePath)

        # Ensure the dimensions have the right sizes
        self.assertDictEqual(
            {"init_time": 1, "variable": 6, "step": 4, "y": 704, "x": 548},
            dict(out.dims.items()),
        )
        # Ensure the dimensions of the variables are in the correct order
        self.assertEqual(("variable", "init_time", "step", "y", "x"), out["UKV"].dims)
        # Ensure the correct variables are in the variable dimension
        self.assertListEqual(
            ["prate", "r", "si10", "t", "vis", "wdir10"],
            sorted(out.coords["variable"].values),
        )

    def test_convertsWholesale2FileCorrectly(self) -> None:
        wholesalePath: pathlib.Path = pathlib.Path(__file__).parent / "test_wholesale2.grib"

        out = testClient.mapTemp(p=wholesalePath)

        # Ensure the dimensions have the right sizes
        self.assertDictEqual(
            {"init_time": 1, "variable": 6, "step": 4, "y": 704, "x": 548},
            dict(out.dims.items()),
        )
        # Ensure the dimensions of the variables are in the correct order
        self.assertEqual(("variable", "init_time", "step", "y", "x"), out["UKV"].dims)
        # Ensure the correct variables are in the variable dimension
        self.assertListEqual(
            ["dlwrf", "dswrf", "hcc", "lcc", "mcc", "sde"],
            sorted(out.coords["variable"].values),
        )

# --------- Static methods --------- #

class TestIsWantedFile(unittest.TestCase):

    def test_correctlyFiltersCEDAFileInfos(self) -> None:
        initTime: dt.datetime = dt.datetime(
            year=2021, month=1, day=1, hour=0, minute=0, tzinfo=dt.timezone.utc,
        )

        wantedFileInfos: list[CEDAFileInfo] = [
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale1.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale2.grib"),
        ]

        unwantedFileInfos: list[CEDAFileInfo] = [
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale1T54.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale2T54.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale3.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale3T54.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale4.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale5.grib"),
            CEDAFileInfo(name="202101010000_u1096_ng_umqv_Wholesale5T54.grib"),
            CEDAFileInfo(name="202101010300_u1096_ng_umqv_Wholesale1T120.grib"),
            CEDAFileInfo(name="202101010300_u1096_ng_umqv_Wholesale1.grib"),
        ]

        self.assertTrue(
            all(_isWantedFile(fi=fo, dit=initTime) for fo in wantedFileInfos))
        self.assertFalse(
            all(_isWantedFile(fi=fo, dit=initTime) for fo in unwantedFileInfos))


class TestReshapeTo2DGrid(unittest.TestCase):

    def test_correctlyReshapesData(self) -> None:
        dataset = xr.Dataset(
            data_vars={
                "wdir10": (("step", "values"), np.random.rand(4, 385792)),
            },
            coords={
                "step": [0, 1, 2, 3],
            },
        )

        reshapedDataset = _reshapeTo2DGrid(ds=dataset)

        self.assertEqual(548, reshapedDataset.dims["x"])
        self.assertEqual(704, reshapedDataset.dims["y"])

        with self.assertRaises(KeyError):
            _ = reshapedDataset["values"]

    def test_raisesErrorForIncorrectNumberOfValues(self) -> None:
        ds1 = xr.Dataset(
            data_vars={
                "wdir10": (("step", "values"), [[1, 2, 3, 4], [5, 6, 7, 8]]),
            },
            coords={
                "step": [0, 1],
            },
        )

        with self.assertRaises(ValueError):
            _ = _reshapeTo2DGrid(ds=ds1)
