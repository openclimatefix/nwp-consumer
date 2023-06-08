import datetime as dt
import pathlib
import unittest.mock
import xarray as xr

from ._models import CEDAFileInfo
from .client import (
    COORDINATE_IGNORE_LIST,
    PARAMETER_IGNORE_LIST,
    CEDAClient,
    _isWantedFile,
    _reshapeTo2DGrid,
    _loadWholesaleFileAsDataset,
)

# --------- Test setup --------- #

testClient = CEDAClient(ftpPassword="", ftpUsername="")


# --------- Client methods --------- #


# --------- Static methods --------- #

class TestLoadWholesaleFileAsDataset(unittest.TestCase):

    def test_loadsAWholesale1FileCorrectly(self):
        wholesalePath: pathlib.Path = pathlib.Path(__file__).parent / "test_wholesale1.grib"

        out = _loadWholesaleFileAsDataset(
            data=wholesalePath.read_bytes(),
        )

        self.assertEqual(({"step": 4, "values": 385792}), out.dims)
        self.assertEqual(['t', 'r', 'vis', 'si10', 'wdir10', 'prate'], list(out.data_vars))

    def test_loadsWholesale2FileCorrectly(self):
        wholesalePath: pathlib.Path = pathlib.Path(__file__).parent / "test_wholesale2.grib"

        out = _loadWholesaleFileAsDataset(
            data=wholesalePath.read_bytes(),
        )

        self.assertEqual(({"step": 4, "values": 385792}), out.dims)
        self.assertEqual(['lcc', 'mcc', 'hcc', 'sde', 'dswrf', 'dlwrf'], list(out.data_vars))


class TestIsWantedFile(unittest.TestCase):

    def test_correctlyFiltersCEDAFileInfos(self):
        initTime: dt.datetime = dt.datetime(year=2021, month=1, day=1, hour=0, minute=0, tzinfo=None)

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
            all([_isWantedFile(fileInfo=fo, desiredInitTime=initTime) for fo in wantedFileInfos]))
        self.assertFalse(
            all([_isWantedFile(fileInfo=fo, desiredInitTime=initTime) for fo in unwantedFileInfos]))


class TestReshapeTo2DGrid(unittest.TestCase):

    def test_correctlyReshapesData(self):
        wholesalePath: pathlib.Path = pathlib.Path(__file__).parent / "test_wholesale1.grib"

        dataset = _loadWholesaleFileAsDataset(
            data=wholesalePath.read_bytes())

        reshapedDataset = _reshapeTo2DGrid(dataset=dataset)

        self.assertEqual(548, reshapedDataset.dims['x'])
        self.assertEqual(704, reshapedDataset.dims['y'])

        with self.assertRaises(KeyError):
            _ = reshapedDataset['values']

    def test_raisesErrorForIncorrectNumberOfValues(self):
        ds1 = xr.Dataset(
            data_vars={
                'wdir10': (('step', 'values'), [[1, 2, 3, 4], [5, 6, 7, 8]]),
            },
            coords={
                'step': [0, 1],
            }
        )

        with self.assertRaises(ValueError):
            _ = _reshapeTo2DGrid(dataset=ds1)
