import datetime as dt
import pathlib
import unittest.mock

import xarray as xr

from nwp_consumer.internal import outputs

from ._models import CEDAFileInfo
from .client import (
    COORDINATE_IGNORE_LIST,
    PARAMETER_IGNORE_LIST,
    CEDAClient,
    _isWantedFile,
    _reshapeTo2DGrid,
)

# --------- Test setup --------- #

testStorer = outputs.localfs.LocalFSClient(
    rawDir=pathlib.Path(__file__).parent.as_posix(),
    zarrDir=pathlib.Path(__file__).parent.as_posix(),
)

testClient = CEDAClient(ftpPassword="", ftpUsername="")


# --------- Client methods --------- #


# --------- Static methods --------- #


class TestIsWantedFile(unittest.TestCase):

    def test_correctlyFiltersCEDAFileInfos(self):
        initTime: dt.datetime = dt.datetime(year=2021, month=1, day=1, hour=0, minute=0, tzinfo=dt.timezone.utc)

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
        wholesalePath: pathlib.Path = pathlib.Path(__file__).parent / "test_truncated_Wholesale1.grib"

        dataset = testClient._loadWholesaleFileAsDataset(
            data=wholesalePath.read_bytes(), initTime=dt.datetime(2021, 1, 1, 0, 0))

        reshapedDataset = _reshapeTo2DGrid(dataset=dataset)
        print(reshapedDataset)

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

