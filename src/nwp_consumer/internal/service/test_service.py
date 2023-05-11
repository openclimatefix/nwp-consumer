import datetime as dt
import pathlib
import unittest

import xarray as xr

from nwp_consumer import internal


class DummyFetcher(internal.FetcherInterface):

    def downloadRawDataForInitTime(self, initTime: dt.datetime) -> list[pathlib.Path]:
        return [pathlib.Path(initTime.strftime("%Y%m%d%H%M"))]

    def loadRawInitTimeDataAsOCFDataset(self, rawRelativePaths: list[pathlib.Path], initTime: dt.datetime) -> xr.Dataset:
        return xr.Dataset()


class TestNWPConsumerService_DownloadRawDataset(unittest.TestCase):

    def TestDownloadsExpectedFiles(self):
        # TODO: Implement
        pass
