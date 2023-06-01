import datetime as dt
import unittest
from unittest import mock

from . import NWPConsumerService
from nwp_consumer import internal


class TestNWPConsumerService_DownloadRawDataset(unittest.TestCase):

    def setUp(self) -> None:
        self.testStorer = mock.create_autospec(spec=internal.StorageInterface)
        self.testFetcher = mock.create_autospec(spec=internal.FetcherInterface)

    def test_DownloadsExpectedFiles(self):
        service = NWPConsumerService(fetcher=self.testFetcher, storer=self.testStorer)

        startDate = dt.date(2021, 1, 1)
        endDate = dt.date(2021, 2, 1)

        paths = service.DownloadRawDataset(startDate=startDate, endDate=endDate)

        self.assertEqual(24*31 + 1, self.testFetcher.listRawFilesForInitTime.call_count)


class TestNWPConsumerService_ConvertRawDataset(unittest.TestCase):

    # TODO: Add tests for the following

    def setUp(self) -> None:
        self.testStorer = mock.create_autospec(spec=internal.StorageInterface)
        self.testFetcher = mock.create_autospec(spec=internal.FetcherInterface)

    def test_ConvertsFiles(self):
        service = NWPConsumerService(fetcher=self.testFetcher, storer=self.testStorer)

        startDate = dt.date(2021, 1, 1)
        endDate = dt.date(2021, 2, 1)

        paths = service.ConvertRawDatasetToZarr(startDate=startDate, endDate=endDate)
