import datetime as dt
import unittest
from unittest.mock import patch

from . import NWPConsumerService
from nwp_consumer import internal


class TestNWPConsumerService_DownloadRawDataset(unittest.TestCase):

    @patch.multiple(internal.FetcherInterface, __abstractmethods__=set())
    @patch.multiple(internal.StorageInterface, __abstractmethods__=set())
    def TestDownloadsExpectedFiles(self):
        testStorer = internal.StorageInterface()
        testFetcher = internal.FetcherInterface()

        service = NWPConsumerService(fetcher=testFetcher, storer=testStorer)

        startDate = dt.date(2021, 1, 1)
        endDate = dt.date(2021, 2, 1)

        paths = service.DownloadRawDataset(startDate=startDate, endDate=endDate)
