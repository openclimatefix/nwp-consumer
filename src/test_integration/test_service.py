import datetime as dt
import unittest

from src.nwp_consumer.internal.inputs import ceda
from src.nwp_consumer.internal.outputs import localfs
from src.nwp_consumer.internal.service.monthlyZarrDataset import createMonthlyZarrDataset

cedaInitTime: dt.datetime = dt.datetime(year=2022, month=1, day=1, hour=0, minute=0, tzinfo=dt.timezone.utc)
metOfficeInitTime: dt.datetime = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


class TestUpdateMonthlyZarrForInitTime(unittest.TestCase):

    def test_createsZarrFromCEDAData(self):
        createMonthlyZarrDataset(
            fetcher=ceda.CEDAClient(storageClient=localfs.LocalFSClient()),
            storer=localfs.LocalFSClient(),
            startDate=cedaInitTime.date(),
            endDate=(cedaInitTime + dt.timedelta(days=62)).date(),
        )
