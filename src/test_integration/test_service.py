import unittest

from src.nwp_consumer.internal.inputs import ceda, metoffice
import datetime as dt

from src.nwp_consumer.internal.service.specificInitTime import (
    updateMonthlyZarrForInitTime
)

cedaInitTime: dt.datetime = dt.datetime(year=2022, month=1, day=1, hour=0, minute=0, tzinfo=dt.timezone.utc)
metOfficeInitTime: dt.datetime = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


class TestUpdateMonthlyZarrForInitTime(unittest.TestCase):

    def test_createsZarrFromCEDAData(self):
        client = ceda.CEDAClient()
        updateMonthlyZarrForInitTime(client=client, initTime=cedaInitTime)
