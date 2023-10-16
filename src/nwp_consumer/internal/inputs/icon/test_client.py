import datetime as dt
import unittest

import requests
from .client import Client, PARAMATER_RENAME_MAP


class TestClient(unittest.TestCase):

    def test_listFiles(self):
        testClient = Client()

        it: dt.datetime = dt.datetime.now(dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        files = testClient.listRawFilesForInitTime(it=it)

        self.assertEqual(len(files), len(PARAMATER_RENAME_MAP) * 49)

