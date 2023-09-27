import inspect
import unittest
import uuid

from unittest.mock import patch

from nwp_consumer import internal

from . import Client


class TestHuggingFaceClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = Client(
            token='test-token'
        )

    @patch('client.__fs.HFFileSysem')
    def test_store(self, mock_HFFileSystem):
        filename = inspect.stack()[0][3] + '.grib'
        src = internal.TMP_DIR / f'nwpc-{uuid.uuid4()}'
        src.write_bytes(bytes(filename))

        self.client.store(src, filename)
        self.assertTrue(mock_HFFileSystem.called)

if __name__ == "__main__":
    unittest.main()
