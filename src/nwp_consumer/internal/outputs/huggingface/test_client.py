import datetime as dt
import inspect
import pathlib
import time
import unittest
import uuid
from unittest.mock import MagicMock, patch

from nwp_consumer import internal

from .client import Client

USER = 'openclimatefix'
RAW = pathlib.Path('raw')


class TestHuggingFaceClient(unittest.TestCase):

    @classmethod
    @patch('huggingface_hub.HfFileSystem')
    def setUpClass(cls, mock_patch):
        cls.repoID = f'{USER}/repo-{uuid.uuid4().hex[:6]}-{int(time.time() * 10e3)}'

        cls.mock_fs = MagicMock()

        cls.mock_fs.du.return_value = 30
        cls.mock_fs.exists.return_value = True
        cls.mock_fs.glob.return_value = [
            pathlib.Path(f"datasets/{cls.repoID}/{RAW.as_posix()}/{dt.datetime.utcnow():{internal.IT_FOLDER_FMTSTR}}")
        ]

        mock_patch.return_value = cls.mock_fs

        cls.client = Client(
            repoID=cls.repoID,
        )

    def test_store(self):
        initTime = dt.datetime.utcnow()
        filename = inspect.stack()[0][3] + '.grib'
        dst = pathlib.Path(f"{initTime:{internal.IT_FOLDER_FMTSTR}}/{filename}")

        # Write the data to the temporary file
        src = internal.TMP_DIR / f'nwpc-{uuid.uuid4()}'
        src.write_bytes(bytes(filename, 'utf-8'))

        n = self.client.store(src=src, dst=dst)
        self.assertEqual(n, 30)
        self.assertTrue(self.mock_fs.put.called_with(src, dst))
        self.assertTrue(self.mock_fs.du.called_with(dst))

    def test_exists(self):
        initTime = dt.datetime.utcnow()
        filename = inspect.stack()[0][3] + '.grib'
        dst = pathlib.Path(f"{initTime:{internal.IT_FOLDER_FMTSTR}}/{filename}")

        out = self.client.exists(dst=dst)
        self.assertEqual(out, True)
        self.assertTrue(self.mock_fs.exists.called_with(dst))

    def test_listInitTimes(self):
        initTimes = self.client.listInitTimes(prefix=RAW)

        self.assertEqual(len(initTimes), 1)
        self.assertEqual(initTimes[0], dt.datetime.utcnow().replace(second=0, microsecond=0))
        self.assertTrue(self.mock_fs.glob.called_with(f"{self.repoID}/{RAW.as_posix()}/*/*/*/*"))

    def test_delete(self):
        initTime = dt.datetime.utcnow()
        filename = inspect.stack()[0][3] + '.grib'
        dst = pathlib.Path(f"{initTime:{internal.IT_FOLDER_FMTSTR}}/{filename}")

        self.client.delete(p=dst)
        self.assertTrue(self.mock_fs.rm.called_with(p=dst))

        # Ensure deleting of directories is handled
        dirDst = dst.parent
        self.client.delete(p=dirDst)
        self.assertTrue(self.mock_fs.rm.called_with(p=dirDst, recursive=True))

