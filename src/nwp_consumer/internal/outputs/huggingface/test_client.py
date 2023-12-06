import datetime as dt
import inspect
import pathlib
import time
import unittest
import uuid
from unittest.mock import MagicMock, patch

from nwp_consumer import internal

from .client import Client

USER = "openclimatefix"
RAW = pathlib.Path("raw")


class TestHuggingFaceClient(unittest.TestCase):
    repoID: str
    mock_fs: MagicMock
    client: internal.StorageInterface
    datasetPath: str

    @classmethod
    @patch("huggingface_hub.HfFileSystem")
    def setUpClass(cls, mock_patch: MagicMock) -> None:
        cls.repoID = f"{USER}/repo-{uuid.uuid4().hex[:6]}-{int(time.time() * 10e3)}"

        cls.mock_fs = MagicMock()

        cls.mock_fs.du.return_value = 30
        cls.mock_fs.isdir.return_value = False
        cls.mock_fs.exists.return_value = True
        cls.mock_fs.glob.return_value = [
            pathlib.Path(
                f"datasets/{cls.repoID}/{RAW.as_posix()}/{dt.datetime.now(tz=dt.timezone.utc):{internal.IT_FOLDER_FMTSTR}}",
            ),
        ]

        mock_patch.return_value = cls.mock_fs

        cls.client = Client(
            repoID=cls.repoID,
        )

        cls.datasetPath = f"datasets/{cls.repoID}"

    def test_store(self) -> None:
        initTime = dt.datetime.now(tz=dt.timezone.utc)
        filename = inspect.stack()[0][3] + ".grib"
        dst = pathlib.Path(f"{initTime:{internal.IT_FOLDER_FMTSTR}}/{filename}")

        # Write the data to the temporary file
        src = internal.TMP_DIR / f"nwpc-{uuid.uuid4()}"
        src.write_bytes(bytes(filename, "utf-8"))

        out = self.client.store(src=src, dst=dst)
        self.assertEqual(out, dst)
        self.mock_fs.put.assert_called_with(
            lpath=src.as_posix(),
            rpath=(self.datasetPath / dst).as_posix(),
            recursive=True,
        )
        self.mock_fs.du.assert_called_with(path=(self.datasetPath / dst).as_posix())

    def test_exists(self) -> None:
        initTime = dt.datetime.now(tz=dt.timezone.utc)
        filename = inspect.stack()[0][3] + ".grib"
        dst = pathlib.Path(f"{initTime:{internal.IT_FOLDER_FMTSTR}}/{filename}")

        out = self.client.exists(dst=dst)
        self.assertEqual(out, True)
        self.mock_fs.exists.assert_called_with(path=self.datasetPath / dst)

    def test_listInitTimes(self) -> None:
        initTimes = self.client.listInitTimes(prefix=RAW)

        self.assertEqual(len(initTimes), 1)
        self.assertEqual(
            initTimes[0],
            dt.datetime.now(tz=dt.timezone.utc).replace(second=0, microsecond=0),
        )
        self.mock_fs.glob.assert_called_with(
            path=self.datasetPath / RAW / internal.IT_FOLDER_GLOBSTR,
        )

    def test_delete(self) -> None:
        initTime = dt.datetime.now(tz=dt.timezone.utc)
        filename = inspect.stack()[0][3] + ".grib"
        dst = pathlib.Path(f"{initTime:{internal.IT_FOLDER_FMTSTR}}/{filename}")

        self.client.delete(p=dst)
        self.mock_fs.isdir.assert_called_with(path=self.datasetPath / dst)
        self.mock_fs.rm.assert_called_with(path=self.datasetPath / dst)

        # Ensure deleting of directories is handled
        dirDst = dst.parent
        self.mock_fs.isdir.return_value = True
        self.client.delete(p=dirDst)
        self.mock_fs.rm.assert_called_with(path=self.datasetPath / dirDst, recursive=True)
