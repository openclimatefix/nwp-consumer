import datetime as dt
import pathlib
import unittest

from nwp_consumer import internal

from .client import Client

USER = "openclimatefix"
RAW = pathlib.Path("raw")


class TestHuggingFaceClient(unittest.TestCase):
    repoID: str
    client: Client

    @classmethod
    def setUpClass(cls) -> None:
        cls.repoID = "PolyAI/minds14"
        cls.client = Client(repoID=cls.repoID)

    def test_get_size(self) -> None:
        """Test that the size of a file is returned correctly."""
        name_size_map: dict[str, int] = {
            "README.md": 5292,
            "data": 471355396,
        }
        for name, exp in name_size_map.items():
            with self.subTest(msg=name):
                self.assertEqual(self.client._get_size(p=pathlib.Path(name)), exp)

    def test_exists(self) -> None:
        """Test that the existence of a file is returned correctly."""
        name_exists_map: dict[str, bool] = {
            "README.md": True,
            "data": True,
            "nonexistent1": False,
            "nonexistent/nonexistent2": False,
        }
        for name, exp in name_exists_map.items():
            with self.subTest(msg=name):
                self.assertEqual(self.client.exists(dst=pathlib.Path(name)), exp)

