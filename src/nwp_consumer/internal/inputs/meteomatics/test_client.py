import pathlib
import unittest

from .client import Client


class TestMeteomaticsClient(unittest.TestCase):
    client = Client(
        "username",
        "password",
        "nw-india",
        "solar",
    )

    def test_map_cached(self) -> None:
        file = pathlib.Path(__file__).parent / "test_solar.csv"

        ds = self.client.mapCachedRaw(p=file)

        self.assertEqual(ds.sizes, {"station_id": 24, "time_utc": 189, "init_time": 1})
        self.assertEqual(list(ds.data_vars), ["direct_rad:W", "diffuse_rad:W", "global_rad:W"])

