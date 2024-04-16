import datetime as dt
import pathlib
import unittest

import xarray as xr
from nwp_consumer.internal.core import domain

from .ecmwf_mars import MARSOperationalArchive


class TestECMWF_MARS(unittest.TestCase):
    def test_metadata(self) -> None:
        mars = MARSOperationalArchive()
        metadata = mars.metadata()
        self.assertEqual(metadata.name, "ecmwf-mars")
        self.assertEqual(metadata.is_archive, True)
        self.assertEqual(metadata.is_order_based, False)
        self.assertEqual(metadata.running_hours, [0, 12])
        self.assertEqual(
            metadata.available_steps,
            [
                *list(range(90)),
                *list(range(90, 144, 3)),
                *list(range(144, 240, 6)),
            ],
        )
        self.assertEqual(
            metadata.available_areas,
            [
                domain.UK,
                domain.NW_INDIA,
                domain.MALTA,
            ],
        )

    def test_initialize_store(self) -> None:
        sourcerepo = MARSOperationalArchive()
        request = domain.DataRequest(
            init_time=dt.datetime(2021, 1, 1, 0, 0, tzinfo=dt.UTC),
            parameters=["param1", "param2"],
            area=domain.UK,
            steps=[0, 1, 2],
        )
        result = sourcerepo.initialize_store(request)
        self.assertEqual(result.is_ok(), True, msg=result.err())
        self.assertEqual(result.unwrap(), pathlib.Path("~/.local/cache/nwp_consumer/20210101T0000-mars.zarr"))
        ds = xr.open_zarr(result.unwrap())
        self.assertEqual(
            ds.coords.sizes, {"init_time": 1, "latitude": 141, "longitude": 151, "step": 3}
        )
