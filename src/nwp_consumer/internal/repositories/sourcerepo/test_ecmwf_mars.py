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
