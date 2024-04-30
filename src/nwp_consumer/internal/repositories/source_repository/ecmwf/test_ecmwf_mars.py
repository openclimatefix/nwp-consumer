import os
import unittest
from unittest import mock

from ecmwf_mars import MARSOperationalArchive
from nwp_consumer.internal.core import domain


class TestECMWFMARS(unittest.TestCase):
    def test_metadata(self) -> None:
        metadata = MARSOperationalArchive.metadata()
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
                domain.AREAS.uk,
                domain.AREAS.nw_india,
                domain.AREAS.malta,
            ],
        )

    @mock.patch.dict(os.environ, {"ECMWF_API_KEY": "test_key", "ECMWF_API_EMAIL": "test_email"})
    def test_from_env(self) -> None:
        _ = MARSOperationalArchive.from_env()

    @mock.patch.dict(os.environ, clear=True)
    def test_from_env_error(self) -> None:
        with self.assertRaises(KeyError):
            _ = MARSOperationalArchive.from_env()



