import datetime as dt
import os
import unittest
from unittest import mock

from nwp_consumer.internal import FetcherInterface

from .main import _parse_from_to



class TestParseFromTo(unittest.TestCase):
    def test_today(self) -> None:
        # Test that today is processed correctly
        start, end = _parse_from_to("today", None)
        self.assertEqual(
            start,
            dt.datetime.now(tz=dt.UTC).replace(hour=0, minute=0, second=0, microsecond=0),
        )
        self.assertEqual(
            end,
            dt.datetime.now(tz=dt.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
            + dt.timedelta(days=1),
        )

    def test_from_date(self) -> None:
        # Test that a date is processed correctly
        start, end = _parse_from_to("2021-01-01", None)
        self.assertEqual(start, dt.datetime(2021, 1, 1, tzinfo=dt.UTC))
        self.assertEqual(end, dt.datetime(2021, 1, 2, tzinfo=dt.UTC))

    def test_from_datetime(self) -> None:
        # Test that a datetime is processed correctly
        start, end = _parse_from_to("2021-01-01T12:00", None)
        self.assertEqual(start, dt.datetime(2021, 1, 1, 12, 0, tzinfo=dt.UTC))
        self.assertEqual(end, dt.datetime(2021, 1, 1, 12, 0, tzinfo=dt.UTC))

    def test_from_datetime_to_date(self) -> None:
        # Test that a datetime is processed correctly
        start, end = _parse_from_to("2021-01-01T12:00", "2021-01-02")
        self.assertEqual(start, dt.datetime(2021, 1, 1, 12, 0, tzinfo=dt.UTC))
        self.assertEqual(end, dt.datetime(2021, 1, 2, 0, tzinfo=dt.UTC))

    def test_from_datetime_to_datetime(self) -> None:
        # Test that a datetime is processed correctly
        start, end = _parse_from_to("2021-01-01T12:00", "2021-01-02T12:00")
        self.assertEqual(start, dt.datetime(2021, 1, 1, 12, 0, tzinfo=dt.UTC))
        self.assertEqual(end, dt.datetime(2021, 1, 2, 12, 0, tzinfo=dt.UTC))

    def test_invalid_datetime(self) -> None:
        # Test that an invalid datetime is processed correctly
        with self.assertRaises(ValueError):
            _parse_from_to("2021-01-01T12:00:00", None)

        with self.assertRaises(ValueError):
            _parse_from_to("2021010100", None)
