import datetime as dt
import unittest

from .main import _process_time_args


class TestProcessTimeArgs(unittest.TestCase):
    # Test suite to test _process_time_args function
    # tests each of the input cases specified by comments in the function
    # with a function per test case

    def test_today(self) -> None:
        # Test that today is processed correctly
        start, end = _process_time_args("today", None)
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
        start, end = _process_time_args("2021-01-01", None)
        self.assertEqual(start, dt.datetime(2021, 1, 1, tzinfo=dt.UTC))
        self.assertEqual(end, dt.datetime(2021, 1, 2, tzinfo=dt.UTC))

    def test_from_datetime(self) -> None:
        # Test that a datetime is processed correctly
        start, end = _process_time_args("2021-01-01T12:00", None)
        self.assertEqual(start, dt.datetime(2021, 1, 1, 12, 0, tzinfo=dt.UTC))
        self.assertEqual(end, dt.datetime(2021, 1, 1, 12, 0, tzinfo=dt.UTC))

    def test_from_datetime_to_date(self) -> None:
        # Test that a datetime is processed correctly
        start, end = _process_time_args("2021-01-01T12:00", "2021-01-02")
        self.assertEqual(start, dt.datetime(2021, 1, 1, 12, 0, tzinfo=dt.UTC))
        self.assertEqual(end, dt.datetime(2021, 1, 2, 0, tzinfo=dt.UTC))

    def test_from_datetime_to_datetime(self) -> None:
        # Test that a datetime is processed correctly
        start, end = _process_time_args("2021-01-01T12:00", "2021-01-02T12:00")
        self.assertEqual(start, dt.datetime(2021, 1, 1, 12, 0, tzinfo=dt.UTC))
        self.assertEqual(end, dt.datetime(2021, 1, 2, 12, 0, tzinfo=dt.UTC))

    def test_invalid_datetime(self) -> None:
        # Test that an invalid datetime is processed correctly
        with self.assertRaises(ValueError):
            _process_time_args("2021-01-01T12:00:00", None)

        with self.assertRaises(ValueError):
            _process_time_args("2021010100", None)
