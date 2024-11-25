import dataclasses
import datetime as dt
import unittest

from .postprocess import PostProcessOptions
from .repometadata import ModelRepositoryMetadata


class TestModelRepositoryMetadata(unittest.TestCase):
    """Test the business methods of the ModelRepositoryMetadata class."""

    metadata: ModelRepositoryMetadata = ModelRepositoryMetadata(
        name="test",
        is_archive=False,
        is_order_based=False,
        running_hours=[0, 6, 12, 18],
        delay_minutes=60,
        required_env=["TEST"],
        optional_env={"TEST": "test"},
        max_connections=1,
        postprocess_options=PostProcessOptions(),
    )

    def test_determine_latest_it_from(self) -> None:
        """Test the determine_latest_it_from method."""

        @dataclasses.dataclass
        class TestCase:
            name: str
            t: dt.datetime
            expected: dt.datetime

        tests = [
            TestCase(
                name="rolls_back_inter-day",
                t=dt.datetime(2021, 1, 2, 0, tzinfo=dt.UTC),
                expected=dt.datetime(2021, 1, 1, 18, tzinfo=dt.UTC),
            ),
            TestCase(
                name="rolls_back_intra-day",
                t=dt.datetime(2021, 1, 1, 5, tzinfo=dt.UTC),
                expected=dt.datetime(2021, 1, 1, 0, tzinfo=dt.UTC),
            ),
        ]

        for test in tests:
            with self.subTest(name=test.name):
                result = self.metadata.determine_latest_it_from(test.t)
                self.assertEqual(result, test.expected)


if __name__ == "__main__":
    unittest.main()
