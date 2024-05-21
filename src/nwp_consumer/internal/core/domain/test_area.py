import unittest

from .area import Area


class TestArea(unittest.TestCase):
    def test_nlats(self) -> None:
        """Test the nlats method of the Area class."""

        area = Area("test", 90, -180, -90, 180)

        for r in [0.1, 0.5, 1, 2]:
            with self.subTest(resolution=r):
                self.assertEqual(area.nlats(
                    resolution_degrees=r),
                    len(area.lats(resolution_degrees=r)),
                )

    def test_nlons(self) -> None:
        """Test the nlons method of the Area class."""

        area = Area("test", 90, -180, -90, 180)

        for r in [0.1, 0.5, 1, 2]:
            with self.subTest(resolution=r):
                self.assertEqual(area.nlons(
                    resolution_degrees=r),
                    len(area.lons(resolution_degrees=r)),
                )


if __name__ == "__main__":
    unittest.main()
