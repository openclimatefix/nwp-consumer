import unittest

from .postprocess import PostProcessOptions


class TestPostProcessOptions(unittest.TestCase):
    """Test the business methods of the PostProcessOptions class."""

    def test_requires_postprocessing(self) -> None:
        """Test that an empty initialization means no postprocessing."""
        test_class = PostProcessOptions()

        self.assertFalse(
            test_class.requires_postprocessing(),
            msg="Empty class should not require postprocessing.",
        )

    def test_requires_rewrite(self) -> None:
        """Test that an empty initialization means no rewriting."""
        test_class = PostProcessOptions()

        self.assertFalse(
            test_class.requires_rewrite(),
            msg="Empty class should not require rewriting.",
        )


if __name__ == "__main__":
    unittest.main()
