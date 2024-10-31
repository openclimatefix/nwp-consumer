import unittest

from hypothesis import given
from hypothesis import strategies as st
from returns.pipeline import is_successful

from .parameters import Parameter


class TestParameters(unittest.TestCase):
    """Test the business methods of the Parameters class."""

    @given(st.sampled_from(Parameter))
    def test_metadata(self, p: Parameter) -> None:
        """Test the metadata method."""
        metadata = p.metadata()
        self.assertEqual(metadata.name, p.value)

    @given(st.sampled_from([s for p in Parameter for s in p.metadata().alternate_shortnames]))
    def test_try_from_shortname(self, shortname: str) -> None:
        """Test the try_from_shortname method."""
        p = Parameter.try_from_alternate(shortname)
        self.assertTrue(is_successful(p))

        p = Parameter.try_from_alternate("invalid")
        self.assertFalse(is_successful(p))


if __name__ == "__main__":
    unittest.main()
