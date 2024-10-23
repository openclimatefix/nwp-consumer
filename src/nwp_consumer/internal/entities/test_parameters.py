import unittest

from hypothesis import given
from hypothesis import strategies as st

from .parameters import Parameter


class TestParameters(unittest.TestCase):
    """Test the business methods of the Parameters class."""

    @given(st.sampled_from(Parameter))
    def test_metadata(self, p: Parameter) -> None:
        """Test the metadata method."""
        metadata = p.metadata()
        self.assertEqual(metadata.name, p.value)


if __name__ == "__main__":
    unittest.main()
