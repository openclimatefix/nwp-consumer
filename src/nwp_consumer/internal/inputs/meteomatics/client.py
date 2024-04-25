"""Implements a client to fetch data from MeteoMatics."""
import datetime as dt
import pathlib

from nwp_consumer import internal


class Client(internal.FetcherInterface):
    """Client to fetch data from MeteoMatics."""

    area: str
    resource_type: str

    def __init__(self, area: str, resource_type: str):
        """Initialize the client."""
        self.area = area
        self.resource_type = resource_type

    def datasetName(self) -> str:
        """Return the dataset name."""
        return f"meteomatics_{self.area}_{self.resource_type}"

    def getInitHours(self) -> int:
        """Return the number of hours to initialize the data."""
        return 1

