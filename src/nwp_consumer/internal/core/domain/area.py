"""Domain models for geographical areas.

NWP data often comes in a grid format, with data points at regular geospatial intervals.
Some sources enable a choice of area at request time, others require it to be specified
in a predetermined order. In order to keep consistency between different sources covering
similar regions, we define a set of areas with bounding boxes in NWSE format.
"""

import attrs


@attrs.frozen
class Area:
    """A geographical area, with bounding box in NWSE format.

    Lat/Long coordinates are in decimal degrees and must be given as signed floats as follows:
    - Latitudes north of the equator are *positive*.
    - Latitudes south of the equator are *negative*.
    - Longitudes east of the prime meridian are *positive*.
    - Longitudes west of the prime meridian are *negative*.

    :var name: Name of the area.
    :var north: Northernmost latitude.
    :var west: Westernmost longitude.
    :var south: Southernmost latitude.
    :var east: Easternmost longitude.
    """

    name: str
    north: float = attrs.field(validator=[attrs.validators.ge(-90), attrs.validators.le(90)])
    west: float = attrs.field(validator=[attrs.validators.ge(-180), attrs.validators.le(180)])
    south: float = attrs.field(validator=[attrs.validators.ge(-90), attrs.validators.le(90)])
    east: float = attrs.field(validator=[attrs.validators.ge(-180), attrs.validators.le(180)])

    def __str__(self) -> str:
        """String representation of the area."""
        return self.name + " (" + self.nwse() + ")"

    def nwse(self) -> str:
        """Return the bounding box as north/west/south/east string."""
        return "/".join([str(x) for x in (self.north, self.west, self.south, self.east)])

    def lats(self, resolution_degrees: float) -> list[float]:
        """Return the latitudes of the area at a given resolution.

        :param resolution_degrees: The resolution of the grid in degrees.
        """
        return [self.north - i * resolution_degrees for i in range(self.nlats(resolution_degrees))]

    def lons(self, resolution_degrees: float) -> list[float]:
        """Return the longitudes of the area at a given resolution.

        :param resolution_degrees: The resolution of the grid in degrees.
        """
        return [self.west + i * resolution_degrees for i in range(self.nlons(resolution_degrees))]

    def nlats(self, resolution_degrees: float) -> int:
        """Return the number of latitudes in the area at a given resolution.

        :param resolution_degrees: The resolution of the grid in degrees.
        """
        # Add one to include the last latitude
        return round((self.north - self.south) / resolution_degrees) + 1

    def nlons(self, resolution_degrees: float) -> int:
        """Return the number of longitudes in the area at a given resolution.

        :param resolution_degrees: The resolution of the grid in degrees.
        """
        # Add one to include the last longitude
        return round((self.east - self.west) / resolution_degrees) + 1


class AREAS:
    """Predefined areas available from sources."""

    gl = Area("global", 90, -180, -90, 180)
    eu = Area("eu", 73.5, -27, 33, 45)
    nw_india = Area("nw_india", 31, 68, 20, 79)
    uk = Area("uk", 62, -12, 48, 3)
    malta = Area("malta", 37, 68, 20, 79)
