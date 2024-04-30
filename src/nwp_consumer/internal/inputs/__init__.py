"""Available inputs to source data from."""

__all__ = [
    "ceda",
    "metoffice",
    "ecmwf",
    "icon",
    "cmc",
    "meteofrance",
    "meteomatics",
    "noaa",
]

from . import (
    ceda,
    cmc,
    ecmwf,
    icon,
    meteofrance,
    meteomatics,
    metoffice,
    noaa,
)

