from argparse import Namespace
from typing import Any

import attrs
import pint

# Custom units
ureg = pint.UnitRegistry()
ureg.define("unit_interval = [] = ui")
ureg.define("octas = unit_interval * 1/8")
ureg.define("percent = unit_interval * 0.01")


@attrs.frozen
class Parameter:
    """A parameter that can be requested from a source repository.

    The level-based parameters must be set if parameter is multi-level.

    In OCF's case, some seeming multi-level parameters will be specified as single-level
    (e.g. 10, 100, 200 meter wind speed). This is because the single/multi-level distinction
    is technically determining how the parameter is stored in the dataset, and isn't a
    semantic specifier.

    Multi-level parameters are often read from datasets by loading all levels at once
    (as this is more efficient and desirable from an ML perspective), and as such are stored
    with all levels in the same chunk. Wind speed however is more useful as a single-level-
    stored parameter, with different chunks for each height, as that is how it is typically
    used in forecasting.
    """

    longname: str = attrs.field(validator=attrs.validators.min_len(3))
    """The full name of the parameter."""

    shortname: str = attrs.field(validator=attrs.validators.max_len(10))
    """A short name for the parameter."""

    units: pint.Unit
    """The units of the parameter."""

    level_type: str = attrs.field(
        default="single",
        validator=attrs.validators.in_(["single", "multi"]),
    )
    """The type of level the parameter is defined on (singlelevel or multilevel)."""

    level_value: int | None = attrs.field(default=None)
    """The number preceding the unit defining the level of the parameter."""

    level_units: pint.Unit | None = attrs.field(default=None)
    """The units grounding the level value into measurable space."""

    @level_value.validator
    @level_units.validator
    def _defined_if_multilevel(
            self,
            attribute: attrs.Attribute,
            value: Any | None,  # noqa: ANN401
    ) -> None:
        if self.level_type == "multi" and value is None:
            raise ValueError(f"{attribute.name} must be defined if level_type is 'multi'.")

    def at_level(self, level_value: int, level_units: pint.Unit) -> "Parameter":
        """Return a new multi-level parameter with the given level specification.

        :param level_value: The number preceding the unit defining the level of the parameter.
        :param level_units: The units that anchor the level value in measurable space.
        """
        return attrs.evolve(
            self,
            level_type="multi",
            level_value=level_value,
            level_units=level_units,
        )

    def __repr__(self) -> str:
        """Return a representation of the parameter."""
        level_repr = f"_{self.level_value}{self.level_units}" if self.level_type == "multi" else ""
        return f"{self.longname}{level_repr}:{self.units}"


# Default parameters and units
PARAMS = Namespace(
    lcc=Parameter("low_cloud_cover", "lcc", ureg.Unit("ui")),
    mcc=Parameter("medium_cloud_cover", "mcc", ureg.Unit("ui")),
    hcc=Parameter("high_cloud_cover", "hcc", ureg.Unit("ui")),
    tcc=Parameter("total_cloud_cover", "tcc", ureg.Unit("ui")),
    vis=Parameter("visibility", "vis", ureg.Unit("m")),
    relhum=Parameter("relative_humidity", "r", ureg.Unit("percent")),
    prate=Parameter("precipitation_rate", "prate", ureg.Unit("kg m ** -2 s ** -1")),
    sdepth=Parameter("snow_depth", "sdepth", ureg.Unit("m")),
    dswrf=Parameter("downward_shortwave_radiation_flux", "dswrf", ureg.Unit("W m ** -2")),
    dlwrf=Parameter("downward_longwave_radiation_flux", "dlwrf", ureg.Unit("W m ** -2")),
    t=Parameter("temperature", "t", ureg.Unit("kelvin")),
    si10=Parameter("10m_wind_speed", "si10", ureg.Unit("m s ** -1")),
    wdir10=Parameter("10m_wind_direction", "wdir10", ureg.Unit("degrees")),
    u10=Parameter("10m_wind_u_component", "u10", ureg.Unit("m s ** -1")),
    v10=Parameter("10m_wind_v_component", "v10", ureg.Unit("m s ** -1")),
    u100=Parameter("100m_wind_u_component", "u100", ureg.Unit("m s ** -1")),
    v100=Parameter("100m_wind_v_component", "v100", ureg.Unit("m s ** -1")),
    u200=Parameter("200m_wind_u_component", "u200", ureg.Unit("m s ** -1")),
    v200=Parameter("200m_wind_v_component", "v200", ureg.Unit("m s ** -1")),
)
