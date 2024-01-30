import datetime as dt
from dataclasses import dataclass

import nwp_consumer.internal as internal


@dataclass
class ECMWFMarsFileInfo(internal.FileInfoModel):
    inittime: dt.datetime
    area: str
    params: list[str]

    def filename(self) -> str:
        """Overrides the corresponding method in the parent class."""
        # ECMWF does not have explicit filenames when using the MARS API
        # * As such, name manually based on their inittime and area covered
        #   e.g. `ecmwf_uk_20210101T0000.grib`
        return f"ecmwf_{self.area}_{self.inittime.strftime('%Y%m%dT%H%M')}.grib"

    def filepath(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return ""

    def it(self) -> dt.datetime:
        """Overrides the corresponding method in the parent class."""
        return self.inittime

    def variables(self) -> list[str]:
        """Overrides the corresponding method in the parent class."""
        return self.params

    def steps(self) -> list[int]:
        """Overrides the corresponding method in the parent class."""
        raise NotImplementedError()
