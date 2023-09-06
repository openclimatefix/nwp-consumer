import datetime as dt
from dataclasses import dataclass

import internal


@dataclass
class ECMWFMarsFileInfo(internal.FileInfoModel):
    inittime: dt.datetime
    area: str

    def filename(self) -> str:
        return f"ecmwf_{self.area}_{self.inittime.strftime('%Y%m%dT%H%M')}.grib"

    def filepath(self) -> str:
        return ""

    def it(self) -> dt.datetime:
        return self.inittime
