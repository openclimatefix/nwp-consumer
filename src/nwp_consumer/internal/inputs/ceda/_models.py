import datetime as dt
from typing import ClassVar

from marshmallow import EXCLUDE, Schema, fields
from marshmallow_dataclass import dataclass

import nwp_consumer.internal as internal


@dataclass
class CEDAFileInfo(internal.FileInfoModel):
    """Schema of the items section of the response from the CEDA API."""

    class Meta:
        unknown = EXCLUDE

    name: str

    Schema: ClassVar[type[Schema]] = Schema  # To prevent confusing type checkers

    def it(self) -> dt.datetime:
        """Return the init time of the file.

        The init time is found the first part of the file name for CEDA files,
        e.g. 202201010000_u1096_ng_umqv_Wholesale1.grib
        """
        return dt.datetime.strptime(self.name.split("_")[0], "%Y%m%d%H%M").replace(
            tzinfo=dt.timezone.utc,
        )

    def filename(self) -> str:
        return self.name

    def filepath(self) -> str:
        return f"badc/ukmo-nwp/data/ukv-grib/{self.it():%Y/%m/%d}/{self.name}"


@dataclass
class CEDAResponse:
    """Schema of the response from the CEDA API."""

    class Meta:
        unknown = EXCLUDE

    path: str
    items: list[CEDAFileInfo] = fields.List(fields.Nested(CEDAFileInfo.Schema()))

    Schema: ClassVar[type[Schema]] = Schema  # To prevent confusing type checkers
