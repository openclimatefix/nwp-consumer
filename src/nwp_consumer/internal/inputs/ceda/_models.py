import datetime as dt
from typing import ClassVar, Type
import nwp_consumer.internal as internal

from marshmallow import EXCLUDE, Schema, fields
from marshmallow_dataclass import dataclass


@dataclass
class CEDAFileInfo(internal.FileInfoModel):
    """Schema of the items section of the response from the CEDA API."""
    class Meta:
        unknown = EXCLUDE

    name: str

    Schema: ClassVar[Type[Schema]] = Schema  # To prevent confusing type checkers

    def initTime(self) -> dt.datetime:
        """Returns the init time of the file.

        The init time is found the first part of the file name for CEDA files,
        e.g. 202201010000_u1096_ng_umqv_Wholesale1.grib
        """
        return dt.datetime.strptime(self.name.split("_")[0], '%Y%m%d%H%M').replace(tzinfo=None)

    def fname(self) -> str:
        """Returns the file name."""
        return self.name


@dataclass
class CEDAResponse:
    """Schema of the response from the CEDA API."""
    class Meta:
        unknown = EXCLUDE

    path: str
    items: list[CEDAFileInfo] = fields.List(fields.Nested(CEDAFileInfo.Schema()))

    Schema: ClassVar[Type[Schema]] = Schema  # To prevent confusing type checkers
