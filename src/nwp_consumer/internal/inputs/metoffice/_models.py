import datetime as dt
from typing import ClassVar

from marshmallow import EXCLUDE, Schema, fields
from marshmallow_dataclass import dataclass

from nwp_consumer import internal


@dataclass
class MetOfficeFileInfo(internal.FileInfoModel):

    class Meta:
        unknown = EXCLUDE

    fileId: str
    runDateTime: dt.datetime

    Schema: ClassVar[type[Schema]] = Schema  # To prevent confusing type checkers

    def it(self) -> dt.datetime:
        return self.runDateTime.replace(tzinfo=None)

    def filename(self) -> str:
        return self.fileId + ".grib"

    def filepath(self) -> str:
        return f"{self.fileId}/data"




@dataclass
class MetOfficeOrderDetails:

    class Meta:
        unknown = EXCLUDE

    files: list[MetOfficeFileInfo] = fields.List(fields.Nested(MetOfficeFileInfo.Schema()))

    Schema: ClassVar[type[Schema]] = Schema  # To prevent confusing type checkers


@dataclass
class MetOfficeResponse:

    orderDetails: MetOfficeOrderDetails

    Schema: ClassVar[type[Schema]] = Schema  # To prevent confusing type checkers
