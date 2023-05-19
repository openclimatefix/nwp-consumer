import datetime as dt
from typing import ClassVar, Type

from marshmallow import EXCLUDE, Schema, fields
from marshmallow_dataclass import dataclass


@dataclass
class MetOfficeFileInfo:

    class Meta:
        unknown = EXCLUDE

    fileId: str
    runDateTime: dt.datetime

    Schema: ClassVar[Type[Schema]] = Schema  # To prevent confusing type checkers

    def initTime(self) -> dt.datetime:
        """Returns the init time of the file."""
        return self.runDateTime.replace(tzinfo=dt.timezone.utc)

    def fname(self) -> str:
        """Returns the file name."""
        return self.fileId


@dataclass
class MetOfficeOrderDetails:

    class Meta:
        unknown = EXCLUDE

    files: list[MetOfficeFileInfo] = fields.List(fields.Nested(MetOfficeFileInfo.Schema()))

    Schema: ClassVar[Type[Schema]] = Schema  # To prevent confusing type checkers


@dataclass
class MetOfficeResponse:

    orderDetails: MetOfficeOrderDetails

    Schema: ClassVar[Type[Schema]] = Schema  # To prevent confusing type checkers
