import datetime as dt
from typing import ClassVar, Type

from marshmallow import EXCLUDE, Schema, fields
from marshmallow_dataclass import dataclass


@dataclass
class CEDAFileInfo:
    class Meta:
        unknown = EXCLUDE

    name: str

    Schema: ClassVar[Type[Schema]] = Schema  # To prevent confusing type checkers

    def initTime(self) -> dt.datetime:
        return dt.datetime.strptime(self.name.split("_")[0], '%Y%m%d%H%M').replace(tzinfo=dt.timezone.utc)

    def fileName(self) -> str:
        return self.name


@dataclass
class CEDAResponse:
    class Meta:
        unknown = EXCLUDE

    path: str
    items: list[CEDAFileInfo] = fields.List(fields.Nested(CEDAFileInfo.Schema()))

    Schema: ClassVar[Type[Schema]] = Schema  # To prevent confusing type checkers
