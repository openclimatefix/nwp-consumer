import datetime as dt

from nwp_consumer import internal


class NOAAFileInfo(internal.FileInfoModel):
    def __init__(
        self, it: dt.datetime, filename: str, currentURL: str, step: int,
    ) -> "NOAAFileInfo":
        self._it = it
        self._filename = filename
        self._url = currentURL
        self.step = step

    def filename(self) -> str:
        return self._filename

    def filepath(self) -> str:
        return self._url + "/" + self._filename

    def it(self) -> dt.datetime:
        return self._it
