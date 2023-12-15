import datetime as dt

from nwp_consumer import internal


class NOAAFileInfo(internal.FileInfoModel):
    def __init__(
        self, it: dt.datetime, filename: str, currentURL: str, step: int,
    ) -> "NOAAFileInfo":
        self._it = it
        # The name of the file when stored by the storer. We decompress from bz2
        # at download time, so we don't want that extension on the filename.
        self._filename = filename
        self._url = currentURL
        self.step = step

    def filename(self) -> str:
        return self._filename

    def filepath(self) -> str:
        # The filename in the fully-qualified filepath still has the .bz2 extension
        # so add it back in
        return self._url + "/" + self._filename

    def it(self) -> dt.datetime:
        return self._it
