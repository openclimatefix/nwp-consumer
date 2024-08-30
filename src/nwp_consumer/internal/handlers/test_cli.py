import datetime as dt
import pathlib
import unittest

from returns.result import ResultE

from nwp_consumer.internal import entities, ports

from .cli import CLIArgs, CLIHandler


class DummyConsumerService(ports.ConsumerUseCase):

    def consume(self, it: dt.datetime | None = None) -> ResultE[pathlib.Path]:
        return ResultE.success(pathlib.Path(f"dummy_{it:%Y%m%dT%H}.zarr"))

    def postprocess(self, options: entities.PostProcessOptions) -> ResultE[str]:
        pass



class TestCLIHandler(unittest.TestCase):

    c: CLIHandler = CLIHandler(
        consumer_usecase=DummyConsumerService(),
    )

    def test_parser(self):


        args = self.c.parse_args(["--help"])
        self.assertEqual(args, CLIArgs(command="consume", init_time=dt.datetime(2021, 1, 1), source="mo-global"))