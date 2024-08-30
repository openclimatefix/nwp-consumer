"""Adaptor for the CLI driving actor."""

import argparse
import datetime as dt
import logging

from returns.result import Failure, Success

from nwp_consumer.internal import ports

log = logging.getLogger("nwp-consumer")


class CLIHandler:
    """CLI driving actor."""

    def __init__(self, consumer_usecase: ports.ConsumerUseCase) -> None:
        """Create a new instance."""
        self._consumer_usecase = consumer_usecase


    @property
    def parser(self) -> argparse.ArgumentParser:
        """Return the CLI argument parser."""
        parser = argparse.ArgumentParser(description="NWP Consumer CLI")
        subparsers = parser.add_subparsers(dest="command")

        consume_command = subparsers.add_parser("consume", help="Consume NWP data")
        consume_command.add_argument(
            "--init-time", "-i",
            help="Initialization time of the forecast (YYYY-MM-DDTHH). "
                 "Omit to pull the latest available forecast.",
            type=dt.datetime.fromisoformat,
            required=False,
        )

        return parser

    def run(self) -> None:
        """Run the CLI handler."""
        args = self.parser.parse_args()
        match args.command:
            case "consume":
                result = self._consumer_usecase.consume()

                match result:
                    case Failure(e):
                        log.error(f"Failed to consume NWP data: {e}")
                        exit(1)
                    case Success(path):
                        log.info(f"Successfully consumed NWP data: {path.as_posix()}")
                        exit(0)
            case _:
                log.error(f"Unknown command: {args.command}")
                self.parser.print_help()
                exit(1)
