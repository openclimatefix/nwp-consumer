"""Adaptor for the CLI driving actor."""

import argparse
import datetime as dt
import logging
import dataclasses

from returns.result import Failure, Success

from nwp_consumer.internal import entities, ports

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

        info_command = subparsers.add_parser("info", help="Show model repository info")
        info_options = info_command.add_mutually_exclusive_group()
        info_options.add_argument(
            "--model",
            help="Show information about the selected model repository.",
            action="store_true",
        )
        info_options.add_argument(
            "--parameters",
            help="Show information about all available parameters.",
            action="store_true",
        )

        return parser

    def run(self) -> int:
        """Run the CLI handler.

        Returns the appropriate exit code.
        """
        args = self.parser.parse_args()
        match args.command:
            case "consume":
                result = self._consumer_usecase.consume(it=args.init_time)

                match result:
                    case Failure(e):
                        log.error(f"Failed to consume NWP data: {e}")
                        return 1
                    case Success(path):
                        log.info(f"Successfully consumed NWP data: {path.as_posix()}")
                        return 0

            case "info":
                if args.model:
                    log.info(self._consumer_usecase.info())
                    return 0
                if args.parameters:
                    for parameter in [
                        getattr(entities.params, f.name)
                        for f in dataclasses.fields(entities.params)
                    ]:
                        log.info(parameter.__repr__())
                        return 0

            case _:
                log.error(f"Unknown command: {args.command}")
                self.parser.print_help()
                return 1

        return 0
