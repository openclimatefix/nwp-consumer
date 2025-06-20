"""Adaptor for the CLI driving actor."""

import argparse
import datetime as dt
import logging

from returns.result import Failure, ResultE

from nwp_consumer.internal import ports, services

log = logging.getLogger("nwp-consumer")


class CLIHandler:
    """CLI driving actor."""

    model_adaptor: type[ports.RawRepository]
    notification_adaptor: type[ports.NotificationRepository]

    def __init__(
        self,
        model_adaptor: type[ports.RawRepository],
        notification_adaptor: type[ports.NotificationRepository],
    ) -> None:
        """Create a new instance."""
        self.model_adaptor = model_adaptor
        self.notification_adaptor = notification_adaptor

    @property
    def parser(self) -> argparse.ArgumentParser:
        """Return the CLI argument parser."""
        parser = argparse.ArgumentParser(description="NWP Consumer CLI")
        subparsers = parser.add_subparsers(dest="command")

        consume_command = subparsers.add_parser(
            "consume",
            help="Consume NWP data for a single init time",
        )
        consume_command.add_argument(
            "--init-time",
            "-i",
            help="Initialization time of the forecast (YYYY-MM-DDTHH). "
            "Omit to pull the latest available forecast.",
            type=dt.datetime.fromisoformat,
            required=False,
        )
        consume_command.add_argument(
            "--keep-failed",
            help="Don't delete the data if consuming fails",
            action="store_true",
        )

        archive_command = subparsers.add_parser(
            "archive",
            help="Archive NWP data for a given month",
        )
        archive_command.add_argument(
            "--year",
            "-y",
            help="Year to archive",
            type=int,
            required=True,
        )
        archive_command.add_argument(
            "--month",
            "-m",
            help="Month to archive",
            type=int,
            required=True,
        )
        archive_command.add_argument(
            "--keep-failed",
            help="Don't delete the data if archiving fails",
            action="store_true",
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
                service_result = services.ConsumerService.from_adaptors(
                    model_adaptor=self.model_adaptor,
                    notification_adaptor=self.notification_adaptor,
                )
                result: ResultE[str] = service_result.do(
                    consume_result
                    for service in service_result
                    for consume_result in service.consume(
                        period=args.init_time,
                        delete_on_failure=not args.keep_failed,
                    )
                )
                if isinstance(result, Failure):
                    log.error(f"Failed to consume NWP data: {result!s}")
                    return 1

            case "archive":
                service_result = services.ConsumerService.from_adaptors(
                    model_adaptor=self.model_adaptor,
                    notification_adaptor=self.notification_adaptor,
                )
                period: dt.date = dt.date(args.year, args.month, 1)
                result = service_result.do(
                    consume_result
                    for service in service_result
                    for consume_result in service.consume(
                        period=period,
                        delete_on_failure=not args.keep_failed,
                    )
                )
                if isinstance(result, Failure):
                    log.error(f"Failed to archive NWP data: {result!s}")
                    return 1

            case "info":
                log.error("Info command is coming soon! :)")

            case _:
                log.error(f"Unknown command: {args.command}")
                self.parser.print_help()
                return 1

        return 0
