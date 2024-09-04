"""Stdout notification_repositories store."""

import logging
from typing import override

from returns.result import Result, ResultE

from nwp_consumer.internal import entities, ports

log = logging.getLogger("nwp-consumer")


class StdoutNotificationRepository(ports.NotificationRepository):
    """Stdout notification_repositories repository."""

    @override
    def notify(
        self,
        message: entities.StoreCreatedNotification | entities.StoreAppendedNotification,
    ) -> ResultE[str]:
        """See parent class."""
        log.info(f"{message}")
        return Result.from_value("Notification sent to stdout successfully.")

