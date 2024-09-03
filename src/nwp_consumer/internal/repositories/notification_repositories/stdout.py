"""Stdout notification_repositories store."""

import logging

from returns.result import Result, ResultE

from nwp_consumer.internal import entities
from nwp_consumer.internal.ports import NotificationRepository

log = logging.getLogger("nwp-consumer")


class StdoutNotificationRepository(NotificationRepository):
    """Stdout notification_repositories repository."""

    def notify(
        self,
        message: entities.StoreCreatedNotification | entities.StoreAppendedNotification,
    ) -> ResultE[str]:
        """Overrides the corresponding method in the parent class."""
        log.info(f"{message}")
        return Result.from_value("Notification sent to stdout successfully.")

