"""Stdout notification store."""

import logging

from returns.io import IOResultE, IOSuccess

from nwp_consumer.internal.core import domain
from nwp_consumer.internal.core.ports import NotificationRepository

log = logging.getLogger(__package__)


class StdoutNotificationRepository(NotificationRepository):
    """Stdout notification repository."""

    def notify(
            self,
            message: domain.StoreCreatedNotification | domain.StoreAppendedNotification,
    ) -> IOResultE[str]:
        """Overrides the corresponding method in the parent class."""
        log.info(f"Sent notification (type: {message.__class__.__name__}): {message}")
        return IOSuccess("Notification sent to stdout successfully.")
