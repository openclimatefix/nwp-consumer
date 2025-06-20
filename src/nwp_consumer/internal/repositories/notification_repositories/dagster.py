"""Dagster pipes notification repository implementation.

`Dagster Pipes <https://docs.dagster.io/_apidocs/pipes#dagster-pipes>`_
enables integration with Dagster for reporting asset materialization
and logging. This module enables dagster instances running this code to recieve
notifications.

See Also:
    - https://docs.dagster.io/concepts/dagster-pipes/subprocess/create-subprocess-asset
"""

import logging
from typing import override

from dagster_pipes import open_dagster_pipes
from returns.result import ResultE, Success

from nwp_consumer.internal import entities, ports

log = logging.getLogger("nwp-consumer")


class DagsterPipesNotificationRepository(ports.NotificationRepository):
    """Dagster pipes notification repository."""

    @override
    def notify(
        self,
        message: entities.StoreCreatedNotification | entities.StoreAppendedNotification,
    ) -> ResultE[str]:
        with open_dagster_pipes() as pipesctx:
            pipesctx.report_asset_materialization(
                metadata={
                    "filename": {"raw_value": message.filename, "type": "text"},
                    "size_mb": {"raw_value": message.size_mb, "type": "float"},
                    "memory_mb": {"raw_value": message.performance.memory_mb, "type": "float"},
                    "duration_minutes": {
                        "raw_value": int(message.performance.duration_seconds / 60),
                        "type": "int",
                    },
                },
            )
        return Success("Notification sent to dagster successfully.")
