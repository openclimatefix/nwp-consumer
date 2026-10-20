import datetime as dt
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import icechunk
import xarray as xr
from returns.pipeline import is_successful
from returns.result import Failure, ResultE, Success

from nwp_consumer.internal import entities
from nwp_consumer.internal.services.consumer_service import ConsumerService

from ._dummy_adaptors import DummyNotificationRepository, DummyRawRepository


class TestParallelConsumer(unittest.TestCase):
    def test_consume(self) -> None:
        """Test the consume method of the ParallelConsumer class."""

        test_consumer = ConsumerService.from_adaptors(
            model_adaptor=DummyRawRepository,
            notification_adaptor=DummyNotificationRepository,
        ).unwrap()

        result = test_consumer.consume(period=dt.datetime(2021, 1, 1, tzinfo=dt.UTC))

        self.assertTrue(is_successful(result), msg=result)

        da: xr.DataArray = xr.open_dataarray(result.unwrap(), engine="zarr", consolidated=False)

        self.assertEqual(
            list(da.sizes.keys()),
            ["init_time", "step", "variable", "latitude", "longitude"],
        )

        path = result.unwrap()
        shutil.rmtree(path)


class TestIcechunkConsumer(unittest.TestCase):
    """Test the consume method against the Icechunk backend."""

    def setUp(self) -> None:
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        env_patch = patch.dict(os.environ, {"ZARRDIR": tmpdir.name, "STORE_BACKEND": "icechunk"})
        env_patch.start()
        self.addCleanup(env_patch.stop)

        self.consumer = ConsumerService.from_adaptors(
            model_adaptor=DummyRawRepository,
            notification_adaptor=DummyNotificationRepository,
        ).unwrap()

    @staticmethod
    def _commit_messages(path: str) -> list[str]:
        repo = icechunk.Repository.open(icechunk.local_filesystem_storage(path))
        return [snapshot.message for snapshot in repo.ancestry(branch="main")]

    def test_consume_commits_once_per_init_time(self) -> None:
        """Test each init time is written in a single commit to the month's store."""
        result = self.consumer.consume(period=dt.datetime(2021, 1, 1, tzinfo=dt.UTC))
        self.assertTrue(is_successful(result), msg=result)
        path = result.unwrap()
        self.assertEqual(os.path.basename(path), "2021-01.icechunk")

        # A later run in the same month adds to the same store
        result = self.consumer.consume(period=dt.datetime(2021, 1, 2, 6, tzinfo=dt.UTC))
        self.assertTrue(is_successful(result), msg=result)
        self.assertEqual(result.unwrap(), path)

        messages = self._commit_messages(path)
        write_messages = [m for m in messages if m.startswith("Write")]
        self.assertListEqual(
            write_messages,
            ["Write init time 2021-01-02T06:00", "Write init time 2021-01-01T00:00"],
        )

    def test_consume_presizes_month(self) -> None:
        """Test the store covers every init time of the month, not just those consumed."""
        result = self.consumer.consume(period=dt.datetime(2021, 2, 1, tzinfo=dt.UTC))
        self.assertTrue(is_successful(result), msg=result)

        repo = icechunk.Repository.open(icechunk.local_filesystem_storage(result.unwrap()))
        ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        running_hours = DummyRawRepository.model().running_hours
        self.assertEqual(ds.sizes["init_time"], 28 * len(running_hours))

    def test_consume_notifications(self) -> None:
        """Test the first run of the month creates the store, and later runs append."""
        its = [dt.datetime(2021, 1, 1, tzinfo=dt.UTC), dt.datetime(2021, 1, 1, 6, tzinfo=dt.UTC)]
        messages = []
        for it in its:
            with patch.object(
                self.consumer.nr,
                "notify",
                wraps=self.consumer.nr.notify,
            ) as notify:
                result = self.consumer.consume(period=it)
            self.assertTrue(is_successful(result), msg=result)
            messages.append(notify.call_args.kwargs["message"])

        self.assertIsInstance(messages[0], entities.StoreCreatedNotification)
        self.assertIsInstance(messages[1], entities.StoreAppendedNotification)
        self.assertListEqual([m.init_times for m in messages], [[its[0]], [its[1]]])
        self.assertTrue(all(m.filename == "2021-01.icechunk" for m in messages))

    def test_maintain(self) -> None:
        """Test maintaining the month's store."""
        result = self.consumer.consume(period=dt.datetime(2021, 1, 1, tzinfo=dt.UTC))
        self.assertTrue(is_successful(result), msg=result)

        maintain_result = self.consumer.maintain(period=dt.date(2021, 1, 1))
        self.assertTrue(is_successful(maintain_result), msg=maintain_result)
        self.assertEqual(maintain_result.unwrap(), result.unwrap())

        with patch.dict(os.environ, {"STORE_BACKEND": "zarr"}):
            self.assertFalse(is_successful(self.consumer.maintain(period=dt.date(2021, 1, 1))))

    def test_fold_dataarrays_generator(self) -> None:
        """Test the number of failed writes is returned, and that no data is a failure."""
        store = MagicMock(spec=entities.TensorStore)
        store.write_to_region.side_effect = [Success(1)] * 19 + [Failure(OSError("Write"))]
        das = [xr.DataArray([1])] * 20
        self.assertEqual(
            ConsumerService._fold_dataarrays_generator(iter([Success(das)]), store).unwrap(),
            1,
        )

        empty_result = ConsumerService._fold_dataarrays_generator(iter([Success([])]), store)
        self.assertIsInstance(empty_result, Failure)

    def test_consume_retries_partial_init_time(self) -> None:
        """Test an init time with failed writes is committed, but retried by later runs."""
        it = dt.datetime(2021, 1, 1, tzinfo=dt.UTC)
        write_to_region = entities.IcechunkTensorStore.write_to_region
        failed_writes: list[bool] = []

        def write_to_region_failing_once(
            store: entities.TensorStore,
            da: xr.DataArray,
        ) -> ResultE[int]:
            if len(failed_writes) == 0:
                failed_writes.append(True)
                return Failure(OSError("Simulated write failure"))
            return write_to_region(store, da=da)

        with patch.object(
            entities.IcechunkTensorStore,
            "write_to_region",
            autospec=True,
            side_effect=write_to_region_failing_once,
        ):
            result = self.consumer.consume(period=it)
        self.assertTrue(is_successful(result), msg=result)

        repo = icechunk.Repository.open(icechunk.local_filesystem_storage(result.unwrap()))
        ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        self.assertFalse(ds["init_time_complete"].sel(init_time="2021-01-01T00").values)

        # A later run retries the init time, and marks it complete
        result = self.consumer.consume(period=it)
        self.assertTrue(is_successful(result), msg=result)
        ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        self.assertTrue(ds["init_time_complete"].sel(init_time="2021-01-01T00").values)

        messages = self._commit_messages(result.unwrap())
        self.assertEqual(messages.count("Write init time 2021-01-01T00:00"), 2)

    def test_consume_non_running_init_time(self) -> None:
        """Test an init time outside the model's running hours is rejected."""
        result = self.consumer.consume(period=dt.datetime(2021, 1, 1, 3, tzinfo=dt.UTC))
        self.assertFalse(is_successful(result))

    def test_consume_failed_validation_commits_nothing(self) -> None:
        """Test an init time failing validation is never committed."""
        with patch.object(
            entities.IcechunkTensorStore,
            "validate_store",
            return_value=Failure(ValueError("Invalid data")),
        ):
            result = self.consumer.consume(period=dt.datetime(2021, 1, 1, tzinfo=dt.UTC))
        self.assertFalse(is_successful(result))

        path = os.path.join(os.environ["ZARRDIR"], "2021-01.icechunk")
        messages = self._commit_messages(path)
        self.assertFalse(any(m.startswith("Write") for m in messages), msg=messages)


if __name__ == "__main__":
    unittest.main()
