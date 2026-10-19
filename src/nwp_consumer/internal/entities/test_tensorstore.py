import contextlib
import dataclasses
import datetime as dt
import logging
import os
import pathlib
import tempfile
import unittest
from collections.abc import Generator
from types import TracebackType
from unittest.mock import patch

import icechunk
import numpy as np
import xarray as xr
from botocore.client import BaseClient as BotocoreClient
from botocore.session import Session
from moto.server import ThreadedMotoServer
from returns.pipeline import is_successful
from returns.result import Failure, ResultE, Success

from .coordinates import NWPDimensionCoordinateMap
from .parameters import Parameter
from .postprocess import PostProcessOptions
from .tensorstore import IcechunkTensorStore, TensorStore, ZarrTensorStore

logging.getLogger("werkzeug").setLevel(logging.ERROR)


class MockS3Bucket:
    client: BotocoreClient
    server: ThreadedMotoServer
    bucket: str = "test-bucket"

    def __enter__(self) -> None:
        """Create a mock S3 server and bucket."""
        self.server = ThreadedMotoServer()
        self.server.start()

        session = Session()
        self.client = session.create_client(
            service_name="s3",
            region_name="us-east-1",
            endpoint_url="http://localhost:5000",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )

        self.client.create_bucket(
            Bucket=self.bucket,
        )

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        response = self.client.list_objects_v2(
            Bucket=self.bucket,
        )
        """Delete all objects in the bucket and stop the server."""
        if "Contents" in response:
            for obj in response["Contents"]:
                self.client.delete_object(
                    Bucket=self.bucket,
                    Key=obj["Key"],
                )
        self.server.stop()


def make_coords(year: int) -> NWPDimensionCoordinateMap:
    """Create test coordinates covering four init times in the given year."""
    return NWPDimensionCoordinateMap(
        init_time=[dt.datetime(year, 1, 1, h, tzinfo=dt.UTC) for h in [0, 6, 12, 18]],
        step=[1, 2, 3, 4],
        variable=[Parameter.TEMPERATURE_SL],
        latitude=np.linspace(90, -90, 12).tolist(),
        longitude=np.linspace(0, 360, 18).tolist(),
    )


def init_store(
    store_cls: type[TensorStore],
    coords: NWPDimensionCoordinateMap,
) -> ResultE[TensorStore]:
    """Initialize a store of the given class for the given coordinates."""
    return store_cls.initialize_empty_store(
        model="test_da",
        repository="dummy_repository",
        coords=coords,
        chunks=coords.chunking(chunk_count_overrides={}),
    )


@contextlib.contextmanager
def make_store(store_cls: type[TensorStore], year: int) -> Generator[TensorStore, None, None]:
    """Create an instance of the given TensorStore class, deleting it afterwards."""
    store = init_store(store_cls, make_coords(year)).unwrap()
    try:
        yield store
    finally:
        store.delete_store()


def init_time_dataarray(ts: TensorStore, it: dt.datetime) -> xr.DataArray:
    """Create a dataarray of ones covering a complete init time of the store."""
    return xr.DataArray(
        name="test_da",
        data=np.ones(
            shape=[1 if k == "init_time" else v for k, v in ts.coordinate_map.shapemap.items()],
        ),
        coords=ts.coordinate_map.to_pandas()
        | {"init_time": [np.datetime64(it.replace(tzinfo=None), "ns")]},
    )


class TestTensorStore(unittest.TestCase):
    """Test the business methods of the TensorStore class."""

    store_cls: type[TensorStore] = ZarrTensorStore

    def store(self, year: int) -> contextlib.AbstractContextManager[TensorStore]:
        """Create an instance of the TensorStore class under test."""
        return make_store(self.store_cls, year)

    @patch.dict(
        os.environ,
        {
            "AWS_ENDPOINT_URL": "http://localhost:5000",
            "AWS_ACCESS_KEY_ID": "test-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
            "ZARRDIR": "s3://test-bucket/data",
        },
        clear=True,
    )
    def test_initialize_and_delete_s3(self) -> None:
        """Test the initialize_empty_store method."""

        with MockS3Bucket(), self.store(year=2022) as ts:
            delete_result = ts.delete_store()
            self.assertIsInstance(delete_result, Success, msg=delete_result)

    def test_write_to_region(self) -> None:
        """Test the write_to_region method."""
        with self.store(year=2022) as ts:
            test_da: xr.DataArray = xr.DataArray(
                name="test_da",
                data=np.ones(
                    shape=list(ts.coordinate_map.shapemap.values()),
                ),
                coords=ts.coordinate_map.to_pandas(),
            )

            # Write each init time and step one at a time
            for it in test_da.coords["init_time"].values:
                ts.begin_transaction().unwrap()
                for step in test_da.coords["step"].values:
                    write_result = ts.write_to_region(
                        da=test_da.where(
                            test_da["init_time"] == it,
                            drop=True,
                        ).where(test_da["step"] == step, drop=True),
                    )
                    self.assertIsInstance(write_result, Success, msg=write_result)
                commit_result = ts.commit(message=f"Write {it}")
                self.assertIsInstance(commit_result, Success, msg=commit_result)

    def test_postprocess(self) -> None:
        """Test the postprocess method."""

        @dataclasses.dataclass
        class TestCase:
            name: str
            options: PostProcessOptions
            should_error: bool

        tests: list[TestCase] = [
            TestCase(
                name="empty_options",
                options=PostProcessOptions(),
                should_error=False,
            ),
        ]

        with self.store(year=1971) as ts:
            for t in tests:
                with self.subTest(name=t.name):
                    result = ts.postprocess(t.options)
                    if t.should_error:
                        self.assertTrue(
                            isinstance(result, Exception),
                            msg="Expected error to be returned.",
                        )
                    else:
                        self.assertTrue(is_successful(result))

    def _write_init_time(self, ts: TensorStore, it: dt.datetime) -> None:
        """Write and commit a complete init time of ones to the store."""
        ts.begin_transaction().unwrap()
        ts.write_to_region(da=init_time_dataarray(ts, it)).unwrap()
        ts.commit(message=f"Write {it}").unwrap()

    def test_reuse_existing_store(self) -> None:
        """Test reopening an existing store with a subset of its init times."""
        with self.store(year=2023) as ts:
            self.assertTrue(ts.created)
            its = ts.coordinate_map.init_time
            subset_coords = dataclasses.replace(ts.coordinate_map, init_time=[its[2]])

            # Zarr stores are named for their init times, so force the subset onto the store
            with patch.object(
                self.store_cls,
                "gen_store_filename",
                return_value=pathlib.Path(ts.path).name,
            ):
                reopen_result = self.store_cls.initialize_empty_store(
                    model="test_da",
                    repository="dummy_repository",
                    coords=subset_coords,
                    chunks=subset_coords.chunking(chunk_count_overrides={}),
                )
            self.assertIsInstance(reopen_result, Success, msg=reopen_result)
            reopened = reopen_result.unwrap()

            self.assertFalse(reopened.created)
            self.assertEqual(reopened.size_kb, 0)
            self.assertEqual(reopened.coordinate_map, ts.coordinate_map)

            # The write must land in the third init time, not the first
            self._write_init_time(reopened, its[2])
            self.assertListEqual(ts.missing_times().unwrap(), [its[0], its[1], its[3]])

    def test_existing_store_not_covering_coords(self) -> None:
        """Test reopening an existing store with init times it does not hold."""
        with self.store(year=2023) as ts:
            other_coords = dataclasses.replace(
                ts.coordinate_map,
                init_time=[dt.datetime(2023, 2, 1, tzinfo=dt.UTC)],
            )
            with patch.object(
                self.store_cls,
                "gen_store_filename",
                return_value=pathlib.Path(ts.path).name,
            ):
                reopen_result = self.store_cls.initialize_empty_store(
                    model="test_da",
                    repository="dummy_repository",
                    coords=other_coords,
                    chunks=other_coords.chunking(chunk_count_overrides={}),
                )
            self.assertIsInstance(reopen_result, Failure)

    def test_missing_times_subset(self) -> None:
        """Test the missing_times method restricted to given init times."""
        with self.store(year=2023) as ts:
            its = ts.coordinate_map.init_time
            self._write_init_time(ts, its[0])

            self.assertListEqual(ts.missing_times(init_times=[its[0], its[1]]).unwrap(), [its[1]])
            self.assertListEqual(ts.missing_times(init_times=[its[0]]).unwrap(), [])

            unknown_result = ts.missing_times(init_times=[dt.datetime(2000, 1, 1, tzinfo=dt.UTC)])
            self.assertIsInstance(unknown_result, Failure)

    def test_validate_store(self) -> None:
        """Test the validate_store method only validates the given init times."""
        with self.store(year=2023) as ts:
            its = ts.coordinate_map.init_time
            self._write_init_time(ts, its[1])

            # The other, unwritten, init times must not cause the validation to fail
            valid_result = ts.validate_store(init_times=[its[1]])
            self.assertIsInstance(valid_result, Success, msg=valid_result)

            invalid_result = ts.validate_store(init_times=[its[0]])
            self.assertIsInstance(invalid_result, Failure)

    def test_missing_times(self) -> None:
        """Test the missing_times method."""

        @dataclasses.dataclass
        class TestCase:
            name: str
            times_to_write: list[dt.datetime]
            expected: list[dt.datetime]

        with self.store(year=2024) as ts:
            tests: list[TestCase] = [
                TestCase(
                    name="all_missing_times",
                    times_to_write=[],
                    expected=ts.coordinate_map.init_time,
                ),
                TestCase(
                    name="some_missing_times",
                    times_to_write=[ts.coordinate_map.init_time[0], ts.coordinate_map.init_time[2]],
                    expected=[ts.coordinate_map.init_time[1], ts.coordinate_map.init_time[3]],
                ),
                TestCase(
                    name="no_missing_times",
                    times_to_write=ts.coordinate_map.init_time,
                    expected=[],
                ),
            ]

            for t in tests:
                with self.subTest(name=t.name):
                    for i in t.times_to_write:
                        self._write_init_time(ts, i)
                    result = ts.missing_times()
                    missing_times = result.unwrap()
                    self.assertListEqual(missing_times, t.expected)


class TestZarrTensorStore(unittest.TestCase):
    """Test the behaviour specific to the Zarr backend."""

    def test_rollback(self) -> None:
        """Test rollback only deletes stores created by the instance."""
        with make_store(ZarrTensorStore, year=2023) as ts:
            pre_existing = dataclasses.replace(ts, created=False)
            self.assertIsInstance(pre_existing.rollback(), Success)
            self.assertTrue(pathlib.Path(ts.path).exists())

            self.assertIsInstance(ts.rollback(), Success)
            self.assertFalse(pathlib.Path(ts.path).exists())


class TestIcechunkTensorStore(TestTensorStore):
    """Run the TensorStore business-method suite against the Icechunk backend.

    Also tests the transactional behaviour specific to the Icechunk backend.
    """

    store_cls: type[TensorStore] = IcechunkTensorStore

    @staticmethod
    def _committed_dataarray(ts: TensorStore) -> xr.DataArray:
        """Read the store as an independent reader, seeing only committed data."""
        if not isinstance(ts, IcechunkTensorStore):
            raise TypeError(f"Expected an IcechunkTensorStore, got {type(ts)}")
        session = ts.repo.readonly_session("main")
        return xr.open_zarr(session.store, consolidated=False)[ts.name]

    def test_uncommitted_writes_not_visible(self) -> None:
        """Test writes are only visible to readers once committed."""
        with self.store(year=2023) as ts:
            it = ts.coordinate_map.init_time[0]
            it_np = np.datetime64(it.replace(tzinfo=None), "ns")
            ts.begin_transaction().unwrap()
            ts.write_to_region(da=init_time_dataarray(ts, it)).unwrap()

            # The open transaction can be validated before it is committed
            validate_result = ts.validate_store(init_times=[it])
            self.assertIsInstance(validate_result, Success, msg=validate_result)
            self.assertTrue(self._committed_dataarray(ts).sel(init_time=it_np).isnull().all())

            ts.commit(message="Write init time").unwrap()
            self.assertTrue(self._committed_dataarray(ts).sel(init_time=it_np).notnull().all())

    def test_rollback(self) -> None:
        """Test rollback discards the open transaction, leaving the store in place."""
        with self.store(year=2023) as ts:
            its = ts.coordinate_map.init_time
            self._write_init_time(ts, its[0])

            ts.begin_transaction().unwrap()
            ts.write_to_region(da=init_time_dataarray(ts, its[1])).unwrap()
            self.assertIsInstance(ts.rollback(), Success)

            self.assertTrue(pathlib.Path(ts.path).exists())
            self.assertListEqual(ts.missing_times().unwrap(), its[1:])
            # A new transaction can be opened after a rollback
            self.assertIsInstance(ts.begin_transaction(), Success)

    def test_transaction_required(self) -> None:
        """Test writes and commits fail outside of a single open transaction."""
        with self.store(year=2023) as ts:
            da = init_time_dataarray(ts, ts.coordinate_map.init_time[0])
            self.assertIsInstance(ts.write_to_region(da=da), Failure)
            self.assertIsInstance(ts.commit(message="Nothing"), Failure)

            ts.begin_transaction().unwrap()
            self.assertIsInstance(ts.begin_transaction(), Failure)

    def test_monthly_store(self) -> None:
        """Test stores are named by month, and cannot span more than one month."""
        with self.store(year=2023) as ts:
            self.assertEqual(pathlib.Path(ts.path).name, "2023-01.icechunk")

            # Init times from the same month are found in the same store
            subset_coords = dataclasses.replace(
                ts.coordinate_map,
                init_time=[ts.coordinate_map.init_time[1]],
            )
            reopened = init_store(IcechunkTensorStore, subset_coords).unwrap()
            self.assertEqual(reopened.path, ts.path)
            self.assertFalse(reopened.created)

        spanning_coords = dataclasses.replace(
            make_coords(year=2023),
            init_time=[
                dt.datetime(2023, 1, 31, 18, tzinfo=dt.UTC),
                dt.datetime(2023, 2, 1, 0, tzinfo=dt.UTC),
            ],
        )
        self.assertIsInstance(init_store(IcechunkTensorStore, spanning_coords), Failure)

    def test_concurrent_transactions(self) -> None:
        """Test concurrent writers to different init times are both committed."""
        with self.store(year=2023) as ts:
            other = init_store(IcechunkTensorStore, ts.coordinate_map).unwrap()
            its = ts.coordinate_map.init_time

            ts.begin_transaction().unwrap()
            other.begin_transaction().unwrap()
            ts.write_to_region(da=init_time_dataarray(ts, its[0])).unwrap()
            other.write_to_region(da=init_time_dataarray(other, its[1])).unwrap()

            self.assertIsInstance(ts.commit(message="Write first"), Success)
            other_commit_result = other.commit(message="Write second")
            self.assertIsInstance(other_commit_result, Success, msg=other_commit_result)
            self.assertListEqual(ts.missing_times().unwrap(), its[2:])

    def test_concurrent_transactions_same_chunks(self) -> None:
        """Test a concurrent writer to the same chunks fails to commit."""
        with self.store(year=2023) as ts:
            other = init_store(IcechunkTensorStore, ts.coordinate_map).unwrap()
            it = ts.coordinate_map.init_time[0]

            ts.begin_transaction().unwrap()
            other.begin_transaction().unwrap()
            ts.write_to_region(da=init_time_dataarray(ts, it)).unwrap()
            other.write_to_region(da=init_time_dataarray(other, it) * 2).unwrap()

            self.assertIsInstance(ts.commit(message="Write first"), Success)
            self.assertIsInstance(other.commit(message="Write second"), Failure)
            self.assertIsInstance(other.rollback(), Success)

            committed = self._committed_dataarray(ts)
            it_np = np.datetime64(it.replace(tzinfo=None), "ns")
            self.assertTrue((committed.sel(init_time=it_np) == 1).all())

    def test_concurrent_store_creation(self) -> None:
        """Test a store created by another writer during initialization is used."""
        coords = make_coords(year=2023)
        open_existing = IcechunkTensorStore._open_existing_dataarray
        race_started: list[bool] = []
        racing_stores: list[TensorStore] = []

        def open_existing_with_race(
            session: icechunk.Session,
            name: str,
        ) -> ResultE[xr.DataArray]:
            # Another writer creates the store after this one has opened its session
            if len(race_started) == 0:
                race_started.append(True)
                racing_stores.append(init_store(IcechunkTensorStore, coords).unwrap())
            return open_existing(session=session, name=name)

        with (
            tempfile.TemporaryDirectory() as tmpdir,
            patch.dict(os.environ, {"ZARRDIR": tmpdir}),
            patch.object(
                IcechunkTensorStore,
                "_open_existing_dataarray",
                side_effect=open_existing_with_race,
            ),
        ):
            result = init_store(IcechunkTensorStore, coords)

            self.assertIsInstance(result, Success, msg=result)
            store = result.unwrap()
            self.assertFalse(store.created)
            self.assertTrue(racing_stores[0].created)
            if not isinstance(store, IcechunkTensorStore):
                raise TypeError(f"Expected an IcechunkTensorStore, got {type(store)}")
            messages = [s.message for s in store.repo.ancestry(branch="main")]
            self.assertEqual(len([m for m in messages if m.startswith("Initialize")]), 1)

    def test_concurrent_repository_creation(self) -> None:
        """Test a repository created by another writer during initialization is used."""
        open_or_create = icechunk.Repository.open_or_create

        def open_or_create_with_race(
            storage: icechunk.Storage,
            config: icechunk.RepositoryConfig | None = None,
        ) -> icechunk.Repository:
            # Another writer creates the repository between the existence check and creation
            open_or_create(storage, config=config)
            raise icechunk.IcechunkError("repository already exists")

        with (
            tempfile.TemporaryDirectory() as tmpdir,
            patch.dict(os.environ, {"ZARRDIR": tmpdir}),
            patch.object(
                icechunk.Repository,
                "open_or_create",
                side_effect=open_or_create_with_race,
            ),
        ):
            result = init_store(IcechunkTensorStore, make_coords(year=2023))
            self.assertIsInstance(result, Success, msg=result)

    def test_manifest_splitting(self) -> None:
        """Test each init time is written to its own manifest."""
        with self.store(year=2023) as ts:
            if not isinstance(ts, IcechunkTensorStore):
                raise TypeError(f"Expected an IcechunkTensorStore, got {type(ts)}")
            self._write_init_time(ts, ts.coordinate_map.init_time[0])
            self._write_init_time(ts, ts.coordinate_map.init_time[1])

            manifests = ts.repo.list_manifest_files(ts.repo.lookup_branch("main"))
            # Coordinate arrays are a single chunk, so have manifests with a single ref
            data_manifest_refs = [m.num_chunk_refs for m in manifests if m.num_chunk_refs > 1]
            chunks_per_init_time = len(ts.coordinate_map.step)
            self.assertListEqual(data_manifest_refs, [chunks_per_init_time] * 2)

    def test_maintain(self) -> None:
        """Test maintenance deletes only unreferenced objects, after the grace period."""
        with self.store(year=2023) as ts:
            if not isinstance(ts, IcechunkTensorStore):
                raise TypeError(f"Expected an IcechunkTensorStore, got {type(ts)}")
            its = ts.coordinate_map.init_time
            self._write_init_time(ts, its[0])
            # Random data doesn't compress enough to be inlined into the manifest,
            # so is written to chunk objects
            orphan_da = init_time_dataarray(ts, its[1])
            orphan_da = orphan_da.copy(data=np.random.default_rng().random(orphan_da.shape))
            ts.begin_transaction().unwrap()
            ts.write_to_region(da=orphan_da).unwrap()
            ts.rollback().unwrap()

            def maintain(expire: dt.timedelta | None = None) -> ResultE[str]:
                return IcechunkTensorStore.maintain(
                    model="test_da",
                    repository="dummy_repository",
                    month=dt.date(2023, 1, 1),
                    expire_snapshots_older_than=expire,
                )

            def count_chunk_objects() -> int:
                chunks_dir = pathlib.Path(ts.path, "chunks")
                return len(list(chunks_dir.iterdir())) if chunks_dir.exists() else 0

            # The rolled back chunks are within the grace period, so are kept
            chunks_before = count_chunk_objects()
            self.assertIsInstance(maintain(), Success)
            self.assertEqual(count_chunk_objects(), chunks_before)

            with patch(
                f"{IcechunkTensorStore.__module__}.ICECHUNK_GC_GRACE_PERIOD",
                dt.timedelta(0),
            ):
                self.assertIsInstance(maintain(), Success)
                self.assertEqual(
                    count_chunk_objects(),
                    chunks_before - len(ts.coordinate_map.step),
                )

                # History is only expired when requested
                history_length = len(list(ts.repo.ancestry(branch="main")))
                self.assertIsInstance(maintain(expire=dt.timedelta(0)), Success)
                self.assertLess(len(list(ts.repo.ancestry(branch="main"))), history_length)

            # The committed data is unaffected
            self.assertListEqual(ts.missing_times().unwrap(), its[1:])

        missing_result = IcechunkTensorStore.maintain(
            model="test_da",
            repository="dummy_repository",
            month=dt.date(1999, 1, 1),
        )
        self.assertIsInstance(missing_result, Failure)


if __name__ == "__main__":
    unittest.main()
