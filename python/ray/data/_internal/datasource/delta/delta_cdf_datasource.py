"""
Delta Lake Change Data Feed (CDF) datasource with streaming execution.
"""

import logging
from typing import Any, Dict, Iterable, List, Optional

import pyarrow as pa

from ray.data.block import Block, BlockMetadata
from ray.data.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)


class DeltaCDFDatasource(Datasource):
    """Datasource for reading Delta Lake Change Data Feed with streaming execution."""

    def __init__(
        self,
        path: str,
        *,
        starting_version: int = 0,
        ending_version: Optional[int] = None,
        storage_options: Optional[Dict[str, str]] = None,
        columns: Optional[List[str]] = None,
        predicate: Optional[str] = None,
    ):
        """Initialize CDF datasource.

        Args:
            path: Path to Delta Lake table.
            starting_version: Starting version for CDF reads (default: 0).
            ending_version: Ending version for CDF reads (None = latest version).
            storage_options: Cloud storage authentication credentials.
            columns: List of column names to read (None = all columns).
            predicate: SQL predicate for filtering CDF records.
        """
        from ray.data._internal.util import _check_import

        _check_import(self, module="deltalake", package="deltalake")

        self.path = path
        self.starting_version = starting_version
        self.ending_version = ending_version
        self.storage_options = storage_options or {}
        self.columns = columns
        self.predicate = predicate

    def get_read_tasks(
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List[ReadTask]:
        """Create streaming ReadTask objects for CDF reading."""
        from deltalake import DeltaTable

        dt = DeltaTable(self.path, storage_options=self.storage_options)
        actual_ending_version = (
            self.ending_version if self.ending_version is not None else dt.version()
        )

        version_range = actual_ending_version - self.starting_version + 1
        if version_range <= 0:
            logger.info(
                f"No CDF versions to read: starting={self.starting_version}, ending={actual_ending_version}"
            )
            return []

        num_tasks = min(parallelism, version_range)
        versions_per_task = max(1, version_range // num_tasks)

        logger.info(
            f"Creating {num_tasks} CDF read tasks for version range {self.starting_version}-{actual_ending_version}"
        )

        read_tasks = []
        for i in range(num_tasks):
            chunk_start = self.starting_version + i * versions_per_task
            # Last task covers all remaining versions up to actual_ending_version
            if i == num_tasks - 1:
                chunk_end = actual_ending_version
            else:
                chunk_end = self.starting_version + (i + 1) * versions_per_task - 1

            if chunk_start > chunk_end:
                continue

            read_fn = self._make_cdf_read_fn(chunk_start, chunk_end)
            metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(read_fn, metadata, per_task_row_limit=per_task_row_limit)
            )

        return read_tasks

    def _make_cdf_read_fn(self, start_ver: int, end_ver: int) -> callable:
        """Create a CDF read function for a specific version range."""

        def read_cdf_range() -> Iterable[Block]:
            """Read CDF for version range and yield blocks incrementally."""
            import pyarrow as pa
            from deltalake import DeltaTable

            dt = DeltaTable(self.path, storage_options=self.storage_options)

            try:
                cdf_reader = dt.load_cdf(
                    starting_version=start_ver,
                    ending_version=end_ver,
                    columns=self.columns,
                    predicate=self.predicate,
                    allow_out_of_range=False,
                )

                batch_count = 0
                for batch in cdf_reader:
                    if batch.num_rows > 0:
                        batch_count += 1
                        yield pa.Table.from_batches([batch])

                if batch_count == 0:
                    logger.debug(
                        f"No CDF data in version range {start_ver}-{end_ver}, yielding empty table with schema"
                    )
                    yield self._create_empty_cdf_table(dt)

            except Exception as e:
                logger.warning(
                    f"Error reading CDF for versions {start_ver}-{end_ver}: {e}. Yielding empty table with schema."
                )
                yield self._create_empty_cdf_table(dt)

        return read_cdf_range

    def _create_empty_cdf_table(self, dt: Any) -> pa.Table:
        """Create an empty CDF table with proper schema."""
        arrow_schema = dt.schema().to_pyarrow()

        if self.columns:
            available_fields = []
            for col in self.columns:
                try:
                    available_fields.append(arrow_schema.field(col))
                except KeyError:
                    logger.debug(f"Column {col} not found in table schema, skipping")
            if available_fields:
                arrow_schema = pa.schema(available_fields)

        cdf_fields = [
            pa.field("_change_type", pa.string()),
            pa.field("_commit_version", pa.int64()),
            pa.field("_commit_timestamp", pa.timestamp("us")),
        ]
        full_schema = pa.schema(list(arrow_schema) + cdf_fields)
        return pa.table({}, schema=full_schema)

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory data size for CDF reads."""
        return None

    def get_name(self) -> str:
        """Return human-readable name for this datasource."""
        return "DeltaCDF"

    def __repr__(self) -> str:
        """String representation of datasource."""
        return (
            f"DeltaCDFDatasource(path={self.path}, "
            f"versions={self.starting_version}-{self.ending_version})"
        )
