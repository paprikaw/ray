"""Delta Lake datasink implementation with two-phase commit for ACID compliance.

This module implements write operations for Delta Lake tables using Ray Data's
Datasink interface. It provides ACID-compliant writes using a two-phase commit
protocol where files are written first, then committed atomically to the Delta
Lake transaction log.

Delta Lake: https://delta.io/
deltalake Python library: https://github.com/delta-io/delta-rs
PyArrow: https://arrow.apache.org/docs/python/
"""

import json
import logging
import os
import time
import urllib.parse
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq

from ray.data._internal.datasource.delta.config import WriteMode
from ray.data._internal.datasource.delta.utilities import (
    get_storage_options,
    try_get_deltatable,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import (
    RetryingPyFileSystem,
    _check_import,
    _resolve_paths_and_filesystem,
)
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink, WriteResult

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from deltalake.transaction import AddAction

logger = logging.getLogger(__name__)

# Maximum number of partitions to prevent filesystem issues
_MAX_PARTITIONS = 10000

# Maximum partition path length (filesystem limit)
_MAX_PARTITION_PATH_LENGTH = 200


class DeltaDatasink(Datasink[List["AddAction"]]):
    """Ray Data datasink for Delta Lake tables using two-phase commit.

    This datasink implements ACID-compliant writes to Delta Lake tables by:
    1. Writing Parquet files to storage (Phase 1)
    2. Committing file metadata to Delta transaction log atomically (Phase 2)

    Supports distributed writes, partitioning, schema validation, and multiple
    write modes (append, overwrite, error, ignore).

    Delta Lake specification: https://delta.io/
    deltalake Python API: https://delta-io.github.io/delta-rs/python/
    """

    def __init__(
        self,
        path: str,
        *,
        mode: str = WriteMode.APPEND.value,
        partition_cols: Optional[List[str]] = None,
        filesystem: Optional[pa_fs.FileSystem] = None,
        schema: Optional[pa.Schema] = None,
        **write_kwargs,
    ):
        _check_import(self, module="deltalake", package="deltalake")

        self.mode = self._validate_mode(mode)
        self.partition_cols = self._validate_partition_column_names(
            partition_cols or []
        )
        self.schema = schema
        self.write_kwargs = write_kwargs
        self._skip_write = False
        self._written_files: Set[str] = set()
        self._existing_table_at_start: Optional["DeltaTable"] = None

        # Set up filesystem with retry support (matches _FileDatasink pattern)
        data_context = DataContext.get_current()
        paths, self.filesystem = _resolve_paths_and_filesystem(path, filesystem)
        self.filesystem = RetryingPyFileSystem.wrap(
            self.filesystem, retryable_errors=data_context.retried_io_errors
        )
        assert len(paths) == 1
        self.path = paths[0]

        self.storage_options = get_storage_options(
            self.path, write_kwargs.get("storage_options")
        )

    def _validate_mode(self, mode: str) -> WriteMode:
        """Validate and return WriteMode."""
        valid_modes = ["append", "overwrite", "error", "ignore"]
        if mode not in valid_modes:
            raise ValueError(f"Invalid mode '{mode}'. Supported: {valid_modes}")
        return WriteMode(mode)

    def _validate_partition_column_names(self, partition_cols: List[str]) -> List[str]:
        """Validate partition column names."""
        if len(partition_cols) > 10:
            raise ValueError(
                f"Too many partition columns ({len(partition_cols)}). Maximum is 10."
            )
        for col in partition_cols:
            if not isinstance(col, str) or not col:
                raise ValueError(f"Invalid partition column name: {col}")
            if "/" in col or "\\" in col or ".." in col:
                raise ValueError(
                    f"Partition column name contains invalid characters: {col}"
                )
        return partition_cols

    def on_write_start(self) -> None:
        """Check ERROR and IGNORE modes before writing files."""
        _check_import(self, module="deltalake", package="deltalake")

        self._existing_table_at_start = try_get_deltatable(
            self.path, self.storage_options
        )

        if self.mode == WriteMode.ERROR and self._existing_table_at_start:
            raise ValueError(
                f"Delta table already exists at {self.path}. Use mode='append' or 'overwrite'."
            )

        if self.mode == WriteMode.IGNORE and self._existing_table_at_start:
            self._skip_write = True
        else:
            self._skip_write = False

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List["AddAction"]:
        """Phase 1: Write Parquet files, return AddAction metadata (no commit)."""
        if self._skip_write:
            return []

        _check_import(self, module="deltalake", package="deltalake")

        # Process blocks individually to avoid memory issues
        all_actions = []
        block_idx = 0
        for block in blocks:
            block_accessor = BlockAccessor.for_block(block)
            if block_accessor.num_rows() == 0:
                continue

            table = block_accessor.to_arrow()
            self._validate_table_schema(table)
            self._validate_partition_columns_in_table(table)

            # Write this block's data
            actions = self._write_table_data(table, ctx.task_idx, block_idx)
            all_actions.extend([a for a in actions if a is not None])
            block_idx += 1

        return all_actions

    def _validate_table_schema(self, table: pa.Table) -> None:
        """Validate table schema matches expected schema if provided."""
        if not self.schema:
            return

        table_cols = set(table.column_names)
        schema_cols = set(self.schema.names)
        missing = schema_cols - table_cols
        if missing:
            raise ValueError(
                f"Missing columns: {sorted(missing)}. Table has: {sorted(table_cols)}"
            )

        for field in self.schema:
            if field.name in table_cols and field.name not in self.partition_cols:
                if not self._types_compatible(field.type, table[field.name].type):
                    raise ValueError(
                        f"Type mismatch for '{field.name}': expected {field.type}, "
                        f"got {table[field.name].type}"
                    )

    def _types_compatible(
        self, expected_type: pa.DataType, actual_type: pa.DataType
    ) -> bool:
        """Check if two PyArrow types are compatible for writing."""
        if expected_type == actual_type:
            return True

        if pa.types.is_integer(expected_type) and pa.types.is_integer(actual_type):
            expected_width = getattr(expected_type, "bit_width", 64)
            actual_width = getattr(actual_type, "bit_width", 64)
            return actual_width <= expected_width

        if pa.types.is_floating(expected_type) and pa.types.is_floating(actual_type):
            return True

        if (
            pa.types.is_string(expected_type) or pa.types.is_large_string(expected_type)
        ) and (
            pa.types.is_string(actual_type) or pa.types.is_large_string(actual_type)
        ):
            return True

        return False

    def _validate_partition_columns_in_table(self, table: pa.Table) -> None:
        """Validate that all partition columns exist in the table schema."""
        if not self.partition_cols:
            return
        missing = [col for col in self.partition_cols if col not in table.column_names]
        if missing:
            raise ValueError(
                f"Partition columns {missing} not found. Available: {table.column_names}"
            )
        if self.schema:
            for col in self.partition_cols:
                if col not in self.schema.names:
                    raise ValueError(f"Partition column {col} not in schema")

    def _write_table_data(
        self, table: pa.Table, task_idx: int, block_idx: int = 0
    ) -> List["AddAction"]:
        """Write table data as partitioned or non-partitioned Parquet files."""
        if self.partition_cols:
            partitioned_tables = self._partition_table(table, self.partition_cols)
            actions = [
                self._write_partition(
                    partition_table, partition_values, task_idx, block_idx
                )
                for partition_values, partition_table in partitioned_tables.items()
            ]
            return [a for a in actions if a is not None]
        action = self._write_partition(table, (), task_idx, block_idx)
        return [action] if action is not None else []

    def _partition_table(
        self, table: pa.Table, partition_cols: List[str]
    ) -> Dict[tuple, pa.Table]:
        """Partition table by columns efficiently using vectorized operations."""
        from collections import defaultdict

        import pyarrow.compute as pc

        partitions = {}
        if len(partition_cols) == 1:
            col = partition_cols[0]
            unique_vals = pc.unique(table[col])
            if len(unique_vals) > _MAX_PARTITIONS:
                raise ValueError(
                    f"Too many partition values ({len(unique_vals)}). Max: {_MAX_PARTITIONS}"
                )
            for val in unique_vals:
                val_py = val.as_py()
                self._validate_partition_value(val_py)
                partitions[(val_py,)] = table.filter(pc.equal(table[col], val))
        else:
            val_lists = [table[col].to_pylist() for col in partition_cols]
            indices = defaultdict(list)
            for idx, tup in enumerate(zip(*val_lists)):
                for v in tup:
                    self._validate_partition_value(v)
                indices[tup].append(idx)
            if len(indices) > _MAX_PARTITIONS:
                raise ValueError(
                    f"Too many partition combinations ({len(indices)}). Max: {_MAX_PARTITIONS}"
                )
            for tup, idxs in indices.items():
                partitions[tup] = table.take(idxs)
        return partitions

    def _validate_partition_value(self, value: Any) -> None:
        """Validate partition value is safe and within limits."""
        if value is None:
            return
        val_str = str(value)
        if ".." in val_str or "/" in val_str or "\\" in val_str:
            raise ValueError(f"Partition value contains invalid chars: {value}")
        if len(val_str) > _MAX_PARTITION_PATH_LENGTH:
            raise ValueError(
                f"Partition value too long ({len(val_str)} chars). Max: {_MAX_PARTITION_PATH_LENGTH}"
            )

    def _write_partition(
        self,
        table: pa.Table,
        partition_values: tuple,
        task_idx: int,
        block_idx: int = 0,
    ) -> Optional["AddAction"]:
        """Write a single partition to Parquet file and create AddAction metadata."""
        from deltalake.transaction import AddAction

        # Skip empty tables
        if len(table) == 0:
            return None

        filename = self._generate_filename(task_idx, block_idx)
        partition_path, partition_dict = self._build_partition_path(partition_values)
        relative_path = partition_path + filename

        # Validate path is within table directory
        self._validate_file_path(relative_path)

        full_path = os.path.join(self.path, relative_path)

        # Track file for cleanup on failure
        self._written_files.add(full_path)

        table_to_write = self._prepare_table_for_write(table)
        file_size = self._write_parquet_file(table_to_write, full_path)
        file_statistics = self._compute_statistics(table_to_write)

        return AddAction(
            path=relative_path,
            size=file_size,
            partition_values=partition_dict,
            modification_time=int(time.time() * 1000),
            data_change=True,
            stats=file_statistics,
        )

    def _generate_filename(self, task_idx: int, block_idx: int = 0) -> str:
        """Generate unique Parquet filename."""
        # Use UUID4 for uniqueness, task_idx and block_idx for debugging
        unique_id = uuid.uuid4().hex
        return f"part-{task_idx:05d}-{block_idx:05d}-{unique_id}.parquet"

    def _validate_file_path(self, relative_path: str) -> None:
        """Validate file path is safe."""
        if ".." in relative_path:
            raise ValueError(f"Invalid file path: {relative_path} (contains '..')")
        if len(relative_path) > 500:
            raise ValueError(f"File path too long: {relative_path}")

    def _build_partition_path(
        self, partition_values: tuple
    ) -> tuple[str, Dict[str, Optional[str]]]:
        """Build Hive-style partition path and dictionary for Delta metadata."""
        if not self.partition_cols or not partition_values:
            return "", {}

        parts, part_dict = [], {}
        for col, val in zip(self.partition_cols, partition_values):
            if val is None:
                val_str = "__HIVE_DEFAULT_PARTITION__"
                part_dict[col] = None
            else:
                val_str = urllib.parse.quote(str(val), safe="")
                part_dict[col] = str(val)
            parts.append(f"{col}={val_str}")

        path = "/".join(parts) + "/"
        if len(path) > _MAX_PARTITION_PATH_LENGTH:
            raise ValueError(
                f"Partition path too long ({len(path)} chars). Max: {_MAX_PARTITION_PATH_LENGTH}"
            )
        return path, part_dict

    def _write_parquet_file(self, table: pa.Table, file_path: str) -> int:
        """Write PyArrow table to Parquet file and return file size."""
        compression = self.write_kwargs.get("compression", "snappy")
        valid_compressions = ["snappy", "gzip", "brotli", "zstd", "lz4", "none"]
        if compression not in valid_compressions:
            raise ValueError(
                f"Invalid compression '{compression}'. Supported: {valid_compressions}"
            )

        self._ensure_parent_directory(file_path)
        pq.write_table(
            table,
            file_path,
            filesystem=self.filesystem,
            compression=compression,
            write_statistics=True,
        )

        file_info = self.filesystem.get_file_info(file_path)
        if file_info.size == 0:
            raise RuntimeError(f"Written file is empty: {file_path}")
        return file_info.size

    def _prepare_table_for_write(self, table: pa.Table) -> pa.Table:
        """Prepare table for writing by removing partition columns."""
        return table if not self.partition_cols else table.drop(self.partition_cols)

    def _ensure_parent_directory(self, file_path: str) -> None:
        """Create parent directory for file if it doesn't exist."""
        parent_dir = os.path.dirname(file_path)
        if parent_dir:
            self.filesystem.create_dir(parent_dir, recursive=True)

    def _compute_statistics(self, table: pa.Table) -> str:
        """Compute file-level statistics for Delta Lake transaction log."""
        if len(table) == 0:
            return json.dumps({"numRecords": 0})

        import pyarrow.compute as pc

        stats = {"numRecords": len(table)}
        min_vals, max_vals, null_counts = {}, {}, {}

        for col_name in table.column_names[:1000]:  # Limit to first 1000 cols
            col = table[col_name]
            null_counts[col_name] = col.null_count
            if col.null_count < len(col):
                col_type = col.type
                if pa.types.is_integer(col_type) or pa.types.is_floating(col_type):
                    min_vals[col_name] = pc.min(col).as_py()
                    max_vals[col_name] = pc.max(col).as_py()
                elif pa.types.is_string(col_type) or pa.types.is_large_string(col_type):
                    min_val = pc.min(col).as_py()
                    max_val = pc.max(col).as_py()
                    if min_val is not None:
                        min_vals[col_name] = str(min_val)
                    if max_val is not None:
                        max_vals[col_name] = str(max_val)

        if min_vals:
            stats["minValues"] = min_vals
        if max_vals:
            stats["maxValues"] = max_vals
        if null_counts:
            stats["nullCount"] = null_counts
        return json.dumps(stats)

    def on_write_complete(self, write_result: WriteResult[List["AddAction"]]) -> None:
        """Phase 2: Commit all files in single ACID transaction."""
        all_file_actions = self._collect_file_actions(write_result)
        existing_table = try_get_deltatable(self.path, self.storage_options)

        if (
            self._existing_table_at_start is None
            and existing_table is not None
            and self.mode == WriteMode.ERROR
        ):
            self._cleanup_written_files()
            raise ValueError(
                f"Race condition detected: Delta table was created at {self.path} "
                f"after write started. Files have been written but not committed to "
                f"the transaction log. Use mode='append' or 'overwrite' if concurrent writes are expected."
            )

        if not all_file_actions:
            if self.schema and not existing_table:
                self._create_empty_table()
            return

        self._validate_file_actions(all_file_actions)

        if existing_table:
            if self.mode == WriteMode.IGNORE:
                self._cleanup_written_files()
                return
            self._commit_to_existing_table(existing_table, all_file_actions)
        else:
            self._create_table_with_files(all_file_actions)
        self._written_files.clear()

    def _collect_file_actions(
        self, write_result: WriteResult[List["AddAction"]]
    ) -> List["AddAction"]:
        """Collect all AddAction objects from distributed write tasks."""
        actions = [
            action
            for task_file_actions in write_result.write_returns
            for action in task_file_actions
            if action is not None
        ]
        paths = [action.path for action in actions]
        if len(paths) != len(set(paths)):
            duplicates = {p for p in paths if paths.count(p) > 1}
            raise ValueError(
                f"Duplicate file paths detected in AddActions: {duplicates}"
            )
        return actions

    def _validate_file_actions(self, file_actions: List["AddAction"]) -> None:
        """Validate file actions before committing."""
        for action in file_actions:
            full_path = os.path.join(self.path, action.path)
            file_info = self.filesystem.get_file_info(full_path)
            if file_info.type == pa_fs.FileType.NotFound:
                raise ValueError(f"File does not exist: {full_path}")

    def _create_empty_table(self) -> None:
        """Create empty Delta table with specified schema."""
        from deltalake.transaction import create_table_with_add_actions

        if not self.schema:
            raise ValueError(
                "Cannot create empty Delta table without explicit schema. Provide schema parameter to write_delta()."
            )

        delta_schema = self._convert_schema_to_delta(self.schema)
        create_table_with_add_actions(
            table_uri=self.path,
            schema=delta_schema,
            add_actions=[],
            mode=self.mode.value,
            partition_by=self.partition_cols or None,
            name=self.write_kwargs.get("name"),
            description=self.write_kwargs.get("description"),
            configuration=self.write_kwargs.get("configuration"),
            storage_options=self.storage_options,
            commit_properties=self.write_kwargs.get("commit_properties"),
            post_commithook_properties=self.write_kwargs.get(
                "post_commithook_properties"
            ),
        )

    def _create_table_with_files(self, file_actions: List["AddAction"]) -> None:
        """Create new Delta table and commit files in single transaction."""
        from deltalake.transaction import create_table_with_add_actions

        table_schema = self._infer_schema(file_actions)
        delta_schema = self._convert_schema_to_delta(table_schema)

        create_table_with_add_actions(
            table_uri=self.path,
            schema=delta_schema,
            add_actions=file_actions,
            mode=self.mode.value,
            partition_by=self.partition_cols or None,
            name=self.write_kwargs.get("name"),
            description=self.write_kwargs.get("description"),
            configuration=self.write_kwargs.get("configuration"),
            storage_options=self.storage_options,
            commit_properties=self.write_kwargs.get("commit_properties"),
            post_commithook_properties=self.write_kwargs.get(
                "post_commithook_properties"
            ),
        )

    def _commit_to_existing_table(
        self, existing_table: "DeltaTable", file_actions: List["AddAction"]
    ) -> None:
        """Commit files to existing Delta table using write transaction."""
        if self.mode == WriteMode.ERROR:
            raise ValueError(
                f"Race condition detected: Delta table was created at {self.path} "
                f"after write started. Files have been written but not committed to "
                f"the transaction log. Use mode='append' or 'overwrite' if concurrent writes are expected."
            )

        if file_actions:
            existing_schema = existing_table.schema().to_pyarrow()
            inferred_schema = self._infer_schema(file_actions)
            if not self._schemas_compatible(existing_schema, inferred_schema):
                existing_cols = {f.name: f.type for f in existing_schema}
                inferred_cols = {f.name: f.type for f in inferred_schema}
                missing = sorted(set(existing_cols) - set(inferred_cols))
                extra = sorted(set(inferred_cols) - set(existing_cols))
                mismatches = [
                    c
                    for c in existing_cols
                    if c in inferred_cols and existing_cols[c] != inferred_cols[c]
                ]
                msg = "Schema mismatch"
                if missing:
                    msg += f": missing {missing}"
                if extra:
                    msg += f": extra {extra}"
                if mismatches:
                    msg += f": type mismatches {mismatches}"
                raise ValueError(msg)

        transaction_mode = "overwrite" if self.mode == WriteMode.OVERWRITE else "append"
        existing_table.create_write_transaction(
            actions=file_actions,
            mode=transaction_mode,
            schema=existing_table.schema(),
            partition_by=self.partition_cols or None,
            commit_properties=self.write_kwargs.get("commit_properties"),
            post_commithook_properties=self.write_kwargs.get(
                "post_commithook_properties"
            ),
        )

    def _schemas_compatible(self, schema1: pa.Schema, schema2: pa.Schema) -> bool:
        """Check if two schemas are compatible for append operations."""
        if len(schema2) > len(schema1):
            return False

        schema1_dict = {f.name: f.type for f in schema1}
        for field in schema2:
            if field.name not in schema1_dict:
                return False
            if not self._types_compatible(schema1_dict[field.name], field.type):
                return False
        return True

    def _infer_schema(self, add_actions: List["AddAction"]) -> pa.Schema:
        """Infer schema from first file and partition columns."""
        if self.schema:
            return self.schema

        if not add_actions:
            raise ValueError("Cannot infer schema from empty file list")

        first_file = os.path.join(self.path, add_actions[0].path)
        file_obj = None
        try:
            file_obj = self.filesystem.open_input_file(first_file)
            parquet_file = pq.ParquetFile(file_obj)
            schema = parquet_file.schema_arrow

            if len(schema) == 0:
                raise ValueError(f"Cannot infer schema from empty file: {first_file}")

            if self.partition_cols:
                for col in self.partition_cols:
                    if col not in schema.names:
                        if col in add_actions[0].partition_values:
                            val = add_actions[0].partition_values[col]
                            col_type = self._infer_partition_type(val)
                            schema = schema.append(pa.field(col, col_type))
        finally:
            if file_obj is not None:
                file_obj.close()

        return schema

    def _infer_partition_type(self, value: Optional[str]) -> pa.DataType:
        """Infer PyArrow type from partition value."""
        if not value:
            return pa.string()
        try:
            int(value)
            return pa.int64()
        except ValueError:
            try:
                float(value)
                return pa.float64()
            except ValueError:
                return pa.string()

    def _convert_schema_to_delta(self, pa_schema: pa.Schema) -> "Any":
        """Convert PyArrow schema to Delta schema."""
        from deltalake import Schema as DeltaSchema

        try:
            return DeltaSchema.from_arrow(pa_schema)
        except (ValueError, TypeError):
            schema_json = self._pyarrow_schema_to_delta_json(pa_schema)
            return DeltaSchema.from_json(schema_json)

    def _pyarrow_schema_to_delta_json(self, pa_schema: pa.Schema) -> str:
        """Convert PyArrow schema to Delta schema JSON format."""
        fields = [
            {
                "name": field.name,
                "type": self._pyarrow_type_to_delta_type(field.type),
                "nullable": field.nullable,
                "metadata": {},
            }
            for field in pa_schema
        ]
        return json.dumps({"type": "struct", "fields": fields})

    def _pyarrow_type_to_delta_type(self, pa_type: pa.DataType) -> str:
        """Convert PyArrow data type to Delta Lake type string.

        Note: uint64 values exceeding int64 max (9223372036854775807) are converted
        to decimal(20,0) to prevent overflow. Delta Lake doesn't natively support
        unsigned integers, so large uint64 values require decimal type.
        See: https://delta-io.github.io/delta-rs/python/api/deltalake.schema.html
        """
        if pa.types.is_int8(pa_type):
            return "byte"
        elif pa.types.is_int16(pa_type):
            return "short"
        elif pa.types.is_int32(pa_type):
            return "integer"
        elif pa.types.is_int64(pa_type):
            return "long"
        elif pa.types.is_uint8(pa_type):
            return "short"
        elif pa.types.is_uint16(pa_type):
            return "integer"
        elif pa.types.is_uint32(pa_type):
            return "long"
        elif pa.types.is_uint64(pa_type):
            # uint64 can exceed int64 max (9223372036854775807), use decimal to prevent overflow
            return "decimal(20,0)"
        elif pa.types.is_float32(pa_type):
            return "float"
        elif pa.types.is_float64(pa_type):
            return "double"
        elif pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
            return "string"
        elif pa.types.is_binary(pa_type) or pa.types.is_large_binary(pa_type):
            return "binary"
        elif pa.types.is_boolean(pa_type):
            return "boolean"
        elif pa.types.is_date32(pa_type) or pa.types.is_date64(pa_type):
            return "date"
        elif pa.types.is_timestamp(pa_type):
            return "timestamp"
        elif pa.types.is_decimal(pa_type):
            return f"decimal({pa_type.precision},{pa_type.scale})"
        else:
            raise ValueError(f"Unsupported PyArrow type for Delta Lake: {pa_type}")

    def on_write_failed(self, error: Exception) -> None:
        """Handle write failure - cleanup orphaned files."""
        logger.error(
            f"Delta write failed for {self.path}: {error}. Cleaning up uncommitted files."
        )
        self._cleanup_written_files()

    def _cleanup_written_files(self) -> None:
        """Clean up all written files that weren't committed."""
        for file_path in self._written_files:
            try:
                if (
                    self.filesystem.get_file_info(file_path).type
                    != pa_fs.FileType.NotFound
                ):
                    self.filesystem.delete_file(file_path)
            except Exception:
                pass
        self._written_files.clear()

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return None

    def get_name(self) -> str:
        return "Delta"
