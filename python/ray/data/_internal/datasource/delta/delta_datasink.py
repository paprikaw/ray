"""
Delta Lake datasink implementation with two-phase commit for ACID compliance.
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
    """Ray Data datasink for Delta Lake tables using two-phase commit."""

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
            logger.info(
                f"Delta table already exists at {self.path}. Skipping write due to mode='ignore'."
            )
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
        if self.schema:
            # Check column names match
            table_cols = set(table.column_names)
            schema_cols = set(self.schema.names)
            missing_cols = schema_cols - table_cols

            if missing_cols:
                raise ValueError(
                    f"Table missing required columns from schema: {sorted(missing_cols)}. "
                    f"Table columns: {sorted(table_cols)}"
                )

            # Validate types match for common columns (excluding partition cols)
            for field in self.schema:
                if (
                    field.name in table.column_names
                    and field.name not in self.partition_cols
                ):
                    table_type = table[field.name].type
                    if not self._types_compatible(field.type, table_type):
                        raise ValueError(
                            f"Type mismatch for column '{field.name}': "
                            f"schema expects {field.type}, table has {table_type}"
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
        missing_cols = [
            col for col in self.partition_cols if col not in table.column_names
        ]
        if missing_cols:
            raise ValueError(
                f"Partition columns {missing_cols} not found in table schema. "
                f"Available columns: {table.column_names}."
            )
        if self.schema:
            for col in self.partition_cols:
                if col not in self.schema.names:
                    raise ValueError(
                        f"Partition column {col} not found in provided schema."
                    )

    def _write_table_data(
        self, table: pa.Table, task_idx: int, block_idx: int = 0
    ) -> List["AddAction"]:
        """Write table data as partitioned or non-partitioned Parquet files."""
        if self.partition_cols:
            partitioned_tables = self._partition_table(table, self.partition_cols)
            return [
                self._write_partition(
                    partition_table, partition_values, task_idx, block_idx
                )
                for partition_values, partition_table in partitioned_tables.items()
            ]
        return [self._write_partition(table, (), task_idx, block_idx)]

    def _partition_table(
        self, table: pa.Table, partition_cols: List[str]
    ) -> Dict[tuple, pa.Table]:
        """Partition table by columns efficiently using vectorized operations."""
        from collections import defaultdict

        import pyarrow.compute as pc

        partitions = {}
        if len(partition_cols) == 1:
            col_name = partition_cols[0]
            unique_values = pc.unique(table[col_name])
            if len(unique_values) > _MAX_PARTITIONS:
                raise ValueError(
                    f"Too many partition values ({len(unique_values)}). "
                    f"Maximum is {_MAX_PARTITIONS}. Consider using fewer partition columns."
                )
            for partition_value in unique_values:
                partition_value_py = partition_value.as_py()
                # Validate partition value is safe
                self._validate_partition_value(partition_value_py)
                row_mask = pc.equal(table[col_name], partition_value)
                partitions[(partition_value_py,)] = table.filter(row_mask)
        else:
            partition_values_lists = [table[col].to_pylist() for col in partition_cols]
            partition_indices = defaultdict(list)
            for row_idx, partition_tuple in enumerate(zip(*partition_values_lists)):
                # Validate all partition values in tuple
                for val in partition_tuple:
                    self._validate_partition_value(val)
                partition_indices[partition_tuple].append(row_idx)
            if len(partition_indices) > _MAX_PARTITIONS:
                raise ValueError(
                    f"Too many partition combinations ({len(partition_indices)}). "
                    f"Maximum is {_MAX_PARTITIONS}."
                )
            for partition_tuple, row_indices in partition_indices.items():
                partitions[partition_tuple] = table.take(row_indices)
        return partitions

    def _validate_partition_value(self, value: Any) -> None:
        """Validate partition value is safe and within limits."""
        if value is None:
            return  # None is allowed (creates __HIVE_DEFAULT_PARTITION__)
        value_str = str(value)
        # Check for path traversal attempts
        if ".." in value_str or "/" in value_str or "\\" in value_str:
            raise ValueError(
                f"Partition value contains invalid characters: {value}. "
                "Values cannot contain '..', '/', or '\\'."
            )
        # Check length
        if len(value_str) > _MAX_PARTITION_PATH_LENGTH:
            raise ValueError(
                f"Partition value too long ({len(value_str)} chars). "
                f"Maximum is {_MAX_PARTITION_PATH_LENGTH}."
            )

    def _write_partition(
        self,
        table: pa.Table,
        partition_values: tuple,
        task_idx: int,
        block_idx: int = 0,
    ) -> "AddAction":
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

        file_size = self._write_parquet_file(table, full_path)
        file_statistics = self._compute_statistics(table)

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

        partition_path_components = []
        partition_dict = {}
        for col_name, col_value in zip(self.partition_cols, partition_values):
            if col_value is None:
                # Delta Lake uses __HIVE_DEFAULT_PARTITION__ for null values
                value_str = "__HIVE_DEFAULT_PARTITION__"
                partition_dict[col_name] = None
            else:
                value_str = str(col_value)
                # URL-encode special characters for path safety
                value_str = urllib.parse.quote(value_str, safe="")
                partition_dict[col_name] = str(col_value)
            partition_path_components.append(f"{col_name}={value_str}")

        partition_path = "/".join(partition_path_components) + "/"

        # Validate total path length
        if len(partition_path) > _MAX_PARTITION_PATH_LENGTH:
            raise ValueError(
                f"Partition path too long ({len(partition_path)} chars). "
                f"Maximum is {_MAX_PARTITION_PATH_LENGTH}."
            )

        return partition_path, partition_dict

    def _write_parquet_file(self, table: pa.Table, file_path: str) -> int:
        """Write PyArrow table to Parquet file and return file size."""
        table_to_write = self._prepare_table_for_write(table)

        compression = self.write_kwargs.get("compression", "snappy")
        valid_compressions = ["snappy", "gzip", "brotli", "zstd", "lz4", "none"]
        if compression not in valid_compressions:
            raise ValueError(
                f"Invalid compression '{compression}'. Supported: {valid_compressions}"
            )

        self._ensure_parent_directory(file_path)
        pq.write_table(
            table_to_write,
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

        statistics = {"numRecords": len(table)}
        min_values = {}
        max_values = {}
        null_counts = {}

        max_stats_cols = 1000
        cols_to_process = (
            table.column_names[:max_stats_cols]
            if len(table.column_names) > max_stats_cols
            else table.column_names
        )

        for col_name in cols_to_process:
            column = table[col_name]
            null_counts[col_name] = column.null_count
            if column.null_count < len(column):
                min_val, max_val = self._compute_column_min_max(column)
                if min_val is not None:
                    min_values[col_name] = min_val
                if max_val is not None:
                    max_values[col_name] = max_val

        if min_values:
            statistics["minValues"] = min_values
        if max_values:
            statistics["maxValues"] = max_values
        if null_counts:
            statistics["nullCount"] = null_counts
        return json.dumps(statistics)

    def _compute_column_min_max(
        self, column: pa.Array
    ) -> tuple[Optional[Any], Optional[Any]]:
        """Compute min and max values for a single column."""
        import pyarrow.compute as pc

        col_type = column.type
        if pa.types.is_integer(col_type) or pa.types.is_floating(col_type):
            return pc.min(column).as_py(), pc.max(column).as_py()
        elif pa.types.is_string(col_type) or pa.types.is_large_string(col_type):
            min_val = pc.min(column).as_py()
            max_val = pc.max(column).as_py()
            return (
                str(min_val) if min_val is not None else None,
                str(max_val) if max_val is not None else None,
            )
        return None, None

    def on_write_complete(self, write_result: WriteResult[List["AddAction"]]) -> None:
        """Phase 2: Commit all files in single ACID transaction."""
        all_file_actions = self._collect_file_actions(write_result)

        # Validate all file actions before committing
        self._validate_file_actions(all_file_actions)

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
                logger.info(
                    f"Creating empty Delta table at {self.path} with specified schema"
                )
                self._create_empty_table()
            else:
                logger.info(
                    f"No files to commit for Delta table at {self.path}. Skipping table creation."
                )
            return

        if existing_table:
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
        logger.info(f"Created empty Delta table at {self.path}")

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
        logger.info(
            f"Created Delta table at {self.path} with {len(file_actions)} files"
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

        if self.mode == WriteMode.IGNORE:
            logger.info(
                f"Table created at {self.path} during write. Skipping commit due to mode='ignore'."
            )
            return
        if file_actions:
            existing_schema = existing_table.schema().to_pyarrow()
            inferred_schema = self._infer_schema(file_actions)
            if not self._schemas_compatible(existing_schema, inferred_schema):
                existing_cols = {f.name: f.type for f in existing_schema}
                inferred_cols = {f.name: f.type for f in inferred_schema}
                missing_cols = set(existing_cols.keys()) - set(inferred_cols.keys())
                extra_cols = set(inferred_cols.keys()) - set(existing_cols.keys())
                type_mismatches = [
                    col
                    for col in existing_cols
                    if col in inferred_cols and existing_cols[col] != inferred_cols[col]
                ]
                error_msg = "Schema mismatch: existing table schema does not match written data schema."
                if missing_cols:
                    error_msg += (
                        f" Missing columns in written data: {sorted(missing_cols)}."
                    )
                if extra_cols:
                    error_msg += (
                        f" Extra columns in written data: {sorted(extra_cols)}."
                    )
                if type_mismatches:
                    error_msg += f" Type mismatches: {[(c, existing_cols[c], inferred_cols[c]) for c in type_mismatches]}."
                raise ValueError(error_msg)

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
        logger.info(
            f"Committed {len(file_actions)} files to Delta table at {self.path} (mode={transaction_mode})"
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
        """Convert PyArrow data type to Delta Lake type string."""
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
            return "long"
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
        if not self._written_files:
            return

        logger.info(f"Cleaning up {len(self._written_files)} uncommitted files")
        for file_path in self._written_files:
            try:
                if (
                    self.filesystem.get_file_info(file_path).type
                    != pa_fs.FileType.NotFound
                ):
                    self.filesystem.delete_file(file_path)
            except Exception as e:
                logger.warning(f"Failed to cleanup file {file_path}: {e}")

        self._written_files.clear()

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return None

    def get_name(self) -> str:
        return "Delta"
