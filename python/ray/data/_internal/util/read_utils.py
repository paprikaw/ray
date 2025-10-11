"""Utility functions for ray.data.read()."""

import logging
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


def detect_file_type(file_path: str, extension_mapping: Dict[str, Tuple[str, any]]) -> Optional[Tuple[str, any]]:
    """Detect file type from path using extension mapping.

    Args:
        file_path: Path to the file.
        extension_mapping: Dictionary mapping extensions to (type_name, reader_func) tuples.

    Returns:
        Tuple of (type_name, reader_func) or None if not detected.
    """
    file_lower = file_path.lower()

    # Try compound extensions first (e.g., .csv.gz)
    for ext_len in [2, 1]:
        parts = file_path.rsplit(".", ext_len)
        if len(parts) == ext_len + 1:
            potential_ext = ".".join(parts[1:]).lower()
            if potential_ext in extension_mapping:
                return extension_mapping[potential_ext]

    return None


def group_files_by_type(
    file_paths: List[str],
    extension_mapping: Dict[str, Tuple[str, any]]
) -> Tuple[Dict[str, List[str]], List[str]]:
    """Group files by detected type.

    Args:
        file_paths: List of file paths.
        extension_mapping: Dictionary mapping extensions to (type_name, reader_func) tuples.

    Returns:
        Tuple of (files_by_type dict, unknown_files list).
    """
    files_by_type: Dict[str, List[str]] = {}
    unknown_files = []

    for file_path in file_paths:
        result = detect_file_type(file_path, extension_mapping)
        if result:
            type_name, _ = result
            if type_name not in files_by_type:
                files_by_type[type_name] = []
            files_by_type[type_name].append(file_path)
        else:
            unknown_files.append(file_path)

    return files_by_type, unknown_files


def get_supported_extensions() -> Set[str]:
    """Get set of all supported file extensions.

    Returns:
        Set of supported extensions (lowercase, without leading dot).
    """
    return {
        # Parquet
        "parquet",
        # CSV
        "csv", "csv.gz", "csv.br", "csv.zst", "csv.lz4",
        # JSON
        "json", "jsonl",
        "json.gz", "jsonl.gz",
        "json.br", "jsonl.br",
        "json.zst", "jsonl.zst",
        "json.lz4", "jsonl.lz4",
        # Text
        "txt",
        # Images
        "png", "jpg", "jpeg", "tif", "tiff", "bmp", "gif",
        # Audio
        "mp3", "wav", "aac", "flac", "ogg", "m4a", "wma",
        "alac", "aiff", "pcm", "amr", "opus",
        # Video
        "mp4", "mkv", "mov", "avi", "wmv", "flv", "webm",
        "m4v", "3gp", "mpeg", "mpg",
        # NumPy
        "npy",
        # Avro
        "avro",
        # TFRecords
        "tfrecords",
        # HTML
        "html", "htm",
    }


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format.

    Args:
        size_bytes: Size in bytes.

    Returns:
        Formatted string (e.g., "1.5 GB").
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def log_read_summary(files_by_type: Dict[str, List[str]]) -> None:
    """Log a summary of files to be read.

    Args:
        files_by_type: Dictionary mapping type names to lists of file paths.
    """
    total_files = sum(len(files) for files in files_by_type.values())
    logger.info(f"Reading {total_files} files across {len(files_by_type)} format(s):")

    for file_type, files in sorted(files_by_type.items()):
        logger.info(f"  - {file_type}: {len(files)} file(s)")


def validate_reader_args(reader_func: any, args: Dict) -> Dict:
    """Validate and filter arguments for a specific reader function.

    Args:
        reader_func: The reader function to validate args for.
        args: Dictionary of arguments.

    Returns:
        Filtered dictionary containing only valid arguments.
    """
    import inspect

    sig = inspect.signature(reader_func)
    valid_params = set(sig.parameters.keys())

    filtered_args = {}
    for key, value in args.items():
        if key in valid_params and value is not None:
            filtered_args[key] = value

    return filtered_args


def estimate_dataset_size(files: List[str], filesystem) -> int:
    """Estimate total dataset size from file list.

    Args:
        files: List of file paths.
        filesystem: PyArrow filesystem to use.

    Returns:
        Estimated size in bytes.
    """
    total_size = 0
    for file_path in files:
        try:
            file_info = filesystem.get_file_info(file_path)
            total_size += file_info.size
        except Exception as e:
            logger.debug(f"Could not get size for {file_path}: {e}")

    return total_size


def check_file_compatibility(files_by_type: Dict[str, List[str]]) -> bool:
    """Check if files of different types can be concatenated.

    Args:
        files_by_type: Dictionary mapping type names to lists of file paths.

    Returns:
        True if files are compatible for concatenation.
    """
    # For now, we assume all formats can be concatenated
    # In the future, we might add more sophisticated schema checking
    return True


def get_optimal_parallelism(
    num_files: int,
    total_size_bytes: int,
    target_block_size_bytes: int = 512 * 1024 * 1024  # 512 MB default
) -> int:
    """Calculate optimal parallelism based on dataset characteristics.

    Args:
        num_files: Number of files in the dataset.
        total_size_bytes: Total size of the dataset in bytes.
        target_block_size_bytes: Target size for each block.

    Returns:
        Recommended parallelism level.
    """
    # Calculate parallelism based on data size
    size_based = max(1, total_size_bytes // target_block_size_bytes)

    # Don't create more blocks than files (usually)
    file_based = num_files

    # Use the minimum, but at least 1
    return max(1, min(size_based, file_based))


def format_read_statistics(stats: Dict) -> str:
    """Format read statistics into human-readable string.

    Args:
        stats: Dictionary of statistics.

    Returns:
        Formatted statistics string.
    """
    lines = ["Read Statistics:"]

    if "num_files" in stats:
        lines.append(f"  Files read: {stats['num_files']}")

    if "total_rows" in stats:
        lines.append(f"  Total rows: {stats['total_rows']:,}")

    if "total_size_bytes" in stats:
        lines.append(f"  Total size: {format_file_size(stats['total_size_bytes'])}")

    if "read_time_seconds" in stats:
        lines.append(f"  Read time: {stats['read_time_seconds']:.2f}s")

    if "throughput_mb_per_sec" in stats:
        lines.append(f"  Throughput: {stats['throughput_mb_per_sec']:.1f} MB/s")

    return "\n".join(lines)


def suggest_optimization(files_by_type: Dict[str, List[str]], total_size_bytes: int) -> List[str]:
    """Suggest optimizations based on dataset characteristics.

    Args:
        files_by_type: Dictionary mapping type names to lists of file paths.
        total_size_bytes: Total size of the dataset in bytes.

    Returns:
        List of optimization suggestions.
    """
    suggestions = []

    # Check for many small files
    total_files = sum(len(files) for files in files_by_type.values())
    if total_files > 1000:
        avg_size = total_size_bytes / total_files if total_files > 0 else 0
        if avg_size < 1024 * 1024:  # Less than 1 MB average
            suggestions.append(
                "Consider consolidating many small files into larger files "
                "for better performance."
            )

    # Check for mixed types
    if len(files_by_type) > 3:
        suggestions.append(
            "Reading many different file formats may have overhead. "
            "Consider using format-specific readers if possible."
        )

    # Check for large dataset
    if total_size_bytes > 10 * 1024 * 1024 * 1024:  # > 10 GB
        suggestions.append(
            "Large dataset detected. Consider using parallelism and "
            "streaming processing for better memory efficiency."
        )

    return suggestions


class ReadMetrics:
    """Track metrics for read operations."""

    def __init__(self):
        """Initialize metrics tracker."""
        self.files_read = 0
        self.rows_read = 0
        self.bytes_read = 0
        self.errors = 0
        self.start_time = None
        self.end_time = None

    def record_file(self, num_rows: int, size_bytes: int):
        """Record metrics for a file.

        Args:
            num_rows: Number of rows in the file.
            size_bytes: Size of the file in bytes.
        """
        self.files_read += 1
        self.rows_read += num_rows
        self.bytes_read += size_bytes

    def record_error(self):
        """Record an error."""
        self.errors += 1

    def get_summary(self) -> Dict:
        """Get summary of metrics.

        Returns:
            Dictionary of metrics.
        """
        return {
            "files_read": self.files_read,
            "rows_read": self.rows_read,
            "bytes_read": self.bytes_read,
            "errors": self.errors,
        }


def is_hidden_file(file_path: str) -> bool:
    """Check if a file is hidden (starts with dot).

    Args:
        file_path: Path to check.

    Returns:
        True if the file appears to be hidden.
    """
    import os.path
    basename = os.path.basename(file_path)
    return basename.startswith(".")


def filter_hidden_files(file_paths: List[str]) -> List[str]:
    """Filter out hidden files from a list.

    Args:
        file_paths: List of file paths.

    Returns:
        List with hidden files removed.
    """
    return [f for f in file_paths if not is_hidden_file(f)]


def deduplicate_files(file_paths: List[str]) -> List[str]:
    """Remove duplicate file paths.

    Args:
        file_paths: List of file paths (may contain duplicates).

    Returns:
        List with duplicates removed, maintaining order.
    """
    seen = set()
    result = []
    for path in file_paths:
        if path not in seen:
            seen.add(path)
            result.append(path)
    return result

