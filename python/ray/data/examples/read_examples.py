"""Example usage patterns for ray.data.read()

This module demonstrates various usage patterns for the unified
ray.data.read() function.
"""

import ray


def example_basic_usage():
    """Basic usage: reading a single file."""
    print("Example 1: Basic single file read")
    
    # Read a single Parquet file
    ds = ray.data.read("s3://bucket/data.parquet")
    print(f"Loaded dataset with {ds.count()} rows")
    print(f"Schema: {ds.schema()}")


def example_directory_read():
    """Reading all files from a directory."""
    print("\nExample 2: Directory read")
    
    # Read all supported files from a directory
    ds = ray.data.read("s3://bucket/data/")
    print(f"Loaded {ds.count()} rows from directory")


def example_multiple_paths():
    """Reading from multiple specific paths."""
    print("\nExample 3: Multiple paths")
    
    # Read from multiple specific files
    paths = [
        "s3://bucket/data1.parquet",
        "s3://bucket/data2.csv",
        "s3://bucket/data3.json"
    ]
    ds = ray.data.read(paths)
    print(f"Loaded {ds.count()} rows from {len(paths)} files")


def example_mixed_file_types():
    """Reading directory with mixed file types."""
    print("\nExample 4: Mixed file types")
    
    # Automatically handles different formats
    ds = ray.data.read("s3://bucket/mixed-data/")
    
    # Process the unified dataset
    processed = ds.map(lambda row: row)
    print(f"Processed {processed.count()} rows from mixed formats")


def example_with_filtering():
    """Reading with data filtering."""
    print("\nExample 5: Read and filter")
    
    # Read and filter in one pipeline
    ds = ray.data.read("s3://bucket/data/")
    filtered = ds.filter(lambda row: row["value"] > 100)
    print(f"Filtered to {filtered.count()} rows")


def example_format_specific_args():
    """Passing format-specific arguments."""
    print("\nExample 6: Format-specific arguments")
    
    # Pass CSV-specific arguments
    ds = ray.data.read(
        "s3://bucket/data.csv",
        delimiter=";",
        columns=["col1", "col2", "col3"]
    )
    print(f"Loaded {ds.count()} rows with custom CSV settings")


def example_with_parallelism():
    """Reading with custom parallelism."""
    print("\nExample 7: Custom parallelism")
    
    # Control parallelism for better performance
    ds = ray.data.read(
        "s3://bucket/large-data/",
        override_num_blocks=100
    )
    print(f"Loaded {ds.count()} rows with 100 parallel blocks")


def example_with_paths_included():
    """Reading with file paths included in output."""
    print("\nExample 8: Include paths")
    
    # Include source file path in each row
    ds = ray.data.read(
        "s3://bucket/data/",
        include_paths=True
    )
    
    sample = ds.take(1)[0]
    print(f"Sample row with path: {sample}")


def example_incremental_processing():
    """Incremental processing of large datasets."""
    print("\nExample 9: Incremental processing")
    
    # Read and process incrementally
    ds = ray.data.read("s3://bucket/large-data/")
    
    for batch in ds.iter_batches(batch_size=1000):
        # Process each batch
        print(f"Processing batch with {len(batch)} rows")
        # Your processing logic here
        break  # Just show first batch


def example_with_transformations():
    """Complete ETL pipeline."""
    print("\nExample 10: ETL pipeline")
    
    # Read, transform, and write
    ds = ray.data.read("s3://bucket/input/")
    
    # Transform
    transformed = (ds
        .filter(lambda row: row["status"] == "active")
        .map(lambda row: {
            **row,
            "value_doubled": row["value"] * 2
        })
    )
    
    # Write results
    transformed.write_parquet("s3://bucket/output/")
    print("ETL pipeline completed")


def example_error_handling():
    """Error handling and missing files."""
    print("\nExample 11: Error handling")
    
    try:
        # Try to read potentially missing files
        ds = ray.data.read(
            ["s3://bucket/file1.parquet", "s3://bucket/file2.parquet"],
            ignore_missing_paths=True
        )
        print(f"Successfully read {ds.count()} rows")
    except Exception as e:
        print(f"Error occurred: {e}")


def example_aggregation():
    """Reading and aggregating data."""
    print("\nExample 12: Aggregation")
    
    # Read and aggregate
    ds = ray.data.read("s3://bucket/sales-data/")
    
    # Group by category and aggregate
    aggregated = ds.groupby("category").mean("revenue")
    results = aggregated.take()
    
    for result in results:
        print(f"Category: {result['category']}, Avg Revenue: {result['mean(revenue)']}")


def example_time_series():
    """Reading time series data."""
    print("\nExample 13: Time series data")
    
    # Read partitioned time series data
    ds = ray.data.read("s3://bucket/timeseries/year=2024/month=*/")
    
    # Process time series
    sorted_ds = ds.sort("timestamp")
    print(f"Loaded {sorted_ds.count()} time series records")


def example_image_processing():
    """Reading and processing images."""
    print("\nExample 14: Image processing")
    
    # Read images
    ds = ray.data.read("s3://bucket/images/")
    
    # Apply transformations
    processed = ds.map(lambda row: {
        **row,
        "image": preprocess_image(row["image"])
    })
    
    print(f"Processed {processed.count()} images")


def preprocess_image(image):
    """Dummy image preprocessing function."""
    return image  # Replace with actual preprocessing


def example_ml_pipeline():
    """Machine learning pipeline."""
    print("\nExample 15: ML pipeline")
    
    # Read training data
    train_ds = ray.data.read("s3://bucket/train/")
    
    # Preprocess
    preprocessed = train_ds.map_batches(
        lambda batch: preprocess_batch(batch),
        batch_size=32
    )
    
    # Train model (pseudocode)
    # model = train(preprocessed)
    
    print(f"Prepared {preprocessed.count()} training examples")


def preprocess_batch(batch):
    """Dummy batch preprocessing function."""
    return batch  # Replace with actual preprocessing


def example_streaming():
    """Streaming data processing."""
    print("\nExample 16: Streaming processing")
    
    # Read data in streaming fashion
    ds = ray.data.read("s3://bucket/streaming-data/")
    
    # Process without materializing all data
    count = 0
    for row in ds.iter_rows():
        # Process each row
        count += 1
        if count >= 10:
            break
    
    print(f"Streamed {count} rows")


def example_multi_modal():
    """Multi-modal data (text + images)."""
    print("\nExample 17: Multi-modal data")
    
    # Read directory with both images and text
    ds = ray.data.read("s3://bucket/multimodal/")
    
    # Process different modalities
    processed = ds.map(lambda row: {
        **row,
        "processed": True
    })
    
    print(f"Processed {processed.count()} multi-modal records")


def example_data_quality():
    """Data quality checks."""
    print("\nExample 18: Data quality checks")
    
    # Read data
    ds = ray.data.read("s3://bucket/data/")
    
    # Check for nulls
    null_check = ds.filter(lambda row: any(v is None for v in row.values()))
    print(f"Found {null_check.count()} rows with null values")
    
    # Check ranges
    range_check = ds.filter(lambda row: row.get("age", 0) < 0 or row.get("age", 0) > 120)
    print(f"Found {range_check.count()} rows with invalid age")


def example_partitioned_data():
    """Reading Hive-partitioned data."""
    print("\nExample 19: Partitioned data")
    
    # Read Hive-partitioned data
    ds = ray.data.read(
        "s3://bucket/partitioned/",
        partitioning="hive"
    )
    
    print(f"Loaded {ds.count()} rows from partitioned dataset")


def example_compressed_data():
    """Reading compressed files."""
    print("\nExample 20: Compressed data")
    
    # Automatically handles .gz, .br, .zst, .lz4
    ds = ray.data.read("s3://bucket/compressed-data/")
    
    print(f"Loaded {ds.count()} rows from compressed files")


def run_all_examples():
    """Run all examples."""
    examples = [
        example_basic_usage,
        example_directory_read,
        example_multiple_paths,
        example_mixed_file_types,
        example_with_filtering,
        example_format_specific_args,
        example_with_parallelism,
        example_with_paths_included,
        example_incremental_processing,
        example_with_transformations,
        example_error_handling,
        example_aggregation,
        example_time_series,
        example_image_processing,
        example_ml_pipeline,
        example_streaming,
        example_multi_modal,
        example_data_quality,
        example_partitioned_data,
        example_compressed_data,
    ]
    
    print("=" * 80)
    print("RAY DATA READ() EXAMPLES")
    print("=" * 80)
    
    for example in examples:
        try:
            example()
        except Exception as e:
            print(f"Example failed: {e}")
        print("-" * 80)


if __name__ == "__main__":
    # Initialize Ray
    ray.init(ignore_reinit_error=True)
    
    try:
        run_all_examples()
    finally:
        ray.shutdown()

