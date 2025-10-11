"""Tests for automatic source detection in ray.data.read()."""

import pytest

from ray.data._internal.read_unified import DataSource, SourceDetector


class TestSourceDetector:
    """Test suite for SourceDetector."""
    
    def test_detect_s3_source(self):
        """Test detection of S3 paths."""
        assert SourceDetector.detect("s3://bucket/path/file.parquet") == DataSource.S3
        assert SourceDetector.detect("s3a://bucket/path/file.parquet") == DataSource.S3
        assert SourceDetector.detect("s3n://bucket/path/file.parquet") == DataSource.S3
    
    def test_detect_gcs_source(self):
        """Test detection of GCS paths."""
        assert SourceDetector.detect("gs://bucket/path/file.parquet") == DataSource.GCS
        assert SourceDetector.detect("gcs://bucket/path/file.parquet") == DataSource.GCS
    
    def test_detect_azure_source(self):
        """Test detection of Azure paths."""
        assert SourceDetector.detect("az://container/path/file.parquet") == DataSource.AZURE
        assert SourceDetector.detect("abfs://container/path/file.parquet") == DataSource.AZURE
        assert SourceDetector.detect("abfss://container/path/file.parquet") == DataSource.AZURE
        assert SourceDetector.detect("wasb://container/path/file.parquet") == DataSource.AZURE
        assert SourceDetector.detect("wasbs://container/path/file.parquet") == DataSource.AZURE
    
    def test_detect_hdfs_source(self):
        """Test detection of HDFS paths."""
        assert SourceDetector.detect("hdfs://namenode/path/file.parquet") == DataSource.HDFS
    
    def test_detect_http_source(self):
        """Test detection of HTTP paths."""
        assert SourceDetector.detect("http://example.com/file.parquet") == DataSource.HTTP
        assert SourceDetector.detect("https://example.com/file.parquet") == DataSource.HTTPS
    
    def test_detect_local_source(self):
        """Test detection of local paths."""
        assert SourceDetector.detect("/path/to/file.parquet") == DataSource.LOCAL
        assert SourceDetector.detect("./path/to/file.parquet") == DataSource.LOCAL
        assert SourceDetector.detect("../path/to/file.parquet") == DataSource.LOCAL
        assert SourceDetector.detect("file:///path/to/file.parquet") == DataSource.LOCAL
        assert SourceDetector.detect("local:///path/to/file.parquet") == DataSource.LOCAL
        assert SourceDetector.detect("path/to/file.parquet") == DataSource.LOCAL
    
    def test_detect_unknown_source(self):
        """Test detection of unknown schemes."""
        assert SourceDetector.detect("ftp://server/file.parquet") == DataSource.UNKNOWN
        assert SourceDetector.detect("custom://path/file.parquet") == DataSource.UNKNOWN
    
    def test_detect_from_paths_single_source(self):
        """Test counting sources from multiple paths of same type."""
        paths = [
            "s3://bucket1/file1.parquet",
            "s3://bucket2/file2.parquet",
            "s3://bucket3/file3.parquet",
        ]
        
        result = SourceDetector.detect_from_paths(paths)
        
        assert len(result) == 1
        assert result[DataSource.S3] == 3
    
    def test_detect_from_paths_multiple_sources(self):
        """Test counting sources from paths with different types."""
        paths = [
            "s3://bucket/file1.parquet",
            "gs://bucket/file2.parquet",
            "/local/file3.parquet",
            "s3://bucket/file4.parquet",
            "https://example.com/file5.parquet",
        ]
        
        result = SourceDetector.detect_from_paths(paths)
        
        assert len(result) == 4
        assert result[DataSource.S3] == 2
        assert result[DataSource.GCS] == 1
        assert result[DataSource.LOCAL] == 1
        assert result[DataSource.HTTPS] == 1
    
    def test_detect_case_insensitive(self):
        """Test that scheme detection is case insensitive."""
        assert SourceDetector.detect("S3://bucket/path") == DataSource.S3
        assert SourceDetector.detect("GS://bucket/path") == DataSource.GCS
        assert SourceDetector.detect("HTTPS://example.com/path") == DataSource.HTTPS


class TestSourceDetectionIntegration:
    """Integration tests for source detection in read flow."""
    
    def test_source_logged_for_single_file(self, ray_start_regular_shared, tmp_path, caplog):
        """Test that source is logged when reading a single file."""
        import ray
        import logging
        
        # Create a test file
        import pandas as pd
        df = pd.DataFrame({"a": [1, 2, 3]})
        file_path = str(tmp_path / "test.parquet")
        df.to_parquet(file_path)
        
        # Enable logging
        with caplog.at_level(logging.INFO, logger="ray.data._internal.read_unified"):
            ds = ray.data.read(file_path)
            assert ds.count() == 3
            
            # Check that source was detected and logged
            log_messages = [record.message for record in caplog.records]
            # Should log "Detected data sources: local=1"
            assert any("Detected data sources" in msg for msg in log_messages)
            assert any("local" in msg for msg in log_messages)
    
    def test_multiple_sources_logged(self, ray_start_regular_shared, tmp_path, caplog):
        """Test that multiple sources are logged when reading from different sources."""
        import ray
        import logging
        import pandas as pd
        
        # Create local test files
        df = pd.DataFrame({"a": [1, 2, 3]})
        file1 = str(tmp_path / "test1.parquet")
        file2 = str(tmp_path / "test2.parquet")
        df.to_parquet(file1)
        df.to_parquet(file2)
        
        # Read multiple files
        with caplog.at_level(logging.INFO, logger="ray.data._internal.read_unified"):
            ds = ray.data.read([file1, file2])
            assert ds.count() == 6
            
            # Check that sources were detected
            log_messages = [record.message for record in caplog.records]
            assert any("Detected data sources" in msg for msg in log_messages)


class TestSourceDetectorPerformance:
    """Performance tests for source detection."""
    
    def test_detect_performance_many_paths(self):
        """Test that detection is fast even with many paths."""
        import time
        
        # Generate many paths
        paths = [f"s3://bucket{i}/file{i}.parquet" for i in range(10000)]
        
        start = time.time()
        result = SourceDetector.detect_from_paths(paths)
        elapsed = time.time() - start
        
        # Should be very fast (< 100ms for 10k paths)
        assert elapsed < 0.1
        assert result[DataSource.S3] == 10000
    
    def test_detect_performance_mixed_paths(self):
        """Test detection with mixed source types."""
        import time
        
        # Generate paths with different sources
        paths = []
        for i in range(1000):
            if i % 4 == 0:
                paths.append(f"s3://bucket/file{i}.parquet")
            elif i % 4 == 1:
                paths.append(f"gs://bucket/file{i}.parquet")
            elif i % 4 == 2:
                paths.append(f"/local/file{i}.parquet")
            else:
                paths.append(f"https://example.com/file{i}.parquet")
        
        start = time.time()
        result = SourceDetector.detect_from_paths(paths)
        elapsed = time.time() - start
        
        # Should be fast
        assert elapsed < 0.05
        assert len(result) == 4
        assert result[DataSource.S3] == 250
        assert result[DataSource.GCS] == 250
        assert result[DataSource.LOCAL] == 250
        assert result[DataSource.HTTPS] == 250

