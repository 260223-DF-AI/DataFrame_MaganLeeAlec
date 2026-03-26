"""
Benchmark report writer for Project 2.
This module creates a benchmarking report for the cloud-native pipeline.
It focuses on:
- Disk Space Savings (%)
- Upload speed (s)
- Query Access duration (s)

Typical usage:
    metrics = create_benchmark_metrics(
        csv_path="src/data/dummy_sales_batch_1.csv",
        parquet_path="src/data/dummy_sales_batch_1.parquet",
        upload_speed_s=3.42,
        query_access_duration_s=0.81
    )

    write_benchmark_report("benchmark_report.txt", metrics)
"""
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable, Any
import os
import time
from src.paths import DATA_DIR

def build_batch_file_paths(prefix: str = "dummy_sales_batch", batch_count: int = 5) -> dict:
    """
    Build lists of CSV and Parquet file paths for all batch files.
    Returns:
        {
            "csv": [Path(...), ...],
            "parquet": [Path(...), ...]
        }
    """
    csv_files = [DATA_DIR / f"{prefix}_{i}.csv" for i in range(1, batch_count + 1)]
    parquet_files = [DATA_DIR / f"{prefix}_{i}.parquet" for i in range(1, batch_count + 1)]
    return {
        "csv": csv_files,
        "parquet": parquet_files
    }


def get_file_size_bytes(filepath: list[Path]) -> int:
    """
    Return the size of a file in bytes.
    Args:
        filepath: Path to the file.
    Returns:
        File size in bytes.
    Raises:
        FileNotFoundError: If the file does not exist.
    """
    total_size = 0
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    total_size += filepath.stat().st_size
    return total_size


def calculate_disk_space_savings_pct(csv_path: list[Path], parquet_path: list[Path]) -> float:
    """
    Calculate how much disk space was saved by converting a CSV file to Parquet.
    Formula:
        ((csv_size - parquet_size) / csv_size) * 100
    Args:
        csv_path: Path to the original CSV file.
        parquet_path: Path to the converted Parquet file.
    Returns:
        Disk space savings as a percentage.
    Raises:
        ValueError: If the CSV file is empty.
        FileNotFoundError: If either file does not exist.
    """
    csv_size = get_file_size_bytes(csv_path)
    parquet_size = get_file_size_bytes(parquet_path)

    if csv_size == 0:
        raise ValueError("CSV file size is 0 bytes; cannot calculate savings percentage.")

    savings_pct = ((csv_size - parquet_size) / csv_size) * 100
    return round(savings_pct, 2)

def measure_query_access_duration(query_callable: Callable[..., Any], *args, **kwargs) -> float:
    """
    Measure how long a query operation takes to run.
    Args:
        query_callable: A function that performs the query.
        *args: Positional args for the query function.
        **kwargs: Keyword args for the query function.
    Returns:
        Duration in seconds, rounded to 4 decimal places.
    """
    start_time = time.time()
    query_callable(*args, **kwargs)
    duration = time.time() - start_time
    return round(duration, 4)

def create_benchmark_metrics(
    upload_speed_s: float,
    query_access_duration_s: float,
    query_execution_time_s: float,
    prefix: str = "dummy_sales_batch",
    batch_count: int = 5
) -> dict:
    """
    Build a metrics dictionary for the benchmark report.
    Args:
        csv_path: Path to the source CSV.
        parquet_path: Path to the converted Parquet file.
        upload_speed_s: Time taken to upload to cloud storage in seconds.
        query_access_duration_s: Time taken to access/query data in seconds.
    Returns:
        Dictionary containing benchmark metrics.
    """
    csv_path = DATA_DIR / f"{prefix}_1.csv"
    parquet_path = DATA_DIR / f"{prefix}_1.parquet"
    batch_files = build_batch_file_paths(prefix=prefix, batch_count=batch_count)
    disk_space_savings_pct = calculate_disk_space_savings_pct(csv_path, parquet_path)

    return {
        "disk_space_savings_pct": round(float(upload_speed_s * 0 + disk_space_savings_pct), 2),
        "upload_speed_s": round(float(upload_speed_s), 4),
        "query_access_duration_s": round(float(query_access_duration_s), 4),
        "query_execution_time_s": round(float(query_execution_time_s), 4),
        "csv_files": batch_files["csv"],
        "parquet_files": batch_files["parquet"]
    }


def write_benchmark_report(filepath: str | Path, metrics: dict) -> None:
    """
    Write the benchmark report to a text file.
    Required keys in metrics:
        - disk_space_savings_pct
        - upload_speed_s
        - query_access_duration_s
    Args:
        filepath: Output report file path.
        metrics: Dictionary of benchmark metrics.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    disk_space_savings_pct = metrics.get("disk_space_savings_pct", 0.0)
    upload_speed_s = metrics.get("upload_speed_s", 0.0)
    query_access_duration_s = metrics.get("query_access_duration_s", 0.0)
    query_execution_time_s = metrics.get("query_execution_time_s", 0.0)
    csv_files = metrics.get("csv_files", [])
    parquet_files = metrics.get("parquet_files", [])

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("=== Project 2 Benchmarking Report ===\n")
        f.write(f"Generated: {timestamp}\n\n")
        f.write("Files Included:\n")
        f.write(f"- CSV files: {len(csv_files)}\n")
        f.write(f"- Parquet files: {len(parquet_files)}\n\n")
        f.write("Benchmark Results:\n")
        f.write(f"Disk Space Savings (%): {disk_space_savings_pct:.2f}\n")
        f.write(f"Most recent upload speed (s): {upload_speed_s:.4f}\n")
        f.write(f"Most recent query access duration (s): {query_access_duration_s:.4f}\n")
        f.write(f"Most recent query execution time (s): {upload_speed_s + query_access_duration_s:.4f}\n")

if __name__ == "__main__":
    batch_files = build_batch_file_paths()

    print("CSV files:")
    for csv_path in batch_files["csv"]:
        print(csv_path)

    print("\nParquet files:")
    for parquet_path in batch_files["parquet"]:
        print(parquet_path)

    metrics = create_benchmark_metrics(
        upload_speed_s=3.4217,
        query_access_duration_s=0.8123,
        query_execution_time_s=4.2345
    )

    write_benchmark_report(DATA_DIR / "benchmark_report.txt", metrics)