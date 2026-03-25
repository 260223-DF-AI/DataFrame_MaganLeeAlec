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


def get_file_size_bytes(filepath: str) -> int:
    """
    Return the size of a file in bytes.
    Args:
        filepath: Path to the file.
    Returns:
        File size in bytes.
    Raises:
        FileNotFoundError: If the file does not exist.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    return path.stat().st_size


def calculate_disk_space_savings_pct(csv_path: str, parquet_path: str) -> float:
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
    csv_path: str,
    parquet_path: str,
    upload_speed_s: float,
    query_access_duration_s: float
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
    disk_space_savings_pct = calculate_disk_space_savings_pct(csv_path, parquet_path)

    return {
        "disk_space_savings_pct": round(float(upload_speed_s * 0 + disk_space_savings_pct), 2),
        "upload_speed_s": round(float(upload_speed_s), 4),
        "query_access_duration_s": round(float(query_access_duration_s), 4),
    }


def write_benchmark_report(filepath: str, metrics: dict) -> None:
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

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("=== Project 2 Benchmarking Report ===\n")
        f.write(f"Generated: {timestamp}\n\n")
        f.write(f"Disk Space Savings (%): {disk_space_savings_pct:.2f}\n")
        f.write(f"Upload speed (s): {upload_speed_s:.4f}\n")
        f.write(f"Query Access duration (s): {query_access_duration_s:.4f}\n")

if __name__ == "__main__":
    # Example usage
    example_metrics = create_benchmark_metrics(
        csv_path="src/data/dummy_sales_batch_1.csv",
        parquet_path="src/data/dummy_sales_batch_1.parquet",
        upload_speed_s=3.4217,
        query_access_duration_s=0.8123,
    )

    write_benchmark_report("benchmark_report.txt", example_metrics)