import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
import src.sales_analysis.file_reader as file_reader


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
@pytest.fixture
def sample_csv_file(tmp_path, monkeypatch):
    """Create a temporary CSV file and point DATA_DIR to tmp_path."""
    monkeypatch.setattr(file_reader, "DATA_DIR", tmp_path)

    csv_file = tmp_path / "test_sales.csv"
    csv_file.write_text(
        "transaction_id,item,price\n"
        "1,coffee,4.50\n"
        "2,cake,5.25\n"
        "3,salad,8.00\n"
    )
    return "test_sales.csv"

@pytest.fixture
def sample_json_file(tmp_path, monkeypatch):
    """Create a temporary JSON file and point DATA_DIR to tmp_path."""
    monkeypatch.setattr(file_reader, "DATA_DIR", tmp_path)
    json_file = tmp_path / "test_sales.json"
    json_file.write_text(
        '[{"transaction_id": 1, "item": "coffee", "price": 4.50},'
        ' {"transaction_id": 2, "item": "cake", "price": 5.25}]'
    )
    return "test_sales.json"

@pytest.fixture
def sample_json_lines_file(tmp_path, monkeypatch):
    """Create a temporary JSON lines file for chunked reading."""
    monkeypatch.setattr(file_reader, "DATA_DIR", tmp_path)
    jsonl_file = tmp_path / "test_sales_lines.json"
    jsonl_file.write_text(
        '{"transaction_id": 1, "item": "coffee", "price": 4.50}\n'
        '{"transaction_id": 2, "item": "cake", "price": 5.25}\n'
        '{"transaction_id": 3, "item": "salad", "price": 8.00}\n'
    )
    return "test_sales_lines.json"

# ======== Tests for read_csv_full =================

def test_read_csv_full_returns_dataframe(sample_csv_file):
    df = file_reader.read_csv_full(sample_csv_file)
    expected = pd.DataFrame({
        "transaction_id": [1, 2, 3],
        "item": ["coffee", "cake", "salad"],
        "price": [4.50, 5.25, 8.00],
    })
    assert isinstance(df, pd.DataFrame)
    assert_frame_equal(df, expected)


def test_read_csv_full_missing_file_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(file_reader, "DATA_DIR", tmp_path)
    result = file_reader.read_csv_full("does_not_exist.csv")
    assert result is None

# ============ Tests for read_csv_nlines ====================

def test_read_csv_nlines_returns_iterable(sample_csv_file):
    chunks = list(file_reader.read_csv_nlines(sample_csv_file, nlines=2))
    assert len(chunks) == 2
    assert isinstance(chunks[0], pd.DataFrame)
    assert isinstance(chunks[1], pd.DataFrame)
    assert len(chunks[0]) == 2
    assert len(chunks[1]) == 1


def test_read_csv_nlines_default_one_line(sample_csv_file):
    chunks = list(file_reader.read_csv_nlines(sample_csv_file))
    assert len(chunks) == 3
    assert all(isinstance(chunk, pd.DataFrame) for chunk in chunks)
    assert all(len(chunk) == 1 for chunk in chunks)


def test_read_csv_nlines_missing_file_returns_empty_string(tmp_path, monkeypatch):
    monkeypatch.setattr(file_reader, "DATA_DIR", tmp_path)
    result = file_reader.read_csv_nlines("missing.csv", nlines=2)
    assert result == ""

# ============= Tests for read_json_full =====================

def test_read_json_full_returns_dataframe(sample_json_file):
    df = file_reader.read_json_full(sample_json_file)
    expected = pd.DataFrame([
        {"transaction_id": 1, "item": "coffee", "price": 4.50},
        {"transaction_id": 2, "item": "cake", "price": 5.25},
    ])
    assert isinstance(df, pd.DataFrame)
    assert_frame_equal(df, expected)

def test_read_json_full_missing_file_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(file_reader, "DATA_DIR", tmp_path)
    result = file_reader.read_json_full("missing.json")
    assert result is None

# ============= Tests for read_json_nlines ======================

def test_read_json_nlines_returns_iterable(sample_json_lines_file):
    chunks = list(file_reader.read_json_nlines(sample_json_lines_file, nlines=2))
    assert len(chunks) == 2
    assert isinstance(chunks[0], pd.DataFrame)
    assert isinstance(chunks[1], pd.DataFrame)
    assert len(chunks[0]) == 2
    assert len(chunks[1]) == 1


def test_read_json_nlines_missing_file_returns_empty_string(tmp_path, monkeypatch):
    monkeypatch.setattr(file_reader, "DATA_DIR", tmp_path)
    result = file_reader.read_json_nlines("missing.json", nlines=2)
    assert result == ""

# ============ Tests for print_df_preview ==================

def test_print_df_preview_outputs_last_five_rows(capsys):
    df = pd.DataFrame({
        "transaction_id": [1, 2, 3, 4, 5, 6],
        "item": ["a", "b", "c", "d", "e", "f"]
    })
    file_reader.print_df_preview(df)
    captured = capsys.readouterr()
    # tail(5) should exclude transaction_id 1 and include 2 through 6
    assert "1" not in captured.out or " a" not in captured.out
    assert "2" in captured.out
    assert "6" in captured.out
    assert "f" in captured.out