## pytesting for report_writeer.py
import pytest
import os
import csv
import src.sales_analysis.report_writer as report_writer

# 1. test that a report file is created
def test_reportfile_created(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    #write_summary_report(filepath, [], [], {})
    assert os.path.exists(filepath)

# 2. test that report content is written
def test_summary_report_contains_basic_sections(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    #write_summary_report(filepath, [], [], {})
    content = filepath.read_text()
    assert "Processing Summary" in content

# 3. test total record count
def test_summary_report_record_counts(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    valid_records = [1,2,3,4]
    errors = ["error1", "error2"]
    #write_summary_report(filepath, valid_records, errors, {})
    content = filepath.read_text()
    assert "Valid records: 4" in content
    assert "Errors: 2" in content

# 4. test aggregations appear in the report
def test_summary_report_includes_aggregations(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    aggregations = {
        "total_sales": 1000,
        "average_sales": 200
    }
    #write_summary_report(filepath, [], [], aggregations)
    content = filepath.read_text()
    assert "total_sales" in content
    assert "1000" in content

# 5. test error reporting
def test_summary_report_includes_errors(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    errors = ["Invalid date", "Missing payment method"]
    #write_summary_report(filepath, [], errors, {})
    content = filepath.read_text()
    assert "Invalid date" in content
    assert "Missing payment method" in content

# 6. test empty dataset handling
def test_summary_report_empty_dataset(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    #write_summary_report(filepath, [], [], {})
    content = filepath.read_text()
    assert "Valid records: 0" in content
    assert "Errors: 0" in content

# 7. test timestamps are included
def test_summary_report_has_timestamp(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    #write_summary_report(filepath, [], [], {})
    content = filepath.read_text()
    assert "Processing timestamp" in content

# 8. test csv report writing
def test_error_report_csv_created(tmp_path):
    filepath = tmp_path / "testresult_errors.csv"
    errors = [{"row":1,"error":"missing field"}]
    #write_error_report(filepath, errors)
    assert filepath.exists()

def test_error_report_csv_content(tmp_path):
    filepath = tmp_path / "testresult_errors.csv"
    errors = [
        {"row":1,"error":"missing field"},
        {"row":2,"error":"invalid date"}
    ]
    #write_error_report(filepath, errors)
    content = filepath.read_text()
    assert "missing field" in content
    assert "invalid date" in content

# 9. test repdef test_report_formatting(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    #write_summary_report(filepath, [], [], {})
    content = filepath.read_text()
    assert "=" in content

# 10. test invalid file path handling
#def test_report_writer_invalid_path():
    #with pytest.raises(Exception):
        #write_summary_report("/invalid/path/report.txt", [], [], {})