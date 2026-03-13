## pytesting for report_writer.py
import pytest
import os
import csv
import src.sales_analysis.report_writer as report_writer

#================= testing for summary report =================================

# 1. test that a report file is created
def test_reportfile_created(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    report_writer.write_summary_report(filepath, [], [], {})
    assert filepath.exists()

# 2. test that report content is written
def test_summary_report_contains_basic_sections(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    report_writer.write_summary_report(filepath, [], [], {})
    content = filepath.read_text()
    assert "=== Cafe Sales Processing Summary Report ===" in content
    assert "Generated:" in content
    assert "Processing Statistics:" in content
    assert "Error Details:" in content
    assert "Sales by Payment Method:" in content
    assert "Sales by Location:" in content
    assert "Top 5 Products by Quantity Sold:" in content

# 3. test total record count
def test_summary_report_record_counts(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    valid_records = [
        {"total_spent": 5.50},
        {"total_spent": 7.25},
        {"total_spent": 3.00},
        {"total_spent": 10.00},
    ]
    errors = ["Invalid date", "Missing payment method"]
    report_writer.write_summary_report(filepath, valid_records, errors, {})
    content = filepath.read_text()
    assert "- Total Records Processed: 6" in content
    assert "- Valid Records: 4" in content
    assert "- Error Records: 2" in content

# 4. test total spent values
def test_summary_report_total_sales_from_valid_records(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    valid_records = [
        {"total_spent": "5.50"},
        {"total_spent": "7.25"},
        {"total_spent": "2.25"},
    ]
    report_writer.write_summary_report(filepath, valid_records, [], {})
    content = filepath.read_text()
    assert "- Total Sales from Valid Records: $15.00" in content

def test_summary_report_ignores_bad_total_spent_values(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    valid_records = [
        {"total_spent": "5.50"},
        {"total_spent": "bad_data"},
        {"total_spent": None},
        {"total_spent": "4.50"},
    ]
    report_writer.write_summary_report(filepath, valid_records, [], {})
    content = filepath.read_text()
    assert "- Total Sales from Valid Records: $10.00" in content

# 5. test aggregations with payment method
def test_summary_report_includes_payment_method_aggregations(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    aggregations = {
        "sales_by_method": {
            "Card": 120.50,
            "Cash": 80.00,
            "Mobile Pay": 45.25,
        }
    }
    report_writer.write_summary_report(filepath, [], [], aggregations)
    content = filepath.read_text()
    assert "- Card: $120.50" in content
    assert "- Cash: $80.00" in content
    assert "- Mobile Pay: $45.25" in content

# test aggregations with location
def test_summary_report_includes_location_aggregations(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    aggregations = {
        "sales_by_location": {
            "Downtown": 250.00,
            "Airport": 175.75,
        }
    }
    report_writer.write_summary_report(filepath, [], [], aggregations)
    content = filepath.read_text()
    assert "- Downtown: $250.00" in content
    assert "- Airport: $175.75" in content

# 6. test error reporting
def test_summary_report_includes_errors(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    errors = ["Invalid date", "Missing payment method"]
    report_writer.write_summary_report(filepath, [], errors, {})
    content = filepath.read_text()
    assert "Invalid date" in content
    assert "Missing payment method" in content

def test_summary_report_no_errors_message(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    report_writer.write_summary_report(filepath, [], [], {})
    content = filepath.read_text()
    assert "No errors encountered." in content

# 7. test empty dataset handling
def test_summary_report_empty_dataset(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    #write_summary_report(filepath, [], [], {})
    content = filepath.read_text()
    assert "Valid records: 0" in content
    assert "Errors: 0" in content

# 8. test empty aggregations
def test_summary_report_empty_aggregations_show_none(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    report_writer.write_summary_report(filepath, [], [], {})
    content = filepath.read_text()
    assert "Sales by Payment Method:\n- None" in content
    assert "Sales by Location:\n- None" in content
    assert "Top 5 Products by Quantity Sold:\n- None" in content


# 9. test timestamps are included
def test_summary_report_has_timestamp(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    #write_summary_report(filepath, [], [], {})
    content = filepath.read_text()
    assert "Processing timestamp" in content

# 10. test sorting methods
def test_summary_report_sorting_payment_methods_descending(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    aggregations = {
        "sales_by_method": {
            "Cash": 50.00,
            "Card": 200.00,
            "Mobile Pay": 100.00,
        }
    }
    report_writer.write_summary_report(filepath, [], [], aggregations)
    content = filepath.read_text()
    card_index = content.index("- Card: $200.00")
    mobile_index = content.index("- Mobile Pay: $100.00")
    cash_index = content.index("- Cash: $50.00")
    assert card_index < mobile_index < cash_index

def test_summary_report_sorting_locations_descending(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    aggregations = {
        "sales_by_location": {
            "Mall": 99.00,
            "Downtown": 300.00,
            "Airport": 150.00,
        }
    }
    report_writer.write_summary_report(filepath, [], [], aggregations)
    content = filepath.read_text()
    downtown_index = content.index("- Downtown: $300.00")
    airport_index = content.index("- Airport: $150.00")
    mall_index = content.index("- Mall: $99.00")
    assert downtown_index < airport_index < mall_index

# 11. test top 5 products in summary report
def test_summary_report_includes_top_5_products(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    aggregations = {
        "qty_by_product": {
            "Coffee": 20,
            "Cake": 15,
            "Cookie": 12,
            "Salad": 8,
            "Sandwich": 7,
            "Smoothie": 5,
        }
    }
    report_writer.write_summary_report(filepath, [], [], aggregations)
    content = filepath.read_text()
    assert "1. Coffee: 20 units sold" in content
    assert "2. Cake: 15 units sold" in content
    assert "3. Cookie: 12 units sold" in content
    assert "4. Salad: 8 units sold" in content
    assert "5. Sandwich: 7 units sold" in content
    assert "Smoothie" not in content  # only top 5 should appear

# 12. test summary report with one valid record and no errors
def test_summary_report_single_record(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    valid_records = [{"total_spent": "3.50"}]
    report_writer.write_summary_report(filepath, valid_records, [], {})
    content = filepath.read_text()
    assert "- Total Records Processed: 1" in content
    assert "- Valid Records: 1" in content
    assert "- Error Records: 0" in content
    assert "- Total Sales from Valid Records: $3.50" in content

# 13. test summary report handles missing total_spent key
def test_summary_report_missing_total_spent_defaults_to_zero(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    valid_records = [
        {"item": "Coffee"},
        {"total_spent": "5.00"}
    ]
    report_writer.write_summary_report(filepath, valid_records, [], {})
    content = filepath.read_text()
    assert "- Total Sales from Valid Records: $5.00" in content

# 14. test summary report handles integer total_spent values
def test_summary_report_integer_total_spent_values(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    valid_records = [
        {"total_spent": 5},
        {"total_spent": 10},
        {"total_spent": 2}
    ]
    report_writer.write_summary_report(filepath, valid_records, [], {})
    content = filepath.read_text()
    assert "- Total Sales from Valid Records: $17.00" in content

# 15. test summary report with product count less than five
def test_summary_report_products_less_than_five(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    aggregations = {
        "qty_by_product": {
            "Coffee": 8,
            "Cake": 4,
            "Cookie": 2,
        }
    }
    report_writer.write_summary_report(filepath, [], [], aggregations)
    content = filepath.read_text()
    assert "1. Coffee: 8 units sold" in content
    assert "2. Cake: 4 units sold" in content
    assert "3. Cookie: 2 units sold" in content

# 16. test summary report does not show none when payment methods exist
def test_summary_report_payment_methods_not_none_when_data_exists(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    aggregations = {
        "sales_by_method": {
            "Card": 25.00
        }
    }
    report_writer.write_summary_report(filepath, [], [], aggregations)
    content = filepath.read_text()
    assert "- Card: $25.00" in content
    assert "Sales by Payment Method:\n- None" not in content

# 17. test summary report does not show none when locations exist
def test_summary_report_locations_not_none_when_data_exists(tmp_path):
    filepath = tmp_path / "summary_report.txt"
    aggregations = {
        "sales_by_location": {
            "Downtown": 75.00
        }
    }
    report_writer.write_summary_report(filepath, [], [], aggregations)
    content = filepath.read_text()
    assert "- Downtown: $75.00" in content
    assert "Sales by Location:\n- None" not in content


# =============== tests for write csv ==========================

# 1. test that it actually creates a file
def test_write_clean_csv_creates_file(tmp_path):
    filepath = tmp_path / "clean_sales.csv"
    records = [
        {
            "transaction_id": "T001",
            "item": "Coffee",
            "quantity": 2,
            "price_per_unit": 3.50,
            "total_spent": 7.00,
            "payment_method": "Card",
            "location": "Downtown",
            "transaction_date": "2026-03-10"
        }
    ]
    report_writer.write_clean_csv(filepath, records)
    assert filepath.exists()

# 2. test that it has a good header and rows
def test_write_clean_csv_writes_header_and_rows(tmp_path):
    filepath = tmp_path / "clean_sales.csv"
    records = [
        {
            "transaction_id": "T001",
            "item": "Coffee",
            "quantity": 2,
            "price_per_unit": 3.50,
            "total_spent": 7.00,
            "payment_method": "Card",
            "location": "Downtown",
            "transaction_date": "2026-03-10"
        },
        {
            "transaction_id": "T002",
            "item": "Cake",
            "quantity": 1,
            "price_per_unit": 4.25,
            "total_spent": 4.25,
            "payment_method": "Cash",
            "location": "Airport",
            "transaction_date": "2026-03-11"
        }
    ]
    report_writer.write_clean_csv(filepath, records)
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
    assert len(reader) == 2
    assert reader[0]["transaction_id"] == "T001"
    assert reader[0]["item"] == "Coffee"
    assert reader[1]["transaction_id"] == "T002"
    assert reader[1]["item"] == "Cake"

# 3. test that any missing fields are blank
def test_write_clean_csv_missing_fields_become_blank(tmp_path):
    filepath = tmp_path / "clean_sales.csv"

    records = [
        {
            "transaction_id": "T001",
            "item": "Coffee"
        }
    ]

    report_writer.write_clean_csv(filepath, records)

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))

    assert reader[0]["transaction_id"] == "T001"
    assert reader[0]["item"] == "Coffee"
    assert reader[0]["quantity"] == ""
    assert reader[0]["payment_method"] == ""

# 4. test clean csv writes correct header fields
def test_write_clean_csv_has_expected_headers(tmp_path):
    filepath = tmp_path / "clean_sales.csv"
    report_writer.write_clean_csv(filepath, [])
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
    assert header == [
        "transaction_id",
        "item",
        "quantity",
        "price_per_unit",
        "total_spent",
        "payment_method",
        "location",
        "transaction_date"
    ]

# 5. test clean csv with no records only writes header
def test_write_clean_csv_empty_records_only_header(tmp_path):
    filepath = tmp_path / "clean_sales.csv"
    report_writer.write_clean_csv(filepath, [])
    with open(filepath, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert len(rows) == 1, "empty clean csv should only contain the header row"

# 6. test extra fields are ignored
def test_write_clean_csv_ignores_extra_fields(tmp_path):
    filepath = tmp_path / "clean_sales.csv"
    records = [
        {
            "transaction_id": "T001",
            "item": "Coffee",
            "quantity": 2,
            "price_per_unit": 3.50,
            "total_spent": 7.00,
            "payment_method": "Card",
            "location": "Downtown",
            "transaction_date": "2026-03-10",
            "extra_field": "ignore me"
        }
    ]
    report_writer.write_clean_csv(filepath, records)
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
    assert "extra_field" not in reader[0]
    assert reader[0]["transaction_id"] == "T001"

# 7. test numeric values are written as strings in csv
def test_write_clean_csv_numeric_values_written(tmp_path):
    filepath = tmp_path / "clean_sales.csv"
    records = [
        {
            "transaction_id": "T001",
            "item": "Coffee",
            "quantity": 2,
            "price_per_unit": 3.50,
            "total_spent": 7.00,
            "payment_method": "Card",
            "location": "Downtown",
            "transaction_date": "2026-03-10"
        }
    ]
    report_writer.write_clean_csv(filepath, records)
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
    assert reader[0]["quantity"] == "2"
    assert reader[0]["price_per_unit"] == "3.5"
    assert reader[0]["total_spent"] == "7.0"

# ========================== tests for error log ==========================

# 1. test that the error log is created
def test_write_error_log_creates_file(tmp_path):
    filepath = tmp_path / "error_log.txt"
    report_writer.write_error_log(filepath, ["Missing item"])
    assert filepath.exists()

# 2. test that it has good header and has errors
def test_write_error_log_contains_header_and_errors(tmp_path):
    filepath = tmp_path / "error_log.txt"
    errors = ["Missing item", "Invalid date"]
    report_writer.write_error_log(filepath, errors)
    content = filepath.read_text()
    assert "=== Cafe Sales Error Log ===" in content
    assert "Generated:" in content
    assert "Total Errors: 2" in content
    assert "1. Missing item" in content
    assert "2. Invalid date" in content

# 3. test that if no errors occured, it will say so
def test_write_error_log_no_errors_message(tmp_path):
    filepath = tmp_path / "error_log.txt"
    report_writer.write_error_log(filepath, [])
    content = filepath.read_text()
    assert "No errors encountered." in content

# 4. test error log does not show total errors when list is empty
def test_write_error_log_no_total_errors_line_when_empty(tmp_path):
    filepath = tmp_path / "error_log.txt"
    report_writer.write_error_log(filepath, [])
    content = filepath.read_text()
    assert "Total Errors:" not in content


# 5. test error log counts errors correctly
def test_write_error_log_total_error_count(tmp_path):
    filepath = tmp_path / "error_log.txt"
    errors = ["Error one", "Error two", "Error three"]
    report_writer.write_error_log(filepath, errors)
    content = filepath.read_text()
    assert "Total Errors: 3" in content

# 6. test error log numbers all errors
def test_write_error_log_numbers_each_error(tmp_path):
    filepath = tmp_path / "error_log.txt"
    errors = ["Missing item", "Invalid date", "Bad payment method"]
    report_writer.write_error_log(filepath, errors)
    content = filepath.read_text()
    assert "1. Missing item" in content
    assert "2. Invalid date" in content
    assert "3. Bad payment method" in content


# 7. test error log has generated timestamp line
def test_write_error_log_has_generated_line(tmp_path):
    filepath = tmp_path / "error_log.txt"
    report_writer.write_error_log(filepath, ["Missing item"])
    content = filepath.read_text()
    assert "Generated:" in content