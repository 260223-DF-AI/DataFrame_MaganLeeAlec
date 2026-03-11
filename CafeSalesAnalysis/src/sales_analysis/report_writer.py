import csv
from datetime import datetime

def write_summary_report(filepath, valid_records, errors, aggregations):
    """
    Write a formatted summary report.
    
    Report should include:
    - Processing timestamp
    - Total records processed
    - Number of valid records
    - Number of errors (with details)
    - Payment method of sales
    - Top 5 products
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_records = len(valid_records) + len(errors)
    sales_by_method = aggregations.get("sales_by_method", {})
    qty_by_product = aggregations.get("qty_by_product", {})

    #sort store totals (descending)
    sorted_methods = sorted(sales_by_method.items(),key = lambda x: x[1], reverse = True)
    #sort products by quantity sold (descending)
    sorted_products = sorted(qty_by_product.items(), key = lambda x: x[1], reverse = True)[:5]

    with open(filepath, "w", encoding = "utf-8") as f:
        f.write("=== Sales Processing Report ===\n")
        f.write(f"Generated: {timestamp}\n\n")

        f.write("Processing Statistics:\n")
        f.write(f"-Total Records: {total_records}\n")
        f.write(f"-Valid Records: {len(valid_records)}\n")
        f.write(f"-Error Records: {len(errors)}\n\n")

        f.write("Errors:\n")
        if errors:
            for error in errors:
                f.write(f"- {error}\n")
        else:
            f.write("No errors encountered.\n")
        f.write("\n")

        f.write("Payment methods::\n") #method of payment (listed in most used to least)
        if sorted_methods:
            for payment_method, total in sorted_methods:
                f.write(f"- {payment_method}: ${total:.2f}\n")
        else:
            f.write("- None\n")
        f.write("\n")

        f.write("Top Products:\n") #top 5 products
        if sorted_products:
            for i, (product, qty) in enumerate(sorted_products, start=1):
                f.write(f"{i}. {product}: {qty} units\n")
        else:
            f.write("- None\n")

def write_clean_csv(filepath, records):
    """
    Write validated records to a clean CSV file.
    """
    if not records:
        fieldnames = ["date", "method", "product", "quantity", "price"]
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
        return
    fieldnames = ["date", "method", "product", "quantity", "price"]
    with open(filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record)

def write_error_log(filepath, errors):
    """
    Write processing errors to a log file.
    """
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("=== Error Log ===\n")
        if not errors:
            f.write("No errors encountered.\n")
        else:
            for error in errors:
                f.write(error + "\n")
