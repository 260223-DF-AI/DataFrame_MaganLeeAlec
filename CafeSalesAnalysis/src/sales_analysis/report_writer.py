import csv
from datetime import datetime


def write_summary_report(filepath, valid_records, errors, aggregations):
    """
    Write a formatted summary report for a cafe sales dataset.

    Report includes:
    - Processing timestamp
    - Total records processed
    - Number of valid records
    - Number of error records
    - Error details
    - Sales by payment method
    - Sales by location
    - Top 5 products by quantity sold
    """

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_records = len(valid_records) + len(errors)

    # Pull aggregation data safely
    sales_by_method = aggregations.get("sales_by_method", {})
    qty_by_product = aggregations.get("qty_by_product", {})
    sales_by_location = aggregations.get("sales_by_location", {})

    # Sort payment methods from highest sales to lowest
    sorted_methods = sorted(
        sales_by_method.items(),
        key=lambda x: x[1],
        reverse=True
    )

    # Sort locations from highest sales to lowest
    sorted_locations = sorted(
        sales_by_location.items(),
        key=lambda x: x[1],
        reverse=True
    )

    # Sort products by quantity sold and keep top 5
    sorted_products = sorted(
        qty_by_product.items(),
        key=lambda x: x[1],
        reverse=True
    )[:5]

    # Calculate total sales from valid records if possible
    total_sales = 0.0
    for record in valid_records:
        try:
            total_sales += float(record.get("total_spent", 0))
        except (ValueError, TypeError):
            pass

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("=== Cafe Sales Processing Summary Report ===\n")
        f.write(f"Generated: {timestamp}\n\n")

        f.write("Processing Statistics:\n")
        f.write(f"- Total Records Processed: {total_records}\n")
        f.write(f"- Valid Records: {len(valid_records)}\n")
        f.write(f"- Error Records: {len(errors)}\n")
        f.write(f"- Total Sales from Valid Records: ${total_sales:.2f}\n\n")

        f.write("Error Details:\n")
        if errors:
            for i, error in enumerate(errors, start=1):
                f.write(f"{i}. {error}\n")
        else:
            f.write("No errors encountered.\n")
        f.write("\n")

        f.write("Sales by Payment Method:\n")
        if sorted_methods:
            for payment_method, total in sorted_methods:
                f.write(f"- {payment_method}: ${total:.2f}\n")
        else:
            f.write("- None\n")
        f.write("\n")

        f.write("Sales by Location:\n")
        if sorted_locations:
            for location, total in sorted_locations:
                f.write(f"- {location}: ${total:.2f}\n")
        else:
            f.write("- None\n")
        f.write("\n")

        f.write("Top 5 Products by Quantity Sold:\n")
        if sorted_products:
            for i, (product, qty) in enumerate(sorted_products, start=1):
                f.write(f"{i}. {product}: {qty} units sold\n")
        else:
            f.write("- None\n")


def write_clean_csv(filepath, records):
    """
    Write validated cafe sales records to a clean CSV file.
    """

    fieldnames = [
        "transaction_id",
        "item",
        "quantity",
        "price_per_unit",
        "total_spent",
        "payment_method",
        "location",
        "transaction_date"
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for record in records:
            clean_row = {
                "transaction_id": record.get("transaction_id", ""),
                "item": record.get("item", ""),
                "quantity": record.get("quantity", ""),
                "price_per_unit": record.get("price_per_unit", ""),
                "total_spent": record.get("total_spent", ""),
                "payment_method": record.get("payment_method", ""),
                "location": record.get("location", ""),
                "transaction_date": record.get("transaction_date", "")
            }
            writer.writerow(clean_row)


def write_error_log(filepath, errors):
    """
    Write processing errors to a detailed error log file.
    """

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("=== Cafe Sales Error Log ===\n")
        f.write(f"Generated: {timestamp}\n\n")

        if not errors:
            f.write("No errors encountered.\n")
        else:
            f.write(f"Total Errors: {len(errors)}\n\n")
            for i, error in enumerate(errors, start=1):
                f.write(f"{i}. {error}\n")