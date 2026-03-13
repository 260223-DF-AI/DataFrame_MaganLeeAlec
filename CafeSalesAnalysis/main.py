"""Demo code"""
from src.sales_analysis import file_reader, validation, clean_data, database, report_writer

# get contents of csv as dataframe
df = file_reader.read_csv_full("dirty_cafe_sales.csv")

# declare data types of entries
list_from_schema = validation.create_schema(df, user_input=False)

# validate data types of entries
validated = validation.validate_data(df, list_from_schema)

# separate clean data and rejects
# TODO: throws typeerror
clean_tuple = clean_data.remove_all_null(validated)
clean_sales, rejects = clean_tuple[0], clean_tuple[1]

# write summary report of data to a new file
# TODO: what is aggregations & how can I fill in this parameter here?
report_writer.write_summary_report("report.txt", clean_sales, rejects, ______)

# write data to database
database.write_from_dataframe(clean_sales)
database.write_from_dataframe(rejects, "reject_entries")
