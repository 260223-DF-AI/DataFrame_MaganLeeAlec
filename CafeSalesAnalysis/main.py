"""Demo code"""
from src.sales_analysis import file_reader, validation, clean_data, database, report_writer
import pandas as pd
# get contents of csv as dataframe
df = file_reader.read_csv_full("dirty_cafe_sales.csv")

# declare data types of entries
list_from_schema = validation.create_schema(df, user_input=False)

# validate data types of entries
validated = validation.validate_data(df, list_from_schema, user_input=False)

# separate clean data and rejects
# TODO: throws typeerror
#tuples have index [0] for valid data, [1] for invalid data
null_checked_tuple = clean_data.remove_all_null(validated)
null_checked_unique_tuple = clean_data.remove_duplicate_entries(null_checked_tuple[0])
clean_sales, rejects = null_checked_unique_tuple[0], pd.concat([null_checked_tuple[1], null_checked_unique_tuple[1]])

print(rejects)
# write summary report of data to a new file
# TODO: what is aggregations & how can I fill in this parameter here?
#report_writer.write_summary_report("report.txt", clean_sales, rejects, ______)

# write data to database
#database.write_from_dataframe(clean_sales)
#database.write_from_dataframe(rejects, "reject_entries")
