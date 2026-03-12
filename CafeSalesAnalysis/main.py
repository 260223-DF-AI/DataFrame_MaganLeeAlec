from src.sales_analysis import file_reader, validation, clean_data, database, report_writer
import pandas as pd

df = file_reader.read_csv_full("dirty_cafe_sales.csv")
list_from_schema = validation.create_schema(df, user_input=False)
validated = validation.validate_data(df, list_from_schema)

"""
1. read data
2. validate data
3. clean data
4. show report
"""