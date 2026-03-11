"""This script will handle validation of data in a Data Frame"""
import pandas as pd
import numpy as np
from datetime import datetime
from .clean_data import remove_all_null, remove_duplicate_entries, replace_values
from .logger import setup_logger

def create_schema(data: pd.DataFrame):
	if not isinstance(data, pd.DataFrame):
		setup_logger(__name__, 'error', f"Error in validation::create_schema - variable: data <- was expecting pd.DataFrame, but got {type(data)}")
	column_schema = []
	valid_cols = [str, int, float, bool, datetime]
	
	for col in data.columns:
		dtype = input(f"Enter a data type for column {col}. \nAccepted types are string, int, float, and bool: ")
		match(dtype.lower()):
			case "string":
				column_schema.append({f"{col}": str})
			case "int":
				column_schema.append({f"{col}": int})
			case "float":
				column_schema.append({f"{col}": float})
			case "bool":
				column_schema.append({f"{col}": bool})
			case _:
				setup_logger(__name__, 'warning', f"Warning in validation::create_schema. Input expected a string, int, float, bool, or datetime, but received an invalid datatype of {dtype}. defaulting to string")
				column_schema.append({f"{col}": str})
    
	return column_schema

def validate_data(data: pd.DataFrame, column_schema: list):
		"""Changes data type to the declared type from the input"""

		invalid_cell = ["NaN", "EMPTY", "empty", "UNKNOWN", "unknown", "ERROR", "error", "NA", "Na", "None", "NULL", "null", np.nan]
		for target in invalid_cell:
			data.replace(target, -1, inplace=True)
	
		for col_dict in column_schema:
			for col_label in col_dict:
				data.fillna(-1, inplace=True)
				data[col_label] = data[col_label].astype(col_dict.get(col_label))

		print(column_schema)
  
		# Constrained columns should have a value / NOT be -1
		constrained_cols = []
		null_value = -1
		while(True):
			column = input("Enter a column to drop NULL/-1 values (Q to quit): ")
			if column.lower() == 'q':
				break
			constrained_cols.append(column)
   
		# Search through constriained columns to filter out the placeholder null value (-1)
		# once filtered, log the rows that were removed
		for col in constrained_cols:
			if col:
				filter_keep = data[col] > type(data[col][1])(null_value)
				setup_logger(__name__, 'warning', f"Records: \n{data[data[col] ==  type(data[col][1])(null_value)]} was removed")
				data = data[filter_keep]

		return data

#Obsolete
def validate_record(record: pd.Series) -> tuple:
	"""Validates a record. Record must be a Pandas Series object. Returns a tuple of (pd.Series, error_list)
 		Required fields in series: Transaction ID: str, Item: str, Quantity: int, Price Per Unit: float, Total Spent: float, Payment Method: str, Location: str, Transaction Date: datetime"""
	valid_cols = [str, str, int, float, float, str, str, datetime]
	if not isinstance(record, pd.Series):
		setup_logger(__name__, 'error', f"Error in validation::validate_record - variable: record <- was expecting pd.Series, but got {type(record)}")
		raise TypeError("record isn't a pandas Series")
	if len(record) != len[valid_cols]:
		setup_logger(__name__, 'error', f"Error in validation::validate_record - variable: record <- length doesn't match required fields")
		raise Exception("record doesn't have all the required fields")

	for i in len(record):
		if not isinstance(record[i], valid_cols[i]):
			setup_logger(__name__, 'error', f"Error in validation::validate_record - variable: record[{i}] <- was expecting {valid_cols[i]}, but got {type(record[i])}")
			raise TypeError("")
#Obsolete
def is_valid_row(record: pd.Series, line_num: int):
	invalid_cell = ["NaN", "EMPTY", "UNKNOWN", "ERROR", np.nan, ""]
	if not isinstance(record, pd.Series):
		setup_logger(__name__, 'error', f"Error in validation::is_valid_row - variable: record <- was expecting pd.Series, but got {type(record)}")
		raise TypeError("invalid datatype")
	for cell in record:
		if cell in invalid_cell:
			setup_logger(__name__, 'warning', f"Line {line_num}: Missing value for cell")
			return False
	return True
#Obsolete
def validate_record_input(record: pd.Series) -> tuple:
	"""Validates a record. Record must be a Pandas Series object. Returns a tuple of (pd.Series, is_valid_row, error_list)"""
 
	error_list = []
	casted_list = []
	
	if not isinstance(record, pd.Series):
		setup_logger(__name__, 'error', f"Error in validation::validate_record - variable: record <- was expecting pd.Series, but got {type(record)}")
		error_list.append(TypeError("Record type isn't a pandas Series object"))
		return (pd.Series(), error_list)
	print("Enter the data types for each field. options: string, int, float, bool, datetime", end="\n\n")
	is_valid_row = True
	# Let user decide which columns in the row gets which data type they enter
	# Log whenever unexpected input or errors arise
	for field in record:
		try:
			response = input(f"enter data type for {field}: ")
			match(response.lower()):
				case "string":
					casted_list.append(str(field))
				case "int":
					casted_list.append(int(field))
				case "float":
					casted_list.append(float(field))
				case "bool":
					casted_list.append(bool(field))
				case "datetime":
					casted_list.append(datetime.strptime(field, "%Y-%d-%m"))
				case _:
					setup_logger(__name__, 'warning', f"Warning in validation::validate_record. Input expected a string, int, float, bool, or datetime, but received an invalid datatype of {response}. Defaulting to original data type")
					casted_list.append(field)
					is_valid_row = False
		except ValueError as e:
			setup_logger(__name__, 'error', f"Error in validation::validate_record. Input expected a string, int, float, bool, or datetime, but received an invalid datatype of {response}")
			error_list.append(TypeError(f"Error in validation::validate_record. Input expected a string, int, float, bool, or datetime, but received an invalid datatype of {response}"))
			casted_list.append(field)
			is_valid_row = False
	return (pd.Series(casted_list), is_valid_row, error_list)
#Obsolete
def change_col_dtype(data: pd.DataFrame | dict, column_str: str, type_change: type) -> pd.DataFrame:
    """Changes the data type of a column_str in the data frame"""

    # Raise exceptions if data types aren't supported
    if not isinstance(column_str, str):
        raise TypeError("Column must be a string")
    if not isinstance(data, (pd.DataFrame, dict)):
        raise TypeError("data must be a Pandas Data Frame or dictionary")
    if not isinstance(type_change, (type, datetime)):
        raise TypeError("Type to change to must be a python primitive type or DateTime object")
    
    data_frame = None
    if isinstance(data, dict):
        data_frame = pd.DataFrame(data)
        data_frame[column_str] = data_frame[column_str].astype(type_change)
        return data_frame
    else:
        data[column_str] = data[column_str].astype(type_change)
        return data

# df = pd.read_csv("src/data/dirty_cafe_sales.csv")

# list_from_schema = create_schema(df)
# validated = validate_data(df, list_from_schema)
# print(validated)
# print(validated)
# validated = remove_all_null(validated)
# print(validated)