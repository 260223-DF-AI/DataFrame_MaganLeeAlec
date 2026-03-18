"""This script will handle validation of data in a Data Frame"""
import pandas as pd
import numpy as np
from .clean_data import remove_all_null, remove_duplicate_entries, replace_values
from .logger import setup_logger

#-- Lee's portion --
def remove_duplicate_entries(data: pd.DataFrame) -> pd.DataFrame:
    """Returns dataframe with duplicate entries removed."""
    return data.drop_duplicates()


# For debugging purposes
import src.sales_analysis.file_reader as file_reader
if __name__ == "__main__":

    # demonstrate that remove_duplicate_entries works
    df = file_reader.read_csv_full("dirty_cafe_sales.csv")
    print(df.info())
    df = remove_duplicate_entries(df)
    print(df.info())
# -------------------

def create_schema(data: pd.DataFrame, user_input=True):
	if not isinstance(data, pd.DataFrame):
		setup_logger(__name__, 'error', f"Error in validation::create_schema - variable: data <- was expecting pd.DataFrame, but got {type(data)}")
		raise TypeError(f"{type(data)} is not a Dataframe")
	column_schema = []
	
	if(user_input):
	# User will put in the data types for each column so they can be constrained to it
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
					setup_logger(__name__, 'warning', f"Warning in validation::create_schema. Input expected a string, int, float, bool, or datetime, but received an invalid datatype of {dtype}. Defaulting to string")
					column_schema.append({f"{col}": str})
	else:
		column_schema = [{"Transaction ID": str}, {"Item": str}, {"Quantity" : int}, {"Price Per Unit" : float}, 
				   {"Total Spent" : float}, {"Payment Method" : str}, {"Location" : str}, {"Transaction Date" : str}]

	return column_schema

def validate_add_record(data: pd.DataFrame, record: list, column_schema: list, invalid_cell=[]):
    #TODO: Accept a row of data in Series or list format, validate, then add if valid.
	if not isinstance(record, (list)):
		raise TypeErrer("record is not a list")		
	if not isinstance(column_schema, list):
		raise TypeError("Column schema not a list.")
	if not column_schema:
		raise("No column schema to compare record to for validation")
	
	if (len(column_schema != len(record))):
		setup_logger(__name__, 'info', "Record doesn't contain all fields for the column.")
		return pd.Series()
	column_names = []
	for column in column_schema:
		column_names.append(column.keys())
	
	for index, column in enumerate(column_schema):
		if not isinstance(record[index], column[column_names[index]]):
			raise TypeError("Record data types don't match the data")
		
	# If code reaches this point, add row to dataframe
	df.iloc[len(df)] = record


def validate_data(data: pd.DataFrame, column_schema: list, invalid_cell=[], dtype="", user_input=True):
		"""Marks invalid cells to -1 to mark for dropping in clead_data::remove_all_null method"""
  
		# replace any of these identifiers in the list with -1
		if not invalid_cell:
			invalid_cell = ["NaN", "EMPTY", "empty", "UNKNOWN", "unknown", "ERROR", "error", "NA", "Na", "None", "NULL", "null", np.nan]
		for target in invalid_cell:
			if data.empty:
				setup_logger(__name__, 'warning', f"Warning in validation::validate_data. No data to validate")
				return data
			else:
				data.replace(target, -1, inplace=True)
		
		# if we don't know the data types for the columns, raise an error
		if not column_schema:
			setup_logger(__name__, 'error', f"Error in validation::validate_data. No column structure to reference")
			raise ValueError("No column format/schema was given")
			
		
		for col_dict in column_schema:
			for col_label in col_dict:
				#data.fillna("NaN", inplace=True)
				#set the column of the data frame to the type in the schema
				data[col_label] = data[col_label].astype(col_dict.get(col_label))

		print(column_schema)
  
		# Constrained columns should have a value / NOT be -1
		constrained_cols = []
  
		# Keep looping until user hits q to quit loop. 
		# They can keep entering column names to append to constained_cols to validate them after this
		if(user_input):
			while(True):
				if dtype: column = dtype
				else: column = input("Enter a column to drop NULL/-1 values (Q to quit): ")
				if column.lower() == 'q':
					break
				if column and (column not in [str(col_schema.keys()) for col_schema in column_schema]):
					setup_logger(__name__, 'error', f"No column was found with that name: {column}")
					raise KeyError(f"No column with that name: {column}")
				if column:
					constrained_cols.append(column)
				dtype = ""
		else:
			for columns in data.columns:
				constrained_cols.append(columns)

		return data

#Obsolete
def change_col_dtype(data: pd.DataFrame | dict, column_str: str, type_change: type) -> pd.DataFrame:
    """Changes the data type of a column_str in the data frame"""

    # Raise exceptions if data types aren't supported
    if not isinstance(column_str, str):
        raise TypeError("Column must be a string")
    if not isinstance(data, (pd.DataFrame, dict)):
        raise TypeError("data must be a Pandas Data Frame or dictionary")
    if not isinstance(type_change, (type)):
        raise TypeError("Type to change to must be a python primitive type")
    
    data_frame = None
    if isinstance(data, dict):
        data_frame = pd.DataFrame(data)
        data_frame[column_str] = data_frame[column_str].astype(type_change)
        return data_frame
    else:
        data[column_str] = data[column_str].astype(type_change)
        return data

def convert_dtypes(data: pd.DataFrame):
	pass

#For testing:
if __name__ == "__main__":
	df = pd.read_csv("src/data/dirty_cafe_sales.csv")
	list_from_schema = create_schema(df, user_input=False)
	validated = validate_data(df, list_from_schema, user_input=False)
	print(validated)
	# df_tuple = remove_all_null(validated)
