"""This script will handle validation of data in a Data Frame"""
import pandas as pd
from .logger import setup_logger
import logging
from datetime import datetime


def validate_record(record: pd.Series) -> tuple:
	"""Validates a record. Record must be a Pandas Series object. Returns a tuple of (pd.Series, error_list)"""
 
	error_list = []
	casted_list = []
	
	if not isinstance(record, pd.Series):
		setup_logger(__name__, 'error', f"Error in validation::validate_record - variable: record <- was expecting pd.Series, but got {type(record)}")
		error_list.append(TypeError("Record type isn't a pandas Series object"))
		return (pd.Series(), error_list)
	print("Enter the data types for each field. options: string, int, float, bool, datetime", end="\n\n")
 
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
		except ValueError as e:
			setup_logger(__name__, 'error', f"Error in validation::validate_record. Input expected a string, int, float, bool, or datetime, but received an invalid datatype of {response}")
			error_list.append(TypeError(f"Error in validation::validate_record. Input expected a string, int, float, bool, or datetime, but received an invalid datatype of {response}"))
			casted_list.append(field)

   
	return (pd.Series(casted_list), error_list)
def validate_dataframe():
    #TODO: implement validation
		pass
  
# try:
# 	s = validate_record(pd.Series(["21.5", "True", "Mario"]))
# 	print(f"{s[0]} : {type(s[0][0])} : {type(s[0][1])} : {type(s[0][2])}")
# except TypeError as e:
# 	print(e)
 
 