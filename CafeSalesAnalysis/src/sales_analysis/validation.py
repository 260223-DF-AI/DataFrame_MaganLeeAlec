"""This script will handle validation of data in a Data Frame"""
import pandas as pd
from .logger import setup_logger
import logging
from datetime import datetime

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
		
	

def validate_all_records(data):
	
		pass

 