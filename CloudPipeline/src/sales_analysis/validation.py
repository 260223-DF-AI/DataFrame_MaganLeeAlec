"""This script will handle validation of data in a Data Frame"""
import pandas as pd
import numpy as np
import pandera as pa
import pyarrow as pw
import pyarrow.parquet as pq
from .clean_data import clean_sales_data, DataTuple
from .logger import setup_logger
from pandera.typing import DataFrame, Series
from datetime import date
import glob

# Class based validation schema using Pandera
# OutputSchema class is the blueprint for the dataframe's schema
class OutputSchema(pa.DataFrameModel):
  """Schema for sales dataset"""
  TransactionID: Series[int] = pa.Field(nullable=False)
  Date: Series[pd.Timestamp] = pa.Field(coerce=True, nullable=True)
  StoreID: Series[str]	
  StoreLocation: Series[str]
  Region: Series[str]
  State: Series[str]	
  CustomerID: Series[str]	
  CustomerName: Series[str]
  Segment: Series[str]	
  ProductID: Series[str]
  ProductName: Series[str]	
  Category: Series[str]
  SubCategory: Series[str]
  Quantity: Series[pa.Int64] = pa.Field(coerce=True, ge=1)
  UnitPrice: Series[float]
  DiscountPercent: Series[float]
  TaxAmount: Series[float]
  ShippingCost: Series[float]
  TotalAmount: Series[float]

  class Config:
    strict = True
    coerce = True

# Pass in a pandas DataFrame as parameter, and the check_types decorator will send output if there are any invalid records in the dataframe
# Invalid data shown in output should be cleaned from dataframe before it is upload to GCS
# lazy=True will make sure that the errors collected from the invalid records will not stop the program at the first error, instead collects all of them
@pa.check_types(lazy=True)
def validate_chunk_dtypes(data_chunk: DataFrame[OutputSchema]) -> DataFrame[OutputSchema]:
  return data_chunk
  
  
if __name__ == "__main__":
   
	files = glob.glob('src/data/dummy*.csv')
	for file in files:
		for chunk in pd.read_csv(file, chunksize=2500):
			data_tuple = clean_sales_data(chunk)
			print(data_tuple.invalid)
			validate_chunk_dtypes(chunk)
			break
