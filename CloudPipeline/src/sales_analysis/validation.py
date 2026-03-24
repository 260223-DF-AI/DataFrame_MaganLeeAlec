"""This script will handle validation of data in a Data Frame"""
import pandas as pd
import numpy as np
import pandera.pandas as pa
import pyarrow as pw
import pyarrow.parquet as pq
from .clean_data import clean_sales_data, DataTuple
import src.sales_analysis.logger as logger, src.sales_analysis.exceptions as ex
from pandera.typing import DataFrame, Series
from datetime import date
import glob
import os

logger = logger.setup_logger(__name__, "info")
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

# Pass in a pandas DataFrame as parameter, decorator will raise exceptions if invalid data
# lazy=True collects all errors / not stopping at first error.
# ***Wrap this function in try except blocks with pa.errors.SchemaError as the exception, then log both cases***
@pa.check_types(lazy=True)
def validate_chunk_dtypes(data_chunk: DataFrame[OutputSchema]) -> DataFrame[OutputSchema]:
  return data_chunk
  
# Each chunk should be cleaned, validated, converted to parquet, and then sent to GCP
if __name__ == "__main__":
	total_rows = 0
	files = glob.glob('src/data/dummy*.csv')
	for file in files:
		for chunk in pd.read_csv(file, chunksize=2500):
			data_tuple = clean_sales_data(chunk)
			#print(data_tuple.invalid)
			validate_chunk_dtypes(chunk)
			total_rows += len(data_tuple.valid) + len(data_tuple.invalid)
			break
		break
	


	# total rows should be 1,250,000
	print (total_rows)