import pandas as pd
import datetime
from .logger import setup_logger
import numpy as np
from collections import namedtuple

DataTuple = namedtuple('DataTuple', ['valid', 'invalid'])

pd.set_option('display.float_format', '{:.2f}'.format)

def clean_sales_data(data_chunk: pd.DataFrame) -> DataTuple:
    """Clean columns of the dataframe by converting values to an appropriate datatype, storing invalid records in their own
    data frame, then returning a clean version of the data frame chunk with the invalid chunk"""
    
    #---Clean Dates---
    data_chunk['Date'] = pd.to_datetime(data_chunk['Date'], format='mixed', dayfirst=False, errors='coerce')
    data_chunk['Date'] = data_chunk['Date'].fillna(pd.to_datetime('03-02-2025'))
    #data_chunk['Date'] = data_chunk['Date'].dt.strftime('%m/%d/%Y')
     
    #---Clean Quantity---
    # Data has 'Ten' to represent the integer 10 - replace with int by using the map function
    # map takes a dict with values to replace, dict used only defined 'Ten', so non-matching values will be null
    # add values back by using .fillna at the end with the original data in Quantity column
    data_chunk['Quantity'] = data_chunk['Quantity'].map({'Ten': 10}).fillna(data_chunk['Quantity'])
    
    # Column will be an object because of the invalid data, convert after cleaning to make the column an int
    data_chunk['Quantity'] = data_chunk['Quantity'].astype(int)
    df_null_quantity = data_chunk[data_chunk['Quantity'].isna().copy()]
    data_chunk.dropna(subset='Quantity', inplace=True)
    
    #---Clean Unit Price---
    df_null_unit_price = data_chunk[data_chunk['UnitPrice'].isna().copy()]
    data_chunk.dropna(subset='UnitPrice', inplace=True)
    
    #---Clean Total Amount---
    df_null_total_amount = data_chunk[data_chunk['TotalAmount'].isna().copy()]
    data_chunk.dropna(subset='TotalAmount', inplace=True)
    
    invalid_data_chunk = pd.concat([df_null_quantity, df_null_unit_price, df_null_total_amount])
    return DataTuple(data_chunk, invalid_data_chunk)
