import pandas as pd
import datetime
from .logger import setup_logger
import numpy as np
pd.set_option('display.float_format', '{:.2f}'.format)

def change_index(data: pd.DataFrame | dict, column_index: pd.Series | list | pd.Index) -> pd.DataFrame:
    """Returns data frame with updated index of column_str that uniquely identifies
    each record.""" 

    # Raise exceptions if data types are not supported
    if not isinstance(data, (pd.DataFrame, dict)):
        raise TypeError("data must be a Data Frame or dictionary")
    if not isinstance(column_index, (pd.Series, list, pd.Index, str)):
        raise TypeError("index must be a Pandas Series object or list")
    
    # if index is a string, turn it to series then into pandas index. else just turn it directly to pandas index
    if type(column_index) == str:
        pd_index = pd.Index(data[column_index],dtype=str)
    else:
        pd_index = pd.Index(column_index)
        
    #if data is already a data frame, plug into set index function - else covert it to a data frame first
    if isinstance(data, pd.DataFrame):
        data.set_index(pd_index, inplace=True)
        return data
    
    if isinstance(data, dict):
        return pd.DataFrame(data, index=pd_index)
    
def remove_all_null(data: pd.DataFrame) -> tuple:
    """Remove all null values (should be converted to -1 with validate method first) 
    returns tuple of (clean_data, dirty_data)"""
    #invalid_cell = ["NaN", "EMPTY", "empty", "UNKNOWN", "unknown", "ERROR", "error", "NA", "Na", "None", "NULL", "null", np.nan]
    null_value = -1
    for col in data.columns:
        if col:
            filter_keep = data[str(col)] > type(data.at[0, col])(null_value)
            removed_records = data[data[col] ==  type(data.at[0, col])(null_value)]
            setup_logger(__name__, 'warning', f"Records: \n{removed_records} was removed")
            data = data[filter_keep]

    clean_dirty_tuple = (data, removed_records)
    return clean_dirty_tuple

def remove_duplicate_entries(data: pd.DataFrame) -> pd.DataFrame:
    """Returns dataframe with duplicate entries removed."""
    return data.drop_duplicates(inplace=True)

def replace_values(data: pd.DataFrame, target, replacement):
    """Replaces values in Data Frame in place"""
    data.replace(target, replacement, inplace=True)
    
def drop_na_by_column(data: pd.DataFrame, column: str):
    
    if not isinstance(data, pd.DataFrame):
        raise TypeError("Not a Data Frame")
    if not isinstance(column, str):
        raise TypeError("column must be a string")
    data = data.dropna(subset=column)
    
    #Obsolete for now, pandas display settings have a value to change format
# def round_floats(data: pd.DataFrame, column_str: str, round_formatter: str) -> pd.DataFrame:
#     """Rounds the floats in the specified column to the number of places in round_formatter"""

#     if not isinstance(round_formatter, int):
#         raise TypeError("formatter must be an int")
#     if not isinstance(column_str, str):
#         raise TypeError("Column must be a string")
#     if not isinstance(data, pd.DataFrame):
#         raise TypeError("data must be a data frame")

#     for field in (data[column_str]):
#         if not isinstance(field, float):
#             continue
#         else:
#             data[column_str] = data[column_str].round(round_formatter)
#     return data
df = pd.DataFrame({
    "Sales": [100, -1, 200, 300],
    "Profit": [10, -1, 20, 30]
})
for col in df.columns:
    print(df.at[1, col])