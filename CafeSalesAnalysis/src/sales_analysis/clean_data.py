import pandas as pd
import datetime


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
        pd_index = pd.Index(data[column_index])
    else:
        pd_index = pd.Index(column_index)
        
    #if data is already a data frame, plug into set index function - else covert it to a data frame first
    if isinstance(data, pd.DataFrame):
        data.set_index(pd_index, inplace=True)
        return data
    
    if isinstance(data, dict):
        return pd.DataFrame(data, index=pd_index)
    
    
def change_data_type(data: pd.DataFrame | dict, column_str: str, type_change: type) -> pd.DataFrame:
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
    
def round_floats(data: pd.DataFrame, column_str: str, round_formatter: str) -> pd.DataFrame:
    """Rounds the floats in the specified column to the number of places in round_formatter"""

    if not isinstance(round_formatter, int):
        raise TypeError("formatter must be an int")
    if not isinstance(column_str, str):
        raise TypeError("Column must be a string")
    if not isinstance(data, pd.DataFrame):
        raise TypeError("data must be a data frame")

    for index, field in enumerate(data[column_str]):
        if not isinstance(field, float):
            continue
        else:
            data.loc[index, field] = round(data.loc[index, field]), round_formatter

