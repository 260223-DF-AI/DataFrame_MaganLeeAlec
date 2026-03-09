import pandas as pd


def set_index(data: pd.DataFrame, column_index: pd.Series) -> pd.Index:
    """Sets index of data frame to column that uniquely identifies
    each record""" 

    if not isinstance(data, (pd.DataFrame)):
        raise TypeError("data isn't a Data Frame")
    
    if not isinstance(column_index, (pd.Series)):
        raise TypeError("index must be a Pandas Series object")
    
    data.set_index(column_index)
    return data.index

def set_index(data: dict, column_index: list) -> pd.Index:
    """Sets index of data frame to column that uniquely identifies
    each record""" 

    if not isinstance(data, (dict)):
        raise TypeError("data isn't a dictionary")
    
    if not isinstance(column_index, (list)):
        raise TypeError("index must be a list")
    
    return pd.DataFrame(data, index=column_index).index
 

    