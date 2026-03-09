import pandas as pd


def set_index(data: pd.DataFrame | dict, column_index: pd.Series | list | pd.Index) -> pd.DataFrame:
    """Returns data frame with updated index of column that uniquely identifies
    each record.""" 

    if not isinstance(data, (pd.DataFrame, dict)):
        raise TypeError("data must be a Data Frame or dictionary")
    
    if not isinstance(column_index, (pd.Series, list, pd.Index)):
        raise TypeError("index must be a Pandas Series object or list")
    
    pd_index = pd.Index(column_index)
    if isinstance(data, pd.DataFrame):
        data.set_index(pd_index, inplace=True)
        return data
    
    if isinstance(data, dict):
        return pd.DataFrame(data, index=pd_index)
    
    

 

    