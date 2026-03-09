import pandas as pd


def change_index(data: pd.DataFrame | dict, column_index: pd.Series | list | pd.Index) -> pd.DataFrame:
    """Returns data frame with updated index of column that uniquely identifies
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
    
    

 

    