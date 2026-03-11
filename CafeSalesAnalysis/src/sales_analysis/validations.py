# In here for now but can be moved - lee
import pandas as pd

def remove_duplicate_entries(data: pd.DataFrame) -> pd.DataFrame:
    """Returns dataframe with duplicate entries removed."""
    return data.drop_duplicates()