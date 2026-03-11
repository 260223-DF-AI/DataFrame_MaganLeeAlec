"""These unit tests will check the functionality of the clean_data.py class"""
from src.sales_analysis import clean_data
import pandas as pd
import pytest


@pytest.mark.parametrize("data, index, expected_index",
    [
        ({"Name": ["Jack", "Jane", "Jake"]}, [0, 1, 2], pd.Index([0, 1, 2])),
        ({"Name": ["Meg", "Marge", "Mike"],
          "Email": ["meg123@email.net", "marge764@wahoo.com", "mikeaw@usa.gov"]}, ["meg123@email.net", "marge764@wahoo.com", "mikeaw@usa.gov"], pd.Index(["meg123@email.net", "marge764@wahoo.com", "mikeaw@usa.gov"])),
        ({"Name": ["Meg", "Marge", "Mike"],
          "Email": ["meg123@email.net", "marge764@wahoo.com", "mikeaw@usa.gov"]}, "Email", pd.Index(["meg123@email.net", "marge764@wahoo.com", "mikeaw@usa.gov"]))
    ])
def test_change_index(data: dict | pd.DataFrame, index: list | pd.Series | pd.Index, expected_index: list | pd.Series | pd.Index):
    """Tests if the index being set will change accurately"""
    #with pytest.raises(TypeError):
    actual = clean_data.change_index(data, index)
    
    # Cannot use == here for Pandas Index object type.
    # Use Pandas assert on index for accurate assertions instead of pytest. includes data type comparisons and order check
    # assert actual.index.equals(expected_index)
    pd.testing.assert_index_equal(actual.index, expected_index, check_order=False)

    
#def test_round_floats(data: pd.DataFrame, column_str: str, round_formatter: str):
    #pass
    
    
    