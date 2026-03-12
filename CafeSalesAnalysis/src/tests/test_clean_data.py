"""These unit tests will check the functionality of the clean_data.py class"""
from src.sales_analysis import clean_data
import pandas as pd
import pytest

#pytests for clean_data.py

#============== tests for change_index ===============
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

def test_change_index_with_dataframe():
    """Tests change_index when a DataFrame is passed in"""
    df = pd.DataFrame({
        "Name": ["Alice", "Bob"],
        "Email": ["alice@email.com", "bob@email.com"]
    })

    actual = clean_data.change_index(df, "Email")

    expected_index = pd.Index(["alice@email.com", "bob@email.com"], dtype=str, name="Email")

    pd.testing.assert_index_equal(actual.index, expected_index)

def test_change_index_invalid_data_type():
    """Tests that invalid data input raises TypeError"""
    with pytest.raises(TypeError):
        clean_data.change_index(123, [0, 1, 2])

def test_change_index_invalid_index_type():
    """Tests that invalid index input raises TypeError"""
    df = pd.DataFrame({"Name": ["Alice", "Bob"]})
    with pytest.raises(TypeError):
        clean_data.change_index(df, 123)

#============== tests for remove_all_null ============
def test_remove_all_null_removes_negative_rows():
    """
    Based on current implementation, rows with values <= -1 are removed.
    This test uses numeric data because the function compares values to -1.
    """
    df = pd.DataFrame({
        "Sales": [100, -1, 200, 300],
        "Profit": [10, -1, 20, 30]
    })
    actual = clean_data.remove_all_null(df)
    expected = pd.DataFrame({
        "Sales": [100, 200, 300],
        "Profit": [10, 20, 30]
    }, index=[0, 2, 3])
    pd.testing.assert_frame_equal(actual, expected)

def test_remove_all_null_keeps_valid_rows():
    """Tests that rows are unchanged when all numeric values are valid"""
    df = pd.DataFrame({
        "Sales": [100, 200, 300],
        "Profit": [10, 20, 30]
    })

    actual = clean_data.remove_all_null(df)

    pd.testing.assert_frame_equal(actual, df)

#============== tests for remove_duplicate_entries ===
def test_remove_duplicate_entries_current_behavior_returns_none():
    """
    Current function uses inplace=True, so it returns None.
    This test matches the current implementation.
    """
    df = pd.DataFrame({
        "Name": ["Alice", "Bob", "Alice"],
        "Sales": [100, 200, 100]
    })
    actual = clean_data.remove_duplicate_entries(df)
    assert actual is None
  
def test_remove_duplicate_entries_modifies_dataframe_in_place():
    """Tests that duplicate rows are removed from the original dataframe"""
    df = pd.DataFrame({
        "Name": ["Alice", "Bob", "Alice"],
        "Sales": [100, 200, 100]
    })
    clean_data.remove_duplicate_entries(df)
    expected = pd.DataFrame({
        "Name": ["Alice", "Bob"],
        "Sales": [100, 200]
    }, index=[0, 1])
    pd.testing.assert_frame_equal(df, expected)
  
#============== tests for replace_values =============
def test_replace_values_replaces_target_in_place():
    """Tests that target values are replaced inside the DataFrame"""
    df = pd.DataFrame({
        "Status": ["ERROR", "OK", "ERROR"],
        "Amount": [10, 20, 30]
    })
    clean_data.replace_values(df, "ERROR", "FIXED")
    expected = pd.DataFrame({
        "Status": ["FIXED", "OK", "FIXED"],
        "Amount": [10, 20, 30]
    })
    pd.testing.assert_frame_equal(df, expected)
  
def test_replace_values_returns_none():
    """Current implementation does not return a DataFrame"""
    df = pd.DataFrame({
        "Status": ["ERROR", "OK"]
    })
    actual = clean_data.replace_values(df, "ERROR", "FIXED")
    assert actual is None

#============== tests for drop_na_by_column ==========

def test_drop_na_by_column_invalid_data_type():
    """Tests that non-DataFrame input raises TypeError"""
    with pytest.raises(TypeError):
        clean_data.drop_na_by_column(["not", "a", "df"], "Name")


def test_drop_na_by_column_invalid_column_type():
    """Tests that non-string column input raises TypeError"""
    df = pd.DataFrame({"Name": ["Alice", None, "Bob"]})
    with pytest.raises(TypeError):
        clean_data.drop_na_by_column(df, 123)


def test_drop_na_by_column_current_behavior_returns_none():
    """
    Current function assigns dropped df to a local variable
    but does not return it, so result is None.
    """
    df = pd.DataFrame({
        "Name": ["Alice", None, "Bob"],
        "Sales": [100, 200, 300]
    })
    actual = clean_data.drop_na_by_column(df, "Name")
    assert actual is None

#def test_round_floats(data: pd.DataFrame, column_str: str, round_formatter: str):
    #pass
    
    
    