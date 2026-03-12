import pytest
import pandas as pd
import numpy as np
from src.sales_analysis import validation

#pytests for validation.py
#============ tests for remove_duplicate_entries ===========
def test_remove_duplicate_entries_removes_duplicates():
    """Tests that duplicate rows are removed correctly."""
    df = pd.DataFrame({
        "Name": ["Alice", "Bob", "Alice"],
        "Sales": [100, 200, 100]
    })
    actual = validation.remove_duplicate_entries(df)
    expected = pd.DataFrame({
        "Name": ["Alice", "Bob"],
        "Sales": [100, 200]
    }, index=[0, 1])
    pd.testing.assert_frame_equal(actual, expected)

def test_remove_duplicate_entries_no_duplicates():
    """Tests that a dataframe with no duplicates stays the same."""
    df = pd.DataFrame({
        "Name": ["Alice", "Bob"],
        "Sales": [100, 200]
    })
    actual = validation.remove_duplicate_entries(df)
    pd.testing.assert_frame_equal(actual, df)

#============ tests for create_schema =====================
@pytest.mark.parametrize("data", [[]])
def test_create_schema(data: pd.DataFrame):
  with pytest.raises(TypeError) as e:
    validation.create_schema(data)
    assert "is not a Dataframe" in str(e.value)

def test_create_schema_with_valid_inputs(monkeypatch):
    """Tests schema creation using user input for each column."""
    df = pd.DataFrame({
        "Name": ["Alice", "Bob"],
        "Sales": [100, 200],
        "Active": [True, False]
    })
    user_inputs = iter(["string", "float", "bool"])
    monkeypatch.setattr("builtins.input", lambda _: next(user_inputs))
    actual = validation.create_schema(df)
    expected = [
        {"Name": str},
        {"Sales": float},
        {"Active": bool}
    ]
    assert actual == expected

def test_create_schema_defaults_invalid_input_to_string(monkeypatch):
    """Tests that invalid type input defaults to string."""
    df = pd.DataFrame({
        "Name": ["Alice", "Bob"]
    })
    user_inputs = iter(["banana"])
    monkeypatch.setattr("builtins.input", lambda _: next(user_inputs))
    actual = validation.create_schema(df)
    expected = [{"Name": str}]
    assert actual == expected

#============ tests for validate_add_record ===============
@pytest.mark.skip(reason="validate_add_record is not implemented yet")
def test_validate_add_record():
    pass

#============ tests for validate_data =====================
@pytest.mark.parametrize("data, column_schema, invalid_cell, dtype", [(pd.DataFrame({"Money": ["21.23", "100.57", "30.00"]}), [],  [], ""),
                                                                        (pd.DataFrame({"Money": ["21.23", "100.57", "30.00"]}), [{'Money': str}],  [], "sauce")])
def test_validate_data(data: pd.DataFrame, column_schema: list, invalid_cell, dtype):
    with pytest.raises((ValueError, TypeError, KeyError)) as exc_info:
        validation.validate_data(data, column_schema, invalid_cell, dtype)
    assert "No column" in str(exc_info.value) or "no format/schema" in str(exc_info.value)

def test_validate_data_raises_value_error_when_schema_missing():
    """Tests that missing schema raises ValueError."""
    df = pd.DataFrame({
        "Money": ["21.23", "100.57", "30.00"]
    })
    with pytest.raises(ValueError) as exc_info:
        validation.validate_data(df, [], [], "")
    assert "no format/schema" in str(exc_info.value).lower()

def test_validate_data_returns_empty_dataframe_when_data_empty():
    """Tests that empty dataframe is returned immediately."""
    df = pd.DataFrame()
    actual = validation.validate_data(df, [{"Money": float}], [], "")
    pd.testing.assert_frame_equal(actual, df)

def test_validate_data_converts_column_types_when_dtype_is_q():
    """
    Tests successful type conversion by forcing the loop to quit immediately.
    Using dtype='q' avoids the input loop and returns the converted dataframe.
    """
    df = pd.DataFrame({
        "Money": ["21.23", "100.57", "30.00"]
    })
    actual = validation.validate_data(df, [{"Money": float}], [], "q")
    expected = pd.DataFrame({
        "Money": [21.23, 100.57, 30.00]
    })
    pd.testing.assert_frame_equal(actual, expected)

def test_validate_data_replaces_invalid_values_before_conversion():
    """Tests that known invalid cell values are replaced before casting."""
    df = pd.DataFrame({
        "Money": ["21.23", "NaN", "30.00"]
    })
    actual = validation.validate_data(df, [{"Money": float}], [], "q")
    expected = pd.DataFrame({
        "Money": [21.23, -1.0, 30.00]
    })
    pd.testing.assert_frame_equal(actual, expected)

def test_validate_data_raises_key_error_for_unknown_column_request():
    """
    Tests that requesting an invalid constrained column raises KeyError.
    This matches the current implementation behavior.
    """
    df = pd.DataFrame({
        "Money": ["21.23", "100.57", "30.00"]
    })
    with pytest.raises(KeyError) as exc_info:
        validation.validate_data(df, [{"Money": float}], [], "sauce")
    assert "No column" in str(exc_info.value)

#============ tests for change_col_dtype ==================
@pytest.mark.parametrize(
        "data, column_str, data_type, expected_column, expected_assert", [({"Money": ["21.23", "100.57", "30.00"]}, "Money", float, {"Money": [21.23, 100.57, 30.00]}, True),
        ({"Money": ["21.23", "NaN", "30.00"]}, "Money", float, {"Money": [21.23, float("NaN"), 30.00]}, False)]
)
def test_change_col_dtype(data, column_str, data_type, expected_column, expected_assert):
    """Tests if a column in a dataframe will change to the declared data type"""
    #with pytest.raises(TypeError):
    actual = validation.change_col_dtype(data, column_str, data_type)

    # asserting with pandas will raise an assert error instead of checking if it matches the expected assert
    #pd.testing.assert_series_equal( actual[column_str], pd.DataFrame(expected_column)[column_str]) is expected_assert
    assert actual[column_str].equals(pd.DataFrame(expected_column)[column_str])
    
def test_change_col_dtype_invalid_column_name_type():
    """Tests that a non-string column name raises TypeError."""
    df = pd.DataFrame({"Money": ["21.23", "100.57"]})
    with pytest.raises(TypeError) as exc_info:
        validation.change_col_dtype(df, 123, float)
    assert "Column must be a string" in str(exc_info.value)

def test_change_col_dtype_invalid_data_type_input():
    """Tests that invalid data container raises TypeError."""
    with pytest.raises(TypeError) as exc_info:
        validation.change_col_dtype(123, "Money", float)
    assert "data must be a Pandas Data Frame or dictionary" in str(exc_info.value)

def change_col_dtype_invalid_type_change():
    """Tests that invalid type_change raises TypeError."""
    df = pd.DataFrame({"Money": ["21.23", "100.57"]})
    with pytest.raises(TypeError) as exc_info:
        validation.change_col_dtype(df, "Money", "float")
    assert "Type to change to must be a python primitive type" in str(exc_info.value)

def test_change_col_dtype_bad_value_raises_error():
    """Tests that invalid conversion values raise an exception."""
    df = {"Money": ["21.23", "hello", "30.00"]}
    with pytest.raises(ValueError):
        validation.change_col_dtype(df, "Money", float)
