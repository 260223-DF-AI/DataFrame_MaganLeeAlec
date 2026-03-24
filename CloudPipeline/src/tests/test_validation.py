import pytest
import pandas as pd
import numpy as np
from src.sales_analysis.validation import OutputSchema, validate_chunk_dtypes
import pytest
import pandera.pandas as pa
from hypothesis import given, settings, HealthCheck

#pytests for validation.py
# settings decorator will suppress health check warnings if tests take too long and @given decorator throws an error
# Set max examples to 10 (default 100) to speed up testing
# @given will generate dummy test data for the assertions
@settings(suppress_health_check=[HealthCheck.too_slow], max_examples=10)
@given(dataframe=OutputSchema.strategy(size=5))
def test_schema_hypothesis(dataframe: pd.DataFrame):
    validate_chunk_dtypes(dataframe)

#============ tests for remove_duplicate_entries ===========
# def test_remove_duplicate_entries_removes_duplicates():
#     """Tests that duplicate rows are removed correctly."""
#     df = pd.DataFrame({
#         "Name": ["Alice", "Bob", "Alice"],
#         "Sales": [100, 200, 100]
#     })
#     actual = validation.remove_duplicate_entries(df)
#     expected = pd.DataFrame({
#         "Name": ["Alice", "Bob"],
#         "Sales": [100, 200]
#     }, index=[0, 1])
#     pd.testing.assert_frame_equal(actual, expected)

# def test_remove_duplicate_entries_no_duplicates():
#     """Tests that a dataframe with no duplicates stays the same."""
#     df = pd.DataFrame({
#         "Name": ["Alice", "Bob"],
#         "Sales": [100, 200]
#     })
#     actual = validation.remove_duplicate_entries(df)
#     pd.testing.assert_frame_equal(actual, df)

# def test_remove_duplicate_entries_empty_dataframe():
#     """Tests duplicate removal on an empty dataframe."""
#     df = pd.DataFrame()
#     actual = validation.remove_duplicate_entries(df)
#     expected = pd.DataFrame()
#     pd.testing.assert_frame_equal(actual, expected)


# def test_remove_duplicate_entries_all_duplicates():
#     """Tests when every row is duplicated."""
#     df = pd.DataFrame({
#         "Name": ["Alice", "Alice", "Alice"],
#         "Sales": [100, 100, 100]
#     })
#     actual = validation.remove_duplicate_entries(df)
#     expected = pd.DataFrame({
#         "Name": ["Alice"],
#         "Sales": [100]
#     }, index=[0])
#     pd.testing.assert_frame_equal(actual, expected)

# #============ tests for create_schema =====================
# @pytest.mark.parametrize("data", [[]])
# def test_create_schema(data: pd.DataFrame):
#   with pytest.raises(TypeError) as e:
#     validation.create_schema(data)
#     assert "is not a Dataframe" in str(e.value)

