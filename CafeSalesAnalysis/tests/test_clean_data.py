"""These unit tests will check the functionality of the clean_data.py class"""
from src.sales_analysis.clean_data import clean
import pandas as pd
import pytest

class test_clean_data:

    def __init__(self):
        
        self.clean_obj = clean()
    
    @pytest.mark.parametrize("data, index, expected_index",
        [
            ({"Name": ["Jack", "Jane", "Jake"]}, [0, 1, 2], pd.Index([0, 1, 2]))
        ])
    def test_set_index(self, data: dict | pd.DataFrame, index: list | pd.Series, expected_index: list | pd.Series):
        clean.set_index
        pass