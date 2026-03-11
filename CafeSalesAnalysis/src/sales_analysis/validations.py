# In here for now but can be moved - lee
import pandas as pd


def remove_duplicate_entries(data: pd.DataFrame) -> pd.DataFrame:
    """Returns dataframe with duplicate entries removed."""
    return data.drop_duplicates()


# For debugging purposes
import src.sales_analysis.file_reader as file_reader
if __name__ == "__main__":

    # demonstrate that remove_duplicate_entries works
    df = file_reader.read_csv_full("dirty_cafe_sales.csv")
    print(df.info())
    df = remove_duplicate_entries(df)
    print(df.info())