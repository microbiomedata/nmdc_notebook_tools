import pandas as pd


def convert_to_df(data: list) -> pd.DataFrame:
    return pd.DataFrame(data)


def split_list(data: list, chunk_size: int) -> list:
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(df1, df2, on="common_column", how="inner")
