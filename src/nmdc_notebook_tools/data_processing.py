# -*- coding: utf-8 -*-
import pandas as pd


def convert_to_df(data: list) -> pd.DataFrame:
    return pd.DataFrame(data)


def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(df1, df2, on="common_column", how="inner")


## Define a merging function to join results
# This function merges new results with the previous results that were used for the new API request. It uses two keys from each result to match on. `df1`
# is the data frame whose matching `key1` value is a STRING. `df2` is the other data frame whose matching `key2` has either a string OR list as a value.
# df1_explode_list and df2_explode_list are optional lists of columns in either dataframe that need to be exploded because they are lists (this is because
# drop_duplicates cant take list input in any column). Note that each if statement includes dropping duplicates after merging as the dataframes are being
# exploded which creates many duplicate rows after merging takes place.


def merge_df(
    df1, df2, key1: str, key2: str, df1_explode_list=None, df2_explode_list=None
):
    if df1_explode_list is not None:
        # Explode the lists in the df (necessary for drop duplicates)
        for list in df1_explode_list:
            df1 = df1.explode(list)
    if df2_explode_list is not None:
        # Explode the lists in the df (necessary for drop duplicates)
        for list in df2_explode_list:
            df2 = df2.explode(list)
    # Merge dataframes
    merged_df = pd.merge(df1, df2, left_on=key1, right_on=key2)
    # Drop any duplicated rows
    merged_df.drop_duplicates(keep="first", inplace=True)
    return merged_df
