# -*- coding: utf-8 -*-
import pandas as pd


def convert_to_df(data: list) -> pd.DataFrame:
    """
    Convert a list of dictionaries to a pandas dataframe.
    params:
        data: list
            A list of dictionaries.
    """
    return pd.DataFrame(data)


def rename_columns(df: pd.DataFrame, new_col_names: list) -> pd.DataFrame:
    """
    Rename columns in a pandas dataframe.
    params:
        df: pd.DataFrame
            The pandas dataframe to rename columns.
        new_col_names: list
            A list of new column names. Names MUST be in order of the columns in the dataframe.\n
            Example:
                If the current column names are - ['old_col1', 'old_col2', 'old_col3']
                You will need to pass in the new names like - ['new_col1', 'new_col2', 'new_col3']
    """
    df.columns = new_col_names
    return df


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
