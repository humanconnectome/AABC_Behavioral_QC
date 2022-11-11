from typing import List

import pandas as pd


def ffill_empty_strings(df: pd.DataFrame, *columns: List[str]) -> pd.DataFrame:
    """Forward fill columns in a dataframe that are empty strings

    Args:
        df: dataframe to forward fill
        columns: columns to forward fill

    Returns:
        A dataframe with the specified columns forward filled
    """
    df = df.copy()
    for column in columns:
        original_values = df[column]

        # forward fill requires NaNs, not empty strings, to know where to fill
        empty_strings_to_nan = original_values.mask(original_values == "")

        # do it
        forward_filled = empty_strings_to_nan.ffill()

        # save results
        df[column] = forward_filled

    return df
