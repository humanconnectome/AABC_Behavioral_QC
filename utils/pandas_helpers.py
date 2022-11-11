import pandas as pd


def fcol(df: pd.DataFrame, *column_name_snippets: str) -> pd.Index:
    """Return the column names that contain the snippets

    Examples:
        fcol(df, 'a', 'b') returns the columns that contain both 'a' and 'b'


    Args:
        df: A dataframe
        *column_name_snippets: A list of strings to search for in the column names

    Returns:
        A list of column names that contain the snippets
    """
    boolean_filter = False
    for col in column_name_snippets:
        boolean_filter |= df.columns.str.contains(col, case=False)
    return df.columns[boolean_filter]
