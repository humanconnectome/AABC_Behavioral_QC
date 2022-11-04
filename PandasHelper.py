from IPython.core.display import display, HTML
from collections.abc import Iterable
from pandas import DataFrame, Series
import numpy

MIN_FLOAT = numpy.finfo(numpy.float64).min


def __in_common_helper__(data, column_names, column_names2):
    if isinstance(data, Series):
        return data.to_frame()
    elif not isinstance(data, DataFrame):
        raise TypeError("Expecting either a Series or DataFrame")

    column_names = column_names or column_names2

    if column_names:

        # if only a single column is provided, convert to one item set
        # else convert iterable into set
        column_names = (
            {column_names}
            if isinstance(column_names, str) or not isinstance(column_names, Iterable)
            else set(column_names)
        )

        return data[column_names].copy()
    else:
        return data.copy()


def in_common(left, right, on=None, left_columns=None, right_columns=None):
    left_df = __in_common_helper__(left, left_columns, on)
    right_df = __in_common_helper__(right, right_columns, on)

    common_columns = left_df.columns.intersection(right_df.columns)
    left_df = left_df[common_columns].fillna(MIN_FLOAT)
    right_df = right_df[common_columns].fillna(MIN_FLOAT)

    return left_df, right_df


def unequal_columns(left, right, on=None, left_columns=None, right_columns=None):
    left_df = __in_common_helper__(left, left_columns, on)
    right_df = __in_common_helper__(right, right_columns, on)
    n = min(len(left_df.columns), len(right_df.columns))
    left_df = left_df.iloc[:, :n].fillna(MIN_FLOAT)
    right_df = right_df.iloc[:, :n].fillna(MIN_FLOAT)

    return left_df, right_df


def difference(left, right, on=None, left_columns=None, right_columns=None, equal_names=True):
    """
    difference Finds the set difference from left to right DataFrames and returns either a DataFrame or a boolean Series

    Args:
        left (DataFrame|Series): Left dataset
        right (DataFrame|Series): Right dataset
        on (str|int|Iterable): Column(s) from both datasets to use for operation
        left_columns (str|int|Iterable): Column(s) from left dataset to use for operation
        right_columns (str|int|Iterable): Column(s) from right dataset to use for operation
        equal_names (bool): Should it automatically try to match intersecting columns, or take the columns in order given as is.

    Returns:
        Series: A boolean of the left dataset indicating also in right dataset
        DataFrame: The rows resulting from the operation

    """

    if equal_names:
        left_data, right_data = in_common(left, right, on, left_columns, right_columns)
    else:
        left_data, right_data = unequal_columns(left, right, on, left_columns, right_columns)

    a = set(map(tuple, left_data.values))
    b = set(map(tuple, right_data.values))
    differences = a - b

    bool_series = left_data.apply(tuple, axis=1).isin(differences)
    return left[bool_series] if isinstance(left, DataFrame) else bool_series


def intersection(left, right, on=None, left_columns=None, right_columns=None):
    """
    intersection Finds the set intersection from left to right DataFrames and returns either a DataFrame or a boolean Series

    Args:
        left (DataFrame|Series): Left dataset
        right (DataFrame|Series): Right dataset
        on (str|int|Iterable): Column(s) from both datasets to use for operation
        left_columns (str|int|Iterable): Column(s) from left dataset to use for operation
        right_columns (str|int|Iterable): Column(s) from right dataset to use for operation

    Returns:
        Series: A boolean of the left dataset indicating also in right dataset
        DataFrame: The rows resulting from the operation

    """
    left_data, right_data = in_common(left, right, on, left_columns, right_columns)

    a = set(map(tuple, left_data.values))
    b = set(map(tuple, right_data.values))
    bool_series = left_data.apply(tuple, axis=1).isin(a.intersection(b))
    return left[bool_series] if isinstance(left, DataFrame) else bool_series


def intersection_both(
    left,
    right,
    on=None,
    left_columns=None,
    right_columns=None,
    sources=None,
    sources_column="sources",
    drop_duplicates=True,
):
    if not isinstance(left, DataFrame) or not isinstance(right, DataFrame):
        raise TypeError("Expecting two dataframes.")

    left_data, right_data = in_common(left, right, on, left_columns, right_columns)

    a = set(map(tuple, left_data.values))
    b = set(map(tuple, right_data.values))
    intersection = a.intersection(b)
    left_df = left[left_data.apply(tuple, axis=1).isin(intersection)].copy()
    right_df = right[right_data.apply(tuple, axis=1).isin(intersection)].copy()

    if sources:
        if sources is True:
            sources = ["left", "right"]
        left_df[sources_column] = sources[0]
        right_df[sources_column] = sources[1]

        neg = left_df.columns[left_df.columns != sources_column]
    else:
        neg = left_df.columns

    result = left_df.append(right_df, sort=False).sort_values(left_data.columns.to_list())

    return result.drop_duplicates(neg, keep=False) if drop_duplicates else result


def showbox(message, heading=None, type="success"):
    box = '<div class="alert alert-%s" role="alert">' % type
    if heading:
        box += '<h4 class="alert-heading">%s</h4>' % heading
    box += "<p>%s</p></div>" % message
    return display(HTML(box))


def showdataframe(df):
    if not df.empty:
        print("Size: r%s x c%s" % df.shape)
        display(df[:3])


def asInt(dfs, *columns):
    if isinstance(dfs, DataFrame):
        dfs = [dfs]
    for df in dfs:
        for column in columns:
            df[column] = df[column].astype(float).astype("Int64")
