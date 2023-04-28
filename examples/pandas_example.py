import re
import typing as T

import pandas as pd

from dpipes.processor import ColumnPipeProcessor, PipeProcessor

data = pd.read_excel("examples/online_retail_II.xlsx", nrows=1000)
data.to_csv("examples/sample.csv", index=False)


def camel_to_snake(x: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z][a-z])", "_", re.sub(r"\s+", "_", x))


def clean_colnames(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(lambda x: camel_to_snake(x).lower(), axis=1)


def add_line_total(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(line_item_total=lambda x: x["quantity"] * x["price"])


def add_order_total(df: pd.DataFrame) -> pd.DataFrame:
    order_total = (
        df.groupby("invoice")["line_item_total"].sum().reset_index(name="order_total")
    )
    return df.merge(order_total, how="left", on="invoice")


def add_total_order_size(df: pd.DataFrame) -> pd.DataFrame:
    order_size = df.groupby("invoice").size().reset_index(name="order_size")
    return df.merge(order_size, how="left", on="invoice")


############################################################################
# Basic Example
############################################################################

# Method chaining
result_a = (
    data.pipe(clean_colnames)
    .pipe(add_line_total)
    .pipe(add_order_total)
    .pipe(add_total_order_size)
)

# PipeProcessor
ps = PipeProcessor(
    [clean_colnames, add_line_total, add_order_total, add_total_order_size]
)
result_b = ps(data)

pd.testing.assert_frame_equal(result_a, result_b)


############################################################################
# Process multiple, similar datasets
############################################################################

split_1, split_2, split_3 = (
    data.iloc[:300, :],
    data.iloc[300:600, :],
    data.iloc[600:, :],
)

# Method chaining
for ds in [split_1, split_2, split_3]:
    result_a = (
        ds.pipe(clean_colnames)
        .pipe(add_line_total)
        .pipe(add_order_total)
        .pipe(add_total_order_size)
    )


# PipeProcessor
for ds in [split_1, split_2, split_3]:
    result_b = ps(ds)

pd.testing.assert_frame_equal(result_a, result_b)


############################################################################
# Process dataframe and individual columns
############################################################################


def float_to_int(
    df: pd.DataFrame, cols: T.Union[str, T.Sequence[str]], fillna: int = -99999
) -> pd.DataFrame:
    df[cols] = df[cols].fillna(fillna).astype(int)
    return df


def int_to_string(
    df: pd.DataFrame, cols: T.Union[str, T.Sequence[str]]
) -> pd.DataFrame:
    df[cols] = df[cols].astype(str)
    return df


# Method chaining
result_a = (
    data.pipe(clean_colnames)
    .pipe(add_line_total)
    .pipe(add_order_total)
    .pipe(add_total_order_size)
    .pipe(lambda x: float_to_int(x, "customer_id"))
    .pipe(lambda x: int_to_string(x, "customer_id"))
)

# PipeProcessor
ps = PipeProcessor(
    [clean_colnames, add_line_total, add_order_total, add_total_order_size]
)
col_ps = PipeProcessor([float_to_int, int_to_string], {"cols": "customer_id"})
pipeline = PipeProcessor([ps, col_ps])

result_b = pipeline(data)

pd.testing.assert_frame_equal(result_a, result_b)

# ColumnPiperProcessor
ps = PipeProcessor(
    [clean_colnames, add_line_total, add_order_total, add_total_order_size]
)
col_ps = ColumnPipeProcessor([float_to_int, int_to_string], cols="customer_id")
pipeline = PipeProcessor([ps, col_ps])

result_b = pipeline(data)

pd.testing.assert_frame_equal(result_a, result_b)
