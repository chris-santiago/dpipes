import re
import typing as T

import polars as pl
from polars import testing

from dpipes.processor import ColumnPipeProcessor, PipeProcessor

data = pl.read_csv("examples/sample.csv", ignore_errors=True)


def camel_to_snake(x: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z][a-z])", "_", re.sub(r"\s+", "_", x))


def clean_colnames(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({x: camel_to_snake(x).lower() for x in df.columns})


def add_line_total(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(line_item_total=pl.col("quantity") * pl.col("price"))


def add_order_total(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(order_total=pl.col("line_item_total").sum().over("invoice"))


def add_order_num_products(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(order_size=pl.count().over("invoice"))


def add_total_order_size(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(order_size=pl.col("quantity").sum().over("invoice"))


############################################################################
# Basic Example
############################################################################

# Method chaining
result_a = (
    data.pipe(clean_colnames)
    .pipe(add_line_total)
    .pipe(add_order_total)
    .pipe(add_order_num_products)
    .pipe(add_total_order_size)
)

# PipeProcessor
ps = PipeProcessor(
    [
        clean_colnames,
        add_line_total,
        add_order_total,
        add_order_num_products,
        add_total_order_size,
    ]
)
result_b = ps(data)

testing.assert_frame_equal(result_a, result_b)


############################################################################
# Process multiple, similar datasets
############################################################################

split_1, split_2, split_3 = data[:300, :], data[300:600, :], data[600:, :]

# Method chaining
for ds in [split_1, split_2, split_3]:
    result_a = (
        ds.pipe(clean_colnames)
        .pipe(add_line_total)
        .pipe(add_order_total)
        .pipe(add_order_num_products)
        .pipe(add_total_order_size)
    )


# PipeProcessor
for ds in [split_1, split_2, split_3]:
    result_b = ps(ds)

testing.assert_frame_equal(result_a, result_b)


############################################################################
# Process dataframe and individual columns
############################################################################


def float_to_int(
    df: pl.DataFrame, cols: T.Union[str, T.Sequence[str]], fillna: int = -99999
) -> pl.DataFrame:
    return df.with_columns(pl.col(cols).fill_nan(fillna).cast(int))


def int_to_string(
    df: pl.DataFrame, cols: T.Union[str, T.Sequence[str]]
) -> pl.DataFrame:
    return df.with_columns(pl.col(cols).cast(str))


# Method chaining
result_a = (
    data.pipe(clean_colnames)
    .pipe(add_line_total)
    .pipe(add_order_total)
    .pipe(add_order_num_products)
    .pipe(add_total_order_size)
    .pipe(lambda x: float_to_int(x, "customer_id"))
    .pipe(lambda x: int_to_string(x, "customer_id"))
)

# PipeProcessor
col_ps = PipeProcessor([float_to_int, int_to_string], {"cols": "customer_id"})
pipeline = PipeProcessor([ps, col_ps])

result_b = pipeline(data)

testing.assert_frame_equal(result_a, result_b)

# ColumnPiperProcessor
col_ps = ColumnPipeProcessor([float_to_int, int_to_string], cols="customer_id")
pipeline = PipeProcessor([ps, col_ps])

result_b = pipeline(data)

testing.assert_frame_equal(result_a, result_b)
