# Polars Tutorial

!!! note

    This tutorial assumes that you have [Polars](https://www.pola.rs/) installed in your Python 
    environment.

As mentioned in [Getting Started](getting-started.md), `dpipes.PipeProcessor` is extensible to any 
API that implements a Pandas-like `DataFrame.pipe` method.

!!! tip

    `dPipes` is extensible beyond Pandas-like APIs, too. The [`dpipes.Pipeline` module](pipeline-ref.md)
    generalizes the pipeline composability to any arbitrary Python functions and objects.

We'll run through a condensed version [Pandas tutorial](tutorial-pandas.md), using [Polars](https://www.pola.rs/)
as the swap from Pandas to Polars is 1:1. The only changes occur in the transformation functions-- 
[Polars API](https://www.pola.rs/) is much more concise than Pandas.

## Basic Example

We'll be using a [sample](https://raw.githubusercontent.com/chris-santiago/dpipes/master/examples/sample.csv)
from the Online Retail II data set. It contains all the transactions occurring for a UK-based and 
registered, non-store online retail between 01/12/2009 and 09/12/2011.The company mainly sells 
unique all-occasion gift-ware. 

The full dataset is available at the [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/Online+Retail+II).

Here's what the first few rows look like:

{{ read_csv('.data/sample.csv') }}

### Setup

We'll import required packages and read in the example dataset:

```python
import re
import typing as T

import polars as pl
from polars import testing

from dpipes.processor import ColumnPipeProcessor, PipeProcessor

data = pl.read_csv("examples/sample.csv", ignore_errors=True)
```

### Transformations

Lets' define some functions to transform our data. We'll start off by converting the camel-case 
column names to snake-case:

```python
def camel_to_snake(x: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z][a-z])", "_", re.sub(r"\s+", "_", x))


def clean_colnames(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({x: camel_to_snake(x).lower() for x in df.columns})
```

Next, we'll define a few functions that will calculate the total price per line item, calculate the
total price per invoice order, calculate the number of unique products in each order, and calculate
the total number of items in each order.

Finally, let's define one function where we convert floats to integers, and another were we convert 
integers to strings.  We'll use these functions to cast `customer_id` field as a string.

```python title="Transformation Functions with Polars"
def add_line_total(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        line_item_total=pl.col("quantity") * pl.col("price")
    )


def add_order_total(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        order_total=pl.col("line_item_total").sum().over("invoice")
    )


def add_order_num_products(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        order_size=pl.count().over("invoice")
    )


def add_total_order_size(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        order_size=pl.col("quantity").sum().over("invoice")
    )


def float_to_int(
    df: pl.DataFrame, cols: T.Union[str, T.Sequence[str]], fillna: int = -99999
) -> pl.DataFrame:
    return df.with_columns(pl.col(cols).fill_nan(fillna).cast(int))


def int_to_string(
    df: pl.DataFrame, cols: T.Union[str, T.Sequence[str]]
) -> pl.DataFrame:
    return df.with_columns(pl.col(cols).cast(str))

```

### Data Pipeline

Now, let's chain these functions together to make a simple processing pipeline.

#### Method Chaining

We can use Polars' `dataframe.pipe` method to chain all these operations together. We'll add two 
`DataFrame.pipe` calls with a lambda function to apply our casting operations to a specific column.

```python title="Using Polars Pipe and Method Chaining"
result_a = (
    data.pipe(clean_colnames)
    .pipe(add_line_total)
    .pipe(add_order_total)
    .pipe(add_order_num_products)
    .pipe(add_total_order_size)
    .pipe(lambda x: float_to_int(x, "customer_id"))
    .pipe(lambda x: int_to_string(x, "customer_id"))
)
```

#### PipeProcessor

Now, let's see how this looks using the `dpipe.PipeProcessor` class. We'll instantiate an object
by passing a list of functions that we want to run, in order. We can this use this object to run
the pipeline on any passed dataset.

We'll create two new `PipeProcessor` objects: one to process functions on the `customer_id` function,
and another that will compose both our original and column-specific pipelines into a single processor.

One can easily create an arbitrary number of sub-pipelines and pipeline compositions.

```python title="PipeProcessor Composition"
ps = PipeProcessor([
    clean_colnames,
    add_line_total,
    add_order_total,
    add_order_num_products,
    add_total_order_size
])
col_ps = PipeProcessor([float_to_int, int_to_string], {"cols": "customer_id"})
pipeline = PipeProcessor([ps, col_ps])
result_b = pipeline(data)

testing.assert_frame_equal(result_a, result_b)
```

!!! note

    Note that we only passed a single dictionary to the `dpipes.PipeProcessor` constructor, and it
    broadcast those keyword arguments to both functions within the pipeline.

**Although both methods produce identical results, only the use of `PipeProcessor` provides a reusable,
modular pipeline object.**

### ColumnPipeProcessor

Finally, if the only keyword arguments to our transformation functions are column names, we can 
choose to use the [`dpipes.ColumnPipeProcessor`](processor-ref.md#dpipes.processor.ColumnPipeProcessor), 
instead. Similar to the `dpipes.PipeProcessor` class,
we can pass in a single column or single list of columns to broadcast to the functions within the
pipeline. You can also specify specific column(s) for each function to act on by passing a list of
lists.

```python title="ColumnPipeProcessor"
col_ps = ColumnPipeProcessor([float_to_int, int_to_string], cols="customer_id")
pipeline = PipeProcessor([ps, col_ps])

result_b = pipeline(data)

testing.assert_frame_equal(result_a, result_b)
```
