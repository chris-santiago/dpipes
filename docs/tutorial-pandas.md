# Pandas Tutorial

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

import pandas as pd

from dpipes.processor import ColumnPipeProcessor, PipeProcessor

data = pd.read_csv("examples/sample.csv")
```

### Transformations

Lets' define some functions to transform our data. We'll start off by converting the camel-case 
column names to snake-case:

```python
def camel_to_snake(x: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z][a-z])", "_", re.sub(r"\s+", "_", x))


def clean_colnames(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(lambda x: camel_to_snake(x).lower(), axis=1)
```

Next, we'll define a few functions that will calculate the total price per line item, calculate the
total price per invoice order, calculate the number of unique products in each order, and calculate
the total number of items in each order.

```python
def add_line_total(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(line_item_total=lambda x: x["quantity"] * x["price"])


def add_order_total(df: pd.DataFrame) -> pd.DataFrame:
    order_total = (
        df.groupby("invoice")["line_item_total"].sum().reset_index(name="order_total")
    )
    return df.merge(order_total, how="left", on="invoice")


def add_order_num_products(df: pd.DataFrame) -> pd.DataFrame:
    num_products = df.groupby("invoice").size().reset_index(name="order_num_products")
    return df.merge(num_products, how="left", on="invoice")


def add_total_order_size(df: pd.DataFrame) -> pd.DataFrame:
    order_size = df.groupby('invoice')['quantity'].sum().reset_index(name="order_size")
    return df.merge(order_size, how="left", on="invoice")
```

### Data Pipeline

Now, let's chain these functions together to make a simple processing pipeline.

#### Naive Version

There's nothing inherently *wrong* about processing the data this way-- the end results will be
identical to other methods. However, some software engineers do advice against over-writing an
object many times.

```python title="Naive, Repeated Calls"
naive = clean_colnames(data)
naive = add_line_total(naive)
naive = add_order_total(naive)
naive = add_order_num_products(naive)
naive = add_total_order_size(naive)
```

#### Method Chaining

A better approach is to use Pandas' `dataframe.pipe` method to chain all these operations together.
As noted, you'll find the results identical.

```python title="Using Pandas Pipe and Method Chaining"
# Method chaining
result_a = (
    data.pipe(clean_colnames)
    .pipe(add_line_total)
    .pipe(add_order_total)
    .pipe(add_order_num_products)
    .pipe(add_total_order_size)
)

pd.testing.assert_frame_equal(naive, result_a)
```

#### PipeProcessor

Now, let's see how this looks using the `dpipe.PipeProcessor` class. We'll instantiate an object
by passing a list of functions that we want to run, in order. We can this use this object to run
the pipeline on any passed dataset.

```python title="Using PipeProcessor"
ps = PipeProcessor([
    clean_colnames,
    add_line_total,
    add_order_total,
    add_order_num_products,
    add_total_order_size
])
result_b = ps(data)

pd.testing.assert_frame_equal(result_a, result_b)
```

!!! tip
    
    We're not showing it in this example, but the `dpipe.PipeProcessor` can take an optional `kwargs` 
    parameter, which can be a single dictionary or a list of dictionaries that map keyword arguments. 
    If a single dictionary is passed, those keyword arguments will be broadcast to each function in 
    the pipeline. If a list of dictionaries is passed, each set of keyword arguments will be applied 
    to their respective functions, in order.

    See [`dpipe.PipeProcessor` reference](processor-ref.md#dpipes.processor.PipeProcessor) for details.

Further, we could now create modularized pipelines that can easily be imported and used elsewhere
in code:

```python title="my_module.py"
"""My pipeline module."""

from dpipes.processor import PipeProcessor


def task_1(...):
    ...


def task_2(...):
    ...


def task_3(...):
    ...


def task_4(...):
    ...


my_pipeline = PipeProcessor([task_1, task_2, task_3, task_4])
```

```python title="main.py"
from my_module import my_pipeline

my_pipeline(my_data)
```


## Processing Multiple Datasets

Continuing on, imagine now that you want to run the same pipeline on multiple datasets, all with
a similar schema. 

```python
split_1, split_2, split_3 = (
    data.iloc[:300, :],
    data.iloc[300:600, :],
    data.iloc[600:, :],
)
```

### Method Chaining

Again, the end results will be identical-- but note how you would need to re-write (or copy/paste)
your entire method-chained operation to run this pipeline on new datasets.

```python title="Using Pandas Pipe and Method Chaining"
for ds in [split_1, split_2, split_3]:
    result_a = (
        ds.pipe(clean_colnames)
        .pipe(add_line_total)
        .pipe(add_order_total)
        .pipe(add_order_num_products)
        .pipe(add_total_order_size)
    )
```

### PipeProcessor

Contrast this with the `dpipes.PipeProcessor` methodology, where you simply need to call the original 
pipeline object.

```python title="Using PipeProcessor"
for ds in [split_1, split_2, split_3]:
    result_b = ps(ds)

pd.testing.assert_frame_equal(result_a, result_b)
```

## Processing Individual Columns

Now, let's suppose that we want to add a few column-specific operations to our pipeline. Let's define
one function where we convert floats to integers, and another were we convert integers to strings. 
We'll use these functions to cast `customer_id` field as a string.

```python
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
```

### Method Chaining

Here we'll add two `DataFrame.pipe` calls with a lambda function to apply our casting operations
to a specific column.

```python title="Adding Lambdas to the Method Chain"
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

### PipeProcessor

We'll create two new `PipeProcessor` objects: one to process functions on the `customer_id` function,
and another that will compose both our original and column-specific pipelines into a single processor.

One can easily create an arbitrary number of sub-pipelines and pipeline compositions.

```python title="PipeProcessor Composition"
col_ps = PipeProcessor([float_to_int, int_to_string], {"cols": "customer_id"})
pipeline = PipeProcessor([ps, col_ps])
result_b = pipeline(data)

pd.testing.assert_frame_equal(result_a, result_b)
```

!!! note

    Note that we only passed a single dictionary to the `dpipes.PipeProcessor` constructor, and it
    broadcast those keyword arguments to both functions within the pipeline.

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

pd.testing.assert_frame_equal(result_a, result_b)
```
