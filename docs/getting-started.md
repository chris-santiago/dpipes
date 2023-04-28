# Getting Started

## About

`dPipes` is a Python package for creating Pythonic, modular data pipelines. It's small project that came 
out of the desire to turn this:

```py
import pandas as pd

data = (data.pipe(func_1)
        .pipe(func_2)
        .pipe(func_3)
)
```

into this:

```py
from dpipes.processor import PipeProcessor

ps = PipeProcessor(
    funcs=[func_1, func_2, func_3]
)

data = ps(data)
```

Although `dPipes` initially addressed `pd.DataFrame.pipe` method-chaining, it's extensible to any
API that implements a pandas-like `DataFrame.pipe` method (e.g. Polars). 

Further, the [dpipes.pipeline module](pipeline-ref.md) extends this composition to any arbitrary Python function.

## Installation

`dPipes` is can be installed via `pip`:

```zsh
pip install dpipes
```

We recommend setting up a virtual environment with Python >= 3.8.  

## Benefits

### Reusable Pipelines

As you'll see in the [tutorials](tutorial-pandas.md), one of the key benefits of using `dPipes` is
the reusable pipeline object that can be called on multiple datasets (provided their schemas are
similar):

```python title="Using PipeProcessor"
for ds in [split_1, split_2, split_3]:
    result_b = ps(ds)

pd.testing.assert_frame_equal(result_a, result_b)
```

### Modular Pipelines

Another is the ability to create modularized pipelines that can easily be imported and used 
elsewhere in code:

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

### Composable Pipelines

Finally, you can compose large, complex processing pipelines using an arbitrary number of sub-pipelines:

```python title="PipeProcessor Composition"
ps = PipeProcessor([
    task_1,
    task_2,
    task_3,
    task_4,
])

col_ps_single = ColumnPipeProcessor(
    funcs=[task_5, task_6],
    cols="customer_id"
)

col_ps_multi = ColumnPipeProcessor(
    funcs=[task_7, task_8],
    cols=["customer_id", "invoice"]
)

col_ps_nested = ColumnPipeProcessor(
    funcs=[task_9, task_10],
    cols=[
        ["quantity", "price"],
        ["invoice"],
    ]
)

pipeline = PipeProcessor([
    ps,
    col_ps_single,
    col_ps_multi,
    col_ps_nested,
])

result = pipeline(data)
```