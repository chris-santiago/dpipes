# dPipes - Pythonic Data Pipelines

## About

`dPipes` is a small project that came out of the desire to turn this:

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

Now, arguably, there is not much functional difference between the two implementations. They both
accomplish the same task with roughly the same amount of code. 

**But, what happens if you want to apply the same pipeline of functions to a different data
object?**

Using the first method, you'd need to re-write (copy/paste) your method-chaining pipeline:

```py
new_data = (new_data.pipe(func_1)
        .pipe(func_2)
        .pipe(func_3)
)
```

Using the latter method, **you'd only need to pass in a different object** to the pipeline:

```py
new_data = ps(new_data)
```

## Under the Hood

`dPipes` uses two functions from Python's `functools` module: `reduce` and `partial`. The `reduce`
function enables function composition; the `partial` function enables use of arbitrary `kwargs`.

## Generalization

Although `dPipes` initially addressed `pd.DataFrame.pipe` method-chaining, it's extensible to any
API that implements a pandas-like `DataFrame.pipe` method (e.g. Polars). Further, the 
`dpipes.pipeline` extends this composition to any arbitrary Python function.  

That is, this:

```py
result = func_3(func_2(func_1(x)))
```

or this:

```py
result = func_1(x)
result = func_2(result)
result = func_3(result)
```

becomes this:

```py
from dpipes.pipeline import Pipeline

pl = Pipeline(funcs=[func_1, func_2, func_3])
result = pl(x)
```

which is, arguably, more readable and, once again, easier to apply to other objects.

!!! note

    Though the above examples use simple callables, users can pass any arbitary `kwargs` to a
    `Pipeline` object. The `PipelineProcessor` objects can also "broadcast" arguments throughout
     a DataFrame pipeline.

    See the tutorials and how-to sections for additional information and examples.