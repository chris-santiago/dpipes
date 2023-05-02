# General Pipeline Tutorial

As mentioned in the [Getting Started](getting-started.md) section, the [`dipes.pipeline` module](pipeline-ref.md)
extends the `DataFrame.pipe` composition into any arbitrary Python function. Let's take a look at 
some examples.

## A Simple List Example

### Setup

We'll use a very simple list as our data object in this example.

```python
import typing as T

from dpipes.pipeline import Pipeline

data = [3, 19, 30, 18]
```

### Transformations

Let's define two simple functions: one that adds two to each element in a list, and another that 
multiplies each element in a list by two.

```python title="Simple Transformation Functions"
def add_two(x: T.List):
    return [z + 2 for z in x]


def mult_two(x: T.List):
    return [z * 2 for z in x]
```

### Pipeline

We'll create our pipeline using the `dpipes.pipeline.Pipeline` class. This class generalizes the
DataFrame pipeline classes to work on any arbitrary Python object. Like the others, it accepts both
a list of functions and an optional dictionary (or sequence of dictionaries) containing keyword
arguments.

```python title="Simple Pipeline Example"
simple_pl = Pipeline([add_two, mult_two])
results = simple_pl(data)
print(results)
```

```zsh title="Output"
>>>   [10, 42, 64, 40]
```

## Text Preprocessing

In this example, we'll take a look at some basic text preprocessing tasks and demonstrate how users
can construct pipelines with and without arbitrary keyword arguments.

!!! note

    This tutorial requires that you have [nltk](https://www.nltk.org/) installed in your Python 
    environment. You can install `nltk` and other tutorial dependencies by executing the command
    `pip install dpipes[demo]`.

### Setup

We'll download the relevant `nltk` objects before beginning, and create a `sample` text snippet.

```python
import re
import string
import typing as T

import nltk

from dpipes.pipeline import Pipeline

nltk.download("punkt")
nltk.download("stopwords")
nltk.download("wordnet")

sample = """
Hello @gabe_flomo 👋🏾, still want us to hit that new sushi spot??? LMK when you're free cuz I 
can't go this or next weekend since I'll be swimming!!! #sushiBros #rawFish #🍱
"""
```

### Transformations

Next, we'll define several functions to clean and process our text to remove punctuation, lower 
text case, remove stopwords, remove emoji and lemmatize tokens

```python title="Basic Text Preprocessing"
def remove_punctuation(tokens: T.List[str], punctuation: str) -> T.List[str]:
    return [t for t in tokens if t not in punctuation]


def to_lower(tokens: T.List[str]) -> T.List[str]:
    return [t.lower() for t in tokens]


def remove_stopwords(tokens: T.List[str], stopwords: T.List[str]) -> T.List[str]:
    return [t for t in tokens if t not in stopwords]


def remove_emoji(text: str) -> str:
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE,
    )
    return emoji_pattern.sub(r"", text)


def lemmatize(tokens: T.List[str]) -> T.List[str]:
    wnl = nltk.stem.WordNetLemmatizer()
    return [wnl.lemmatize(t) for t in tokens]
```

### Pipeline

Now, let's create our pipeline. We'll use our previously-defined functions along with `nltk`'s
work tokenizer.

```python title="Text Preprocessing Pipeline"
ps = Pipeline(
    funcs=[
        remove_emoji,
        nltk.tokenize.word_tokenize,
        to_lower,
        remove_punctuation,
        remove_stopwords,
        lemmatize,
    ],
    kwargs=[
        None,
        None,
        None,
        {"punctuation": string.punctuation},
        {"stopwords": nltk.corpus.stopwords.words("english")},
        None,
    ],
)

result = ps(sample)
print(result)
```

```zsh title="Output"
>>> ['hello', 'gabe_flomo', 'still', 'want', 'u', 'hit', 'new', 'sushi', 'spot', 'lmk', "'re", 'free', 'cuz', 'ca', "n't", 'go', 'next', 'weekend', 'since', "'ll", 'swimming', 'sushibros', 'rawfish']
```

#### Composing Pipelines

Of course, there's nothing to say that you must include all pipeline tasks in a single, monolithic
pipeline. We can breakup pipelines into logical, sub-pipelines (if so desired)-- provided that the
order remains the same or is irrelevant.

Let's decompose our original text preprocessing pipeline into two separate objects, based on the 
need for keyword arguments.

```python title="Pipeline Composition"
sub_1 = Pipeline(
    funcs=[
        remove_emoji,
        nltk.tokenize.word_tokenize,
        to_lower,
        lemmatize,
    ]
)
sub_2 = Pipeline(
    funcs=[
        remove_punctuation,
        remove_stopwords,
    ],
    kwargs=[
        {"punctuation": string.punctuation},
        {"stopwords": nltk.corpus.stopwords.words("english")},
    ]
)

pl = Pipeline([sub_1, sub_2])
result = pl(sample)
print(result)
```

```zsh title="Output"
>>> ['hello', 'gabe_flomo', 'still', 'want', 'u', 'hit', 'new', 'sushi', 'spot', 'lmk', "'re", 'free', 'cuz', 'ca', "n't", 'go', 'next', 'weekend', 'since', "'ll", 'swimming', 'sushibros', 'rawfish']
```

## Processing Custom Objects

To again illustrate the flexibility of the `dpipes.Pipeline` class, let's create a custom dataclass
for a grocery product. We'll keep it simple and track its price, description and number of available
units.

### Setup

```python
import dataclasses
import typing as T

from dpipes.pipeline import Pipeline


@dataclasses.dataclass
class Product:
    price: float
    description: str
    units_available: float
```

### Transformations

Next, let's define some transformation functions. We'll create one that adjusts a product's price,
one that changes its description to title case, and another that updates the number of available
units.

```python title="Dataclass Transformations"
def adjust_price(p: Product, fn: T.Callable) -> Product:
    p.price = fn(p.price)
    return p


def clean_description(p: Product) -> Product:
    p.description = p.description.title()
    return p


def add_units(p: Product, n_units: int) -> Product:
    p.units_available += n_units
    return p
```

### Pipeline

Finally, let's create a few product and process them with a pipeline.

```python title="Dataclass Pipeline"
eggs = Product(4.99, "one dozen eggs", 20)
bread = Product(3.99, "wheat, natural", 10)
milk = Product(2.99, "whole, 1 quart", 20)

pl = Pipeline(
    funcs=[
        adjust_price,
        clean_description,
        add_units,
    ],
    kwargs=[
        {"fn": lambda x: x * 1.1},
        None,
        {"n_units": 100}
    ],
)

for prod in [eggs, bread, milk]:
    result = pl(prod)
    print(result)
```

```zsh title="Output"
>>> Product(price=5.489000000000001, description='One Dozen Eggs', units_available=120)
>>> Product(price=4.389, description='Wheat, Natural', units_available=110)
>>> Product(price=3.2890000000000006, description='Whole, 1 Quart', units_available=120)
```