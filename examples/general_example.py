import dataclasses
import re
import string
import typing as T

import nltk

from dpipes.pipeline import Pipeline

nltk.download("punkt")
nltk.download("stopwords")
nltk.download("wordnet")


############################################################################
# Simple list example
############################################################################

data = [3, 19, 30, 18]


def add_two(x: T.List):
    return [z + 2 for z in x]


def mult_two(x: T.List):
    return [z * 2 for z in x]


simple_pl = Pipeline([add_two, mult_two])
results = simple_pl(data)


############################################################################
# Text processing example
############################################################################


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


sample = """
Hello @gabe_flomo 👋🏾, still want us to hit that new sushi spot??? LMK when you're free cuz I
can't go this or next weekend since I'll be swimming!!! #sushiBros #rawFish #🍱
"""

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

############################################################################
# Arbitrary dataclass & kwargs example
############################################################################


@dataclasses.dataclass
class Product:
    price: float
    description: str
    units_available: float


def adjust_price(p: Product, fn: T.Callable) -> Product:
    p.price = fn(p.price)
    return p


def clean_description(p: Product) -> Product:
    p.description = p.description.title()
    return p


def add_units(p: Product, n_units: int) -> Product:
    p.units_available += n_units
    return p


eggs = Product(4.99, "one dozen eggs", 20)
bread = Product(3.99, "wheat, natural", 10)
milk = Product(2.99, "whole, 1 quart", 20)

pl = Pipeline(
    funcs=[
        adjust_price,
        clean_description,
        add_units,
    ],
    kwargs=[{"fn": lambda x: x * 1.1}, None, {"n_units": 100}],
)

for prod in [eggs, bread, milk]:
    result = pl(prod)
    print(result)
