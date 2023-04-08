import pandas as pd
import pytest


@pytest.fixture
def data():
    return pd.DataFrame({"a": [5, 10], "b": [2, 4]})
