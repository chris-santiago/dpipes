import pandas as pd
import pytest

from dpipes.pipeline import Pipeline


def add_two(x):
    return x + 2


def mult_two(x):
    return x * 2


def df_add(df, x):
    return df + x


def df_mult(df, x):
    return df * x


class TestPipeline:
    def test_no_kwargs(self):
        z = 2
        pl = Pipeline([add_two, add_two, mult_two, add_two])

        expected = add_two(mult_two(add_two(add_two(2))))
        actual = pl(z)
        assert expected == actual

    def test_equal_kwargs(self):
        z = pd.DataFrame({"a": [2, 2], "b": [2, 2]})

        pl = Pipeline(
            funcs=[df_add, df_add, df_mult, df_add],
            kwargs=[{"x": 2}, {"x": 2}, {"x": 2}, {"x": 2}],
        )

        expected = df_add(df_mult(df_add(df_add(z, 2), 2), 2), 2)
        actual = pl(z)
        pd.testing.assert_frame_equal(expected, actual)

    def test_too_few_kwargs_raises(self):
        pd.DataFrame({"a": [2, 2], "b": [2, 2]})

        with pytest.raises(ValueError):
            Pipeline(
                funcs=[df_add, df_add, df_mult, df_add],
                kwargs=[{"x": 2}, {"x": 2}, {"x": 2}],
            )

    def test_broadcast_kwargs(self):
        z = pd.DataFrame({"a": [2, 2], "b": [2, 2]})

        pl = Pipeline(
            funcs=[df_add, df_add, df_mult, df_add],
            kwargs={"x": 2},
        )

        expected = df_add(df_mult(df_add(df_add(z, 2), 2), 2), 2)
        actual = pl(z)
        pd.testing.assert_frame_equal(expected, actual)
