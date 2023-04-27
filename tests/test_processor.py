import pandas as pd
import pytest

import dpipes


def add_two(df, cols):
    df = df.copy()
    df[cols] += 2
    return df


def mult_two(df, cols):
    df = df.copy()
    df[cols] *= 2
    return df


def add_five(df):
    df = df.copy()
    return df + 5


class TestPipeProcessor:
    def test_broadcast_kwargs(self, data):
        pl = dpipes.PipeProcessor(funcs=[add_two, mult_two], kwargs={"cols": "a"})
        actual = pl(data)
        expected = pd.DataFrame({"a": [14, 24], "b": [2, 4]})
        pd.testing.assert_frame_equal(actual, expected)

    def test_no_kwargs_single(self, data):
        pl = dpipes.PipeProcessor(funcs=[add_five])
        actual = pl(data)
        expected = pd.DataFrame({"a": [10, 15], "b": [7, 9]})
        pd.testing.assert_frame_equal(actual, expected)

    def test_no_kwargs_multi(self, data):
        pl = dpipes.PipeProcessor(funcs=[add_five, add_five])
        actual = pl(data)
        expected = pd.DataFrame({"a": [15, 20], "b": [12, 14]})
        pd.testing.assert_frame_equal(actual, expected)

    def test_kwargs_single(self, data):
        pl = dpipes.PipeProcessor(
            funcs=[add_two],
            kwargs=[
                {"cols": ["a"]},
            ],
        )
        actual = pl(data)
        expected = pd.DataFrame({"a": [7, 12], "b": [2, 4]})
        pd.testing.assert_frame_equal(actual, expected)

    def test_kwargs_all(self, data):
        pl = dpipes.PipeProcessor(
            funcs=[add_two, mult_two], kwargs=[{"cols": ["a", "b"]}, {"cols": "a"}]
        )
        actual = pl(data)
        expected = pd.DataFrame({"a": [14, 24], "b": [4, 6]})
        pd.testing.assert_frame_equal(actual, expected)

    def test_kwargs_and_none(self, data):
        pl = dpipes.PipeProcessor(
            funcs=[add_two, add_five], kwargs=[{"cols": ["a", "b"]}, None]
        )
        actual = pl(data)
        expected = pd.DataFrame({"a": [12, 17], "b": [9, 11]})
        pd.testing.assert_frame_equal(actual, expected)

    def test_bad_length_raises(self, data):
        with pytest.raises(ValueError) as exc:
            dpipes.PipeProcessor(
                funcs=[add_two, add_five],
                kwargs=[
                    {"cols": ["a", "b"]},
                ],
            )
        print(exc.value.args[0])


class TestColumnPipeProcessor:
    def test_one_col(self, data):
        pl = dpipes.ColumnPipeProcessor(funcs=[add_two], cols="a")
        actual = pl(data)
        expected = pd.DataFrame({"a": [7, 12], "b": [2, 4]})
        pd.testing.assert_frame_equal(actual, expected)

    def test_two_cols(self, data):
        pl = dpipes.ColumnPipeProcessor(funcs=[add_two], cols=["a", "b"])
        actual = pl(data)
        expected = pd.DataFrame({"a": [7, 12], "b": [4, 6]})
        pd.testing.assert_frame_equal(actual, expected)

    def test_mulit_one_col(self, data):
        pl = dpipes.ColumnPipeProcessor(funcs=[add_two, mult_two], cols="a")
        actual = pl(data)
        expected = pd.DataFrame({"a": [14, 24], "b": [2, 4]})
        pd.testing.assert_frame_equal(actual, expected)

    def test_multi_multi_col(self, data):
        pl = dpipes.ColumnPipeProcessor(
            funcs=[add_two, mult_two], cols=["a", ["a", "b"]]
        )
        actual = pl(data)
        expected = pd.DataFrame({"a": [14, 24], "b": [4, 8]})
        pd.testing.assert_frame_equal(actual, expected)
