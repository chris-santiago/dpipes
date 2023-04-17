from pipes.pipeline import Pipeline

import pandas as pd

add_two = lambda x: x + 2
mult_two = lambda x: x * 2

df_add = lambda df, x: df + x
df_mult = lambda df, x: df * x


class TestPipeline:

    def test_no_kwargs(self):
        z = 2
        pl = Pipeline([
            add_two,
            add_two,
            mult_two,
            add_two
        ])

        expected = add_two(mult_two(add_two(add_two(2))))
        actual = pl(z)
        assert expected == actual

    def test_kwargs(self):
        z = pd.DataFrame({
            'a': [2, 2],
            'b': [2, 2]
        })

        pl = Pipeline(
            funcs=[
                df_add,
                df_add,
                df_mult,
                df_add
            ],
            kwargs=[
                {'x': 2},
                {'x': 2},
                {'x': 2},
                {'x': 2}
            ]
        )

        expected = df_add(df_mult(df_add(df_add(z, 2), 2), 2), 2)
        actual = pl(z)
        pd.testing.assert_frame_equal(expected, actual)
