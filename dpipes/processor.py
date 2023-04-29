import functools
import typing as T

from dpipes.pipeline import Pipeline, make_partials


class PipeProcessor(Pipeline):
    """
    Class to sequentially process an arbitrary number of pandas.DataFrame.pipe functions.
    """

    def __call__(self, df):
        if self.kwargs:
            self.funcs = make_partials(self.funcs, self.kwargs)
        return functools.reduce(lambda _df, trans: _df.pipe(trans), self.funcs, df)


class ColumnPipeProcessor(PipeProcessor):
    """
    Class to sequentially process an arbitrary number of pandas.DataFrame.pipe functions by column.
    """

    def __init__(
        self,
        funcs: T.Sequence[T.Callable],
        cols: T.Optional[T.Union[str, T.Sequence[T.Union[str, T.Sequence[str]]]]],
    ):
        """
        Instantiate processor.

        Parameters
        ----------
        funcs: Sequence[Callable]
            An iterable collection of user-defined functions. Function signatures should match
            `func(df, cols)`, where `df` is a pandas.DataFrame and `cols` is an optional list of
            columns to apply functions to.
        cols: Optional[Union[str, Sequence[Union[str, Sequence[str]]]
            An iterable collection of columns to apply respective functions to. If a single string
            or single list of strings is passed they will be broadcast across the sequence of
            functions.

        Returns
        -------
        pd.DataFrame
            A processed DataFrame.
        """
        super().__init__(funcs)
        if cols:
            # broadcast single string or single list
            if isinstance(cols, str) or (
                isinstance(cols, T.Sequence) and all(isinstance(x, str) for x in cols)
            ):
                self.funcs = [functools.partial(f, cols=cols) for f in funcs]

            else:
                self._check_args(funcs, cols)
                self.funcs = [
                    functools.partial(f, cols=c) if c else f
                    for f, c in zip(funcs, cols)
                ]

        else:  # apply funcs to entire dataframe
            self.funcs = funcs
