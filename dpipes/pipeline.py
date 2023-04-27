import functools
import typing as T


def make_pipeline(
    funcs: T.Sequence[T.Callable],
    kwargs: T.Optional[T.Sequence[T.Optional[T.Dict[str, T.Any]]]],
) -> T.Callable:
    """
    Compose a pipeline from a sequence of functions.

    Parameters
    ----------
    funcs: Sequence[Callable]
        A sequence of callable functions.
    kwargs: Optional[Sequence[Optional[Dict[str, Any]]]]
        An optional, iterable collection of (optional) kwargs to apply respective functions to.
        Use `None` if a respective function does not require additional args.

    Returns
    -------
    Callable
        A pipeline composition function.
    """
    if kwargs:
        funcs = make_partials(funcs, kwargs)
    return functools.reduce(lambda f, g: lambda x: g(f(x)), funcs)


def make_partials(
    funcs: T.Sequence[T.Callable], kwargs: T.Sequence[T.Optional[T.Dict[str, T.Any]]]
) -> T.Sequence[T.Callable]:
    """
    Create a sequence of partial functions.

    Parameters
    ----------
    funcs: Sequence[Callable]
        A sequence of callable functions.
    kwargs: Sequence[Optional[Dict[str, Any]]]
        An iterable collection of (optional) kwargs to apply respective functions to. Use `None` if
        a respective function does not require additional args.

    Returns
    -------
    Sequence[Callable]
        A sequence of partial functions.
    """
    return [functools.partial(f, **kw) if kw else f for f, kw in zip(funcs, kwargs)]


class Pipeline:
    """
    Class to sequentially process an arbitrary number of functions.
    """

    def __init__(
        self,
        funcs: T.Sequence[T.Callable],
        kwargs: T.Optional[T.Sequence[T.Optional[T.Dict[str, T.Any]]]] = None,
    ):
        """
        Instantiate a Pipeline object.

        Parameters
        ----------
        funcs: Sequence[Callable]
            A sequence of callable functions.
        kwargs: Optional[Sequence[Optional[Dict[str, Any]]]]
            An optional, iterable collection of (optional) kwargs to apply respective functions to.
            Use `None` if a respective function does not require additional args.
        """
        self.funcs = funcs
        self.kwargs = kwargs

    def __call__(self, x):
        reducer = make_pipeline(self.funcs, self.kwargs)
        return reducer(x)
