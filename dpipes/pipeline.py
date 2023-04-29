import functools
import typing as T


def make_pipeline(
    funcs: T.Sequence[T.Callable],
    kwargs: T.Optional[
        T.Union[T.Dict[str, T.Any], T.Sequence[T.Optional[T.Dict[str, T.Any]]]]
    ] = None,
) -> T.Callable:
    """
    Compose a pipeline from a sequence of functions.

    Parameters
    ----------
    funcs: Sequence[Callable]
        A sequence of callable functions.
    kwargs: Optional[Union[Dict[str, Any], Sequence[Optional[Dict[str, Any]]]
        An iterable collection of kwargs to apply respective functions to. If a single set of
        kwargs is passed they will be broadcast across the sequence of functions.
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
    funcs: T.Sequence[T.Callable],
    kwargs: T.Union[T.Dict[str, T.Any], T.Sequence[T.Optional[T.Dict[str, T.Any]]]],
) -> T.Sequence[T.Callable]:
    """
    Create a sequence of partial functions.

    Parameters
    ----------
    funcs: Sequence[Callable]
        A sequence of callable functions.
    kwargs: Union[Dict[str, Any], Sequence[Optional[Dict[str, Any]]
        An iterable collection of kwargs to apply respective functions to. If a single set of
        kwargs is passed they will be broadcast across the sequence of functions.
        Use `None` if a respective function does not require additional args.

    Returns
    -------
    Sequence[Callable]
        A sequence of partial functions.
    """
    if isinstance(kwargs, dict):
        return [functools.partial(f, **kwargs) for f in funcs]
    return [functools.partial(f, **kw) if kw else f for f, kw in zip(funcs, kwargs)]


class Pipeline:
    """
    Class to sequentially process an arbitrary number of functions.
    """

    def __init__(
        self,
        funcs: T.Sequence[T.Callable],
        kwargs: T.Optional[
            T.Union[T.Dict[str, T.Any], T.Sequence[T.Optional[T.Dict[str, T.Any]]]]
        ] = None,
    ):
        """
        Instantiate a Pipeline object.

        Parameters
        ----------
        funcs: Sequence[Callable]
            A sequence of callable functions.
        kwargs: Optional[Union[Dict[str, Any], Sequence[Optional[Dict[str, Any]]]
            An iterable collection of kwargs to apply respective functions to. If a single set of
            kwargs is passed they will be broadcast across the sequence of functions.
            Use `None` if a respective function does not require additional args.
        """
        if kwargs:
            if isinstance(kwargs, T.Sequence):
                self._check_args(funcs, kwargs)
        self.funcs = funcs
        self.kwargs = kwargs

    def __call__(self, x):
        reducer = make_pipeline(self.funcs, self.kwargs)
        return reducer(x)

    @staticmethod
    def _check_args(funcs, args):
        if len(funcs) != len(args):
            raise ValueError(
                f"""
                    Length of `kwargs` must match length of `funcs`.
                    Expected {len(funcs)} collections of kwargs, only got {len(args)}.
                    """
            )
