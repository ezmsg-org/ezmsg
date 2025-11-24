from collections.abc import Mapping
import typing


T = typing.TypeVar("T")


def is_dict_like(value: typing.Any) -> typing.TypeGuard[Mapping]:
    """
    Check if a value behaves like a dictionary.

    This function checks if the value has the basic dictionary interface
    by verifying it has 'keys' and '__getitem__' attributes.

    :param value: The value to check
    :type value: typing.Any
    :return: True if the value is dict-like, False otherwise
    :rtype: typing.TypeGuard[collections.abc.Mapping]
    """
    return hasattr(value, "keys") and hasattr(value, "__getitem__")


def either_dict_or_kwargs(
    pos_kwargs: Mapping[str, T] | None,
    kw_kwargs: Mapping[str, T],
    func_name: str,
) -> Mapping[str, T]:
    """
    Handle flexible argument passing patterns for functions that accept either
    positional dict or keyword arguments.

    This utility function helps implement the common pattern where a function
    can accept either a dictionary as the first argument or keyword arguments,
    but not both.

    :param pos_kwargs: Optional mapping passed as positional argument
    :type pos_kwargs: collections.abc.Mapping[str, T] | None
    :param kw_kwargs: Mapping of keyword arguments
    :type kw_kwargs: collections.abc.Mapping[str, T]
    :param func_name: Name of the calling function (for error messages)
    :type func_name: str
    :return: The resolved mapping of arguments
    :rtype: collections.abc.Mapping[str, T]
    :raises ValueError: If both positional and keyword arguments are provided,
                       or if the positional argument is not dict-like
    """
    if pos_kwargs is None or pos_kwargs == {}:
        # Need an explicit cast to appease mypy due to invariance; see
        # https://github.com/python/mypy/issues/6228
        return typing.cast(Mapping[str, T], kw_kwargs)

    if not is_dict_like(pos_kwargs):
        raise ValueError(f"the first argument to .{func_name} must be a dictionary")
    if kw_kwargs:
        raise ValueError(
            f"cannot specify both keyword and positional arguments to .{func_name}"
        )
    return pos_kwargs
