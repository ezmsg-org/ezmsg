import contextlib
import logging
import os
from collections.abc import Mapping
import typing


logger = logging.getLogger("ezmsg")

EZMSG_FD_LIMIT_ENV = "EZMSG_FD_LIMIT"
EZMSG_FD_LIMIT_DEFAULT = 100_000

try:
    import resource
except ImportError:
    resource = None  # type: ignore[assignment]


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


def _configured_fd_limit() -> int:
    value = os.environ.get(EZMSG_FD_LIMIT_ENV, str(EZMSG_FD_LIMIT_DEFAULT))
    try:
        limit = int(value)
    except ValueError:
        logger.warning(
            f"Invalid {EZMSG_FD_LIMIT_ENV}={value!r}; using default {EZMSG_FD_LIMIT_DEFAULT}"
        )
        return EZMSG_FD_LIMIT_DEFAULT
    return max(1, limit)


def _fd_limit_supported() -> bool:
    return resource is not None and hasattr(resource, "RLIMIT_NOFILE")


@contextlib.contextmanager
def elevated_fd_limit(limit: int | None = None):
    if not _fd_limit_supported():
        yield
        return

    assert resource is not None
    target = _configured_fd_limit() if limit is None else max(1, limit)
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    desired_limit = min(target, hard_limit)

    if desired_limit <= soft_limit:
        yield
        return

    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (desired_limit, hard_limit))
    except (OSError, ValueError) as exc:
        logger.warning(
            f"Unable to raise RLIMIT_NOFILE from {soft_limit} to {desired_limit}: {exc}"
        )
        yield
        return

    logger.info(
        f"Raised RLIMIT_NOFILE soft limit from {soft_limit} to {desired_limit}"
    )
    try:
        yield
    finally:
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (soft_limit, hard_limit))
        except (OSError, ValueError) as exc:
            logger.warning(
                f"Unable to restore RLIMIT_NOFILE soft limit to {soft_limit}: {exc}"
            )
