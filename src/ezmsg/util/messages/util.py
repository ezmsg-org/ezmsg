from dataclasses import replace as slow_replace
import os
from typing import Any, TypeVar


T = TypeVar("T")


def fast_replace(arr: T, **kwargs: Any) -> T:
    """
    Fast replacement of dataclass fields with reduced safety.

    Unlike dataclasses.replace, this function does not check for type compatibility,
    nor does it check that the passed in fields are valid fields for the dataclass
    and not flagged as init=False.

    BEWARE: This function is not type safe and may lead to runtime errors if
    used incorrectly. It implicitly assumes arr has a __dict__ attribute and
    that kwargs are valid init parameters for the dataclass of arr.

    User code may choose to use this replace or the legacy replace according to their needs.
    To force ezmsg to use the legacy replace, set the environment variable:
    EZMSG_DISABLE_FAST_REPLACE
    Unset the variable to use this replace function.

    :param arr: The dataclass instance to create a modified copy of.
    :type arr: T
    :param kwargs: Field values to update in the new instance.
    :return: A new instance of the same type with updated field values.
    :rtype: T
    """
    out_kwargs = arr.__dict__.copy()  # Shallow copy
    out_kwargs.update(kwargs)
    return arr.__class__(**out_kwargs)


replace = slow_replace if "EZMSG_DISABLE_FAST_REPLACE" in os.environ else fast_replace
