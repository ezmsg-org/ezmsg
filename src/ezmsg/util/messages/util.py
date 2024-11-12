from dataclasses import replace as slow_replace
import os
import typing


T = typing.TypeVar("T")


def fast_replace(arr: typing.Generic[T], **kwargs) -> T:
    out_kwargs = arr.__dict__.copy()  # Shallow copy
    out_kwargs.update(**kwargs)
    return arr.__class__(**out_kwargs)


replace = slow_replace if "EZMSG_DISABLE_FAST_REPLACE" in os.environ else fast_replace
