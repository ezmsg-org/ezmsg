import warnings

from abc import ABC, ABCMeta
from dataclasses import dataclass

from typing import Any

# All message classes are dataclasses
# https://rednafi.github.io/digressions/python/2020/06/26/python-metaclasses.html
#  see -- #avoiding-dataclass-decorator-with-metaclasses


class MessageMeta(ABCMeta):
    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        classdict: dict[str, Any],
        **kwargs: Any,
    ) -> type["Message"]:
        new_cls = super().__new__(cls, name, bases, classdict)
        return dataclass(unsafe_hash=True, frozen=True)(new_cls)  # type: ignore


class Message(ABC, metaclass=MessageMeta):
    """
    Deprecated base class for messages in the ezmsg framework.

    .. deprecated::
       Message is deprecated. Use @dataclass decorators instead of inheriting
       from ez.Message. For data arrays, use :obj:`ezmsg.util.messages.AxisArray`.

    .. note::
       This class will issue a DeprecationWarning when instantiated.
    """

    def __init__(self):
        warnings.warn(
            "Message is deprecated. Replace ez.Message with @dataclass decorators",
            DeprecationWarning,
            stacklevel=2,
        )


@dataclass
class Flag:
    """
    A message with no contents.

    Flag is used as a simple signal message that carries no data,
    typically used for synchronization or simple event notification.
    """

    ...
