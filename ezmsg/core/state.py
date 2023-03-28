from abc import ABC, ABCMeta
from dataclasses import dataclass

from typing import (
    Dict,
    Tuple,
    Any,
    Type,
)


class StateMeta(ABCMeta):
    def __new__(
        cls,
        name: str,
        bases: Tuple[type, ...],
        classdict: Dict[str, Any],
        **kwargs: Any
    ) -> Type["State"]:
        new_cls = super().__new__(cls, name, bases, classdict)
        return dataclass(unsafe_hash=True, frozen=False, init=False)(new_cls)  # type: ignore


class State(ABC, metaclass=StateMeta):
    """
    States are mutable dataclasses that are instantiated by the Unit in its home process.
    """

    ...
