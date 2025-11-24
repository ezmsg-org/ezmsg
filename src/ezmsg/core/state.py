from abc import ABC, ABCMeta
from dataclasses import dataclass

from typing import Any


class StateMeta(ABCMeta):
    """
    Metaclass that automatically applies dataclass decorator to State classes.

    This metaclass ensures all State subclasses are automatically converted
    to mutable dataclasses with hash support but no automatic initialization.
    """

    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        classdict: dict[str, Any],
        **kwargs: Any,
    ) -> type["State"]:
        """
        Create a new State class with dataclass transformation.

        :param name: Name of the class being created.
        :type name: str
        :param bases: Base classes for the new class.
        :type bases: tuple[type, ...]
        :param classdict: Class namespace dictionary.
        :type classdict: dict[str, Any]
        :param kwargs: Additional keyword arguments.
        :return: New State class with dataclass applied.
        :rtype: type[State]
        """
        new_cls = super().__new__(cls, name, bases, classdict)
        return dataclass(unsafe_hash=True, frozen=False, init=False)(new_cls)  # type: ignore


class State(ABC, metaclass=StateMeta):
    """
    States are mutable dataclasses that are instantiated by the Unit in its home process.

    To track a mutable state for a ``Component``, inherit from ``State``.

    .. code-block:: python

       class YourState(State):
          state1: int
          state2: float

    To use, declare the ``State`` object for a ``Component`` as a member variable called (all-caps!) ``STATE``.
    ezmsg will monitor the variable called ``STATE`` in the background, so it is important to name it correctly.

    Member functions can then access and mutate ``STATE`` as needed during function execution.
    It is recommended to initialize state values inside the ``initialize()`` or ``configure()`` lifecycle hooks if
    defaults are not defined.

    .. code-block:: python

       class YourUnit(Unit):

          STATE = YourState

          async def initialize(self):
             this.STATE.state1 = 0
             this.STATE.state2 = 0.0

    .. note::
       ``State`` uses type hints to define member variables, but does not enforce type checking.
    """

    ...
