import sys
import typing

from abc import ABC, ABCMeta
from dataclasses import dataclass

if sys.version_info < (3, 12):
    from typing_extensions import dataclass_transform
else:
    from typing import dataclass_transform

# All settings classes are dataclasses
# https://rednafi.github.io/digressions/python/2020/06/26/python-metaclasses.html
#  see -- #avoiding-dataclass-decorator-with-metaclasses


@dataclass_transform()
class SettingsMeta(ABCMeta):
    """
    Metaclass that automatically applies dataclass decorator to Settings classes.

    This metaclass ensures all Settings subclasses are automatically converted
    to frozen dataclasses, providing immutability and proper initialization.
    """

    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        classdict: dict[str, typing.Any],
        **kwargs: typing.Any,
    ) -> type["Settings"]:
        """
        Create a new Settings class with dataclass transformation.

        :param name: Name of the class being created.
        :type name: str
        :param bases: Base classes for the new class.
        :type bases: tuple[type, ...]
        :param classdict: Class namespace dictionary.
        :type classdict: dict[str, typing.Any]
        :param kwargs: Additional keyword arguments.
        :return: New Settings class with dataclass applied.
        :rtype: type[Settings]
        """
        new_cls = super().__new__(cls, name, bases, classdict)
        return dataclass(frozen=True)(new_cls)  # type: ignore


class Settings(ABC, metaclass=SettingsMeta):
    """
    To pass parameters into a :obj:`Component`, inherit from ``Settings``.

    .. code-block:: python

       class YourSettings(Settings):
          setting1: int
          setting2: float

    To use, declare the ``Settings`` object for a ``Component`` as a member variable called (all-caps!) ``SETTINGS``. ezmsg will monitor the variable called ``SETTINGS`` in the background, so it is important to name it correctly.

    .. code-block:: python

       class YourUnit(Unit):

          SETTINGS = YourSettings

    A ``Unit`` can accept a ``Settings`` object as a parameter on instantiation.

    .. code-block:: python

       class YourCollection(Collection):

          YOUR_UNIT = YourUnit(
             YourSettings(
                setting1: int,
                setting2: float
             )
          )

    .. note::
       ``Settings`` uses type hints to define member variables, but does not enforce type checking.

    """

    ...
