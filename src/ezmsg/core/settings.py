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
    def __new__(
        cls,
        name: str,
        bases: typing.Tuple[type, ...],
        classdict: typing.Dict[str, typing.Any],
        **kwargs: typing.Any
    ) -> typing.Type["Settings"]:
        new_cls = super().__new__(cls, name, bases, classdict)
        return dataclass(frozen=True)(new_cls)  # type: ignore


class Settings(ABC, metaclass=SettingsMeta):
    ...
