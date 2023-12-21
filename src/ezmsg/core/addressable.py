from typing import List, Optional

SEPARATOR = "/"


class Addressable:
    _name: Optional[str]
    _location: Optional[List[str]]

    def __init__(self) -> None:
        self._name = None
        self._location = None

    def _set_name(self, name: Optional[str] = None) -> None:
        if name is None:
            name = self.__class__.__name__
        assert self._name is None
        self._name = name

    def _set_location(self, location: Optional[List[str]] = None):
        if location is None:
            location = []
        assert self._location is None
        self._location = location

    @property
    def name(self) -> str:
        if self._name is None:
            raise AssertionError
        return self._name

    @property
    def location(self) -> List[str]:
        if self._location is None:
            raise AssertionError
        return self._location

    @property
    def address(self) -> str:
        return "/".join(self.location + [self.name])
