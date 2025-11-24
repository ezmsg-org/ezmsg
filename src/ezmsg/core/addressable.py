SEPARATOR = "/"


class Addressable:
    """
    Base class for objects that can be addressed within the ezmsg system.

    Addressable objects have a hierarchical address structure consisting of
    a location path and a name, similar to a filesystem path.
    """

    _name: str | None
    _location: list[str] | None

    def __init__(self) -> None:
        """
        Initialize an Addressable object.

        The name and location are initially None and must be set before
        the object can be properly addressed. This is achieved through
        the ``_set_name()`` and ``_set_location()`` methods.
        """
        self._name = None
        self._location = None

    def _set_name(self, name: str | None = None) -> None:
        if name is None:
            name = self.__class__.__name__
        assert self._name is None
        self._name = name

    def _set_location(self, location: list[str] | None = None):
        if location is None:
            location = []
        assert self._location is None
        self._location = location

    @property
    def name(self) -> str:
        """
        Get the name of this addressable object.

        :return: The object's name
        :rtype: str
        :raises AssertionError: If name has not been set
        """
        if self._name is None:
            raise AssertionError
        return self._name

    @property
    def location(self) -> list[str]:
        """
        Get the location path of this addressable object.

        :return: List of path components representing the object's location
        :rtype: list[str]
        :raises AssertionError: If location has not been set
        """
        if self._location is None:
            raise AssertionError
        return self._location

    @property
    def address(self) -> str:
        """
        Get the full address of this object.

        The address is constructed by joining the location path and name
        with forward slashes, similar to a filesystem path.

        :return: The full address string
        :rtype: str
        """
        return "/".join(self.location + [self.name])
