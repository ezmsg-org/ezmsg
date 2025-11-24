from abc import ABCMeta
from copy import deepcopy
from dataclasses import fields, MISSING, is_dataclass

from .settings import Settings
from .state import State
from .addressable import Addressable
from .stream import Stream

from collections.abc import Callable
from typing import Any

import logging

logger = logging.getLogger("ezmsg")


class ComponentMeta(ABCMeta):
    def __init__(
        cls, name: str, bases: tuple[type, ...], fields: dict[str, Any], **kwargs: Any
    ) -> None:
        super(ComponentMeta, cls).__init__(name, bases, fields)

        cls.__streams__ = {}

        if "SETTINGS" in cls.__annotations__:
            settings_cls = cls.__annotations__["SETTINGS"].__name__
            logger.warning(
                f"{name} SETTINGS should be assigned rather than annotated: `SETTINGS = {settings_cls}` -> `SETTINGS = {settings_cls}`"
            )
            fields["SETTINGS"] = cls.__annotations__["SETTINGS"]
        if "STATE" in cls.__annotations__:
            state_cls = cls.__annotations__["STATE"].__name__
            logger.warning(
                f"{name} STATE should be assigned rather than annotated: `STATE = {state_cls}` -> `STATE = {state_cls}`"
            )
            fields["STATE"] = cls.__annotations__["STATE"]

        base_settings_types = []
        base_state_types = []
        for base in bases:
            if hasattr(base, "__streams__"):
                streams = getattr(base, "__streams__")
                for stream_name, stream in streams.items():
                    cls.__streams__[stream_name] = stream
            if hasattr(base, "__settings_type__"):
                base_settings_types.append(base.__settings_type__)
            if hasattr(base, "__state_type__"):
                base_state_types.append(base.__state_type__)

        for field_name, field_value in fields.items():
            if isinstance(field_value, Stream):
                field_value._set_name(field_name)
                cls.__streams__[field_name] = field_value
                continue

            if field_name == "SETTINGS":
                if not issubclass(field_value, Settings):
                    logger.error(
                        f"{name} Settings must be a subclass of `ez.Settings`!"
                    )

                for settings_type in base_settings_types:
                    if not issubclass(field_value, settings_type):
                        logger.error(
                            f"{name} Settings of type {field_value.__name__} must be a subclass of {settings_type.__name__}"
                        )
                cls.__settings_type__ = field_value

            if field_name == "STATE":
                if not issubclass(field_value, State):
                    logger.error(f"{name} State must be a subclass of `ez.State`!")

                for state_type in base_state_types:
                    if not issubclass(field_value, state_type):
                        logger.error(
                            f"{name} State of type {field_value.__name__} must be a subclass of {state_type.__name__}"
                        )
                cls.__state_type__ = field_value

        if not hasattr(cls, "__state_type__"):
            if len(base_state_types) == 1:
                cls.__state_type__ = base_state_types[0]
            elif len(base_state_types) > 1:
                logger.error("More than one state base class inherited!")
            else:
                cls.__state_type__ = State

        if not hasattr(cls, "__settings_type__"):
            if len(base_settings_types) == 1:
                cls.__settings_type__ = base_settings_types[0]
            elif len(base_settings_types) > 1:
                logger.error("More than one settings base class inherited!")
            else:
                cls.__settings_type__ = Settings


class Component(Addressable, metaclass=ComponentMeta):
    """
    Metaclass which :obj:`Unit` and :obj:`Collection` inherit from.

    The Component class provides the foundation for all components in the ezmsg framework,
    including Units and Collections. It manages settings, state, streams, and provides
    the basic infrastructure for message-passing components.

    :param settings: Optional settings object for component configuration
    :type settings: Settings | None

    .. note::

        When creating ezmsg nodes, inherit directly from :obj:`Unit` or :obj:`Collection`.
    """

    _tasks: dict[str, Callable]  # Only Units will have tasks
    _streams: dict[str, Stream]  # All Components can have streams
    _components: dict[str, "Component"]  # Only Collections will have components
    _main: Callable[..., None] | None
    _threads: dict[str, Callable]

    def __init__(self, *args, settings: Settings | None = None, **kwargs):
        super(Component, self).__init__()

        self.SETTINGS = None
        self.STATE = None
        self._settings_applied = False
        self._components = {}
        self._tasks = {}
        self._main = None
        self._threads = {}

        self._streams = deepcopy(self.__class__.__streams__)
        for stream_name, stream in self.streams.items():
            setattr(self, stream_name, stream)

        if settings is None:
            # settings not supplied as a kwarg. Try to build it.
            if len(args) > 0 and isinstance(args[0], self.__class__.__settings_type__):
                settings = args[0]
            elif len(args) > 0 or len(kwargs) > 0:
                settings = self.__class__.__settings_type__(*args, **kwargs)
            else:
                try:
                    # If we weren't supplied settings, we will try to
                    # instantiate the settings type from annotations
                    settings = self.__class__.__settings_type__()
                except TypeError:
                    # We couldn't instantiate settings with default value
                    # We will rely on late configuration via apply_settings
                    pass

        if settings is not None:
            self.apply_settings(settings)

    def _instantiate_state(self) -> None:
        assert self.STATE is None
        self.STATE = self.__class__.__state_type__()
        assert is_dataclass(self.STATE)
        for field in fields(self.STATE):
            if field.default_factory is not MISSING and field.default is MISSING:
                setattr(self.STATE, field.name, field.default_factory())

    def _check_state(self) -> None:
        assert is_dataclass(self.STATE.__class__)
        for field in fields(self.STATE.__class__):
            if not hasattr(self.STATE, field.name):
                raise AttributeError(
                    f"{self.address}: STATE.{field.name} was not initialized!"
                )

    def apply_settings(self, settings: Settings) -> None:
        """
        Update the Component's Settings object.

        This method applies configuration settings to the component. Settings must be
        applied before the component can be properly initialized and used.

        :param settings: An instance of the class-specific Settings
        :type settings: Settings
        """
        self.SETTINGS = settings
        self._settings_applied = True

    def _set_location(self, location: list[str] | None = None):
        super(Component, self)._set_location(location)

        # Percolate the location down to submodules and streams
        for comp in self.components.values():
            comp._set_location(self.location + [self.name])
        for stream in self.streams.values():
            stream._set_location(self.location + [self.name])

    @property
    def tasks(self) -> dict[str, Callable]:
        """
        Get the dictionary of tasks for this component.

        :return: Dictionary mapping task names to their callable functions
        :rtype: dict[str, collections.abc.Callable]
        """
        return self._tasks

    @property
    def streams(self) -> dict[str, Stream]:
        """
        Get the dictionary of streams for this component.

        :return: Dictionary mapping stream names to their Stream objects
        :rtype: dict[str, Stream]
        """
        return self._streams

    @property
    def components(self) -> dict[str, "Component"]:
        """
        Get the dictionary of child components for this component.

        :return: Dictionary mapping component names to their Component objects
        :rtype: dict[str, Component]
        """
        return self._components

    @property
    def main(self) -> Callable[..., None] | None:
        """
        Get the main function for this component.

        :return: The main callable function, or None if not set
        :rtype: collections.abc.Callable[..., None] | None
        """
        return self._main

    @property
    def threads(self) -> dict[str, Callable]:
        """
        Get the dictionary of thread functions for this component.

        :return: Dictionary mapping thread names to their callable functions
        :rtype: dict[str, collections.abc.Callable]
        """
        return self._threads
