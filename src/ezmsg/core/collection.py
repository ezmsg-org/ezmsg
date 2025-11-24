from collections.abc import Iterable
from collections.abc import Collection as AbstractCollection
import typing
from copy import deepcopy

from .stream import Stream
from .component import ComponentMeta, Component
from .settings import Settings


# Iterable of (output_stream, input_stream) pairs defining the network connections
NetworkDefinition = Iterable[tuple[Stream | str, Stream | str]]


class CollectionMeta(ComponentMeta):
    def __init__(
        cls,
        name: str,
        bases: tuple[type, ...],
        fields: dict[str, typing.Any],
        **kwargs: typing.Any,
    ) -> None:
        super(CollectionMeta, cls).__init__(name, bases, fields)

        cls.__components__ = {}

        for base in bases:
            if hasattr(base, "__components__"):
                components = getattr(base, "__components__")
                for component_name, component in components.items():
                    cls.__components__[component_name] = component

        for field_name, field_value in fields.items():
            if isinstance(field_value, Component):
                field_value._set_name(field_name)
                cls.__components__[field_name] = field_value


class Collection(Component, metaclass=CollectionMeta):
    """
    Connects :obj:`Units <Unit>` together by defining a graph which connects OutputStreams to InputStreams.

    Collections are composite components that contain and coordinate multiple Units,
    defining how they communicate through stream connections.

    :param settings: Optional settings object for collection configuration
    :type settings: Settings | None
    """

    def __init__(self, *args, settings: Settings | None = None, **kwargs):
        super(Collection, self).__init__(*args, settings=settings, **kwargs)

        self._components = deepcopy(self.__class__.__components__)
        for comp_name, comp in self.components.items():
            setattr(self, comp_name, comp)

    def configure(self) -> None:
        """
        A lifecycle hook that runs when the Collection is instantiated.

        This is the best place to call ``Unit.apply_settings()`` on each member
        Unit of the Collection. Override this method to perform collection-specific
        configuration of child components.
        """
        ...

    def network(self) -> NetworkDefinition:
        """
        Override this method and have the definition return a NetworkDefinition
        which defines how InputStreams and OutputStreams from member Units will be connected.

        The NetworkDefinition specifies the message routing between components by
        connecting output streams to input streams.

        :return: Network definition specifying stream connections
        :rtype: NetworkDefinition
        """
        return ()

    def process_components(self) -> AbstractCollection[Component]:
        """
        Override this method and have the definition return a tuple which contains
        Units and Collections which should run in their own processes.

        This method allows you to specify which components should be isolated
        in separate processes for performance or isolation requirements.

        :return: Collection of components that should run in separate processes
        :rtype: collections.abc.Collection[Component]
        """
        return (self,)
