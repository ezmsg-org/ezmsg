from copy import deepcopy

from .stream import Stream
from .topic import Topic
from .component import ComponentMeta, Component
from .settings import Settings

from typing import (
    Any,
    Dict,
    Iterable,
    Tuple,
    Optional,
    Union
)


NetworkDefinition = Iterable[Tuple[Union[Stream, str], Union[Stream, str]]]


class CollectionMeta(ComponentMeta):

    __components__: Dict[str, Component]
    __topics__: Dict[str, Topic]

    def __init__(
        cls, name: str, bases: Tuple[type, ...], fields: Dict[str, Any], **kwargs: Any
    ) -> None:

        super(CollectionMeta, cls).__init__(name, bases, fields)

        cls.__components__ = {}

        for base in bases:
            if hasattr(base, "__components__"):
                components: Dict[str, Component] = getattr(base, "__components__")
                for component_name, component in components.items():
                    cls.__components__[component_name] = component

        for field_name, field_value in fields.items():
            if isinstance(field_value, Component):
                field_value._set_name(field_name)
                cls.__components__[field_name] = field_value

        cls.__topics__ = {}

        for base in bases:
            if hasattr(base, "__topics__"):
                topics: Dict[str, Topic] = getattr(base, "__topics__")
                for topic_name, topic in topics.items():
                    cls.__topics__[topic_name] = topic

        for field_name, field_value in fields.items():
            if isinstance(field_value, Topic):
                field_value._set_name(field_name)
                cls.__topics__[field_name] = field_value


class Collection(Component, metaclass=CollectionMeta):
    """Collections can contain subunits and connect them together"""

    _topics: Dict[str, Topic]  # Only Collections can have Topics

    def __init__(self, settings: Optional[Settings] = None):
        super(Collection, self).__init__(settings)

        self._components = deepcopy(self.__class__.__components__)
        for comp_name, comp in self.components.items():
            setattr(self, comp_name, comp)

        self._topics = deepcopy(self.__class__.__topics__)
        for stream_name, stream in self.topics.items():
            setattr(self, stream_name, stream)

    @property
    def topics(self) -> Dict[str, Topic]:
        return self._topics

    def configure(self) -> None:
        """This is where to percolate apply_settings to subnodes"""
        ...

    def network(self) -> NetworkDefinition:
        return ()

    def process_components(self) -> Tuple[Component, ...]:
        return (self,)
