import typing
from copy import deepcopy

from .stream import Stream
from .component import ComponentMeta, Component
from .settings import Settings


NetworkDefinition = typing.Iterable[typing.Tuple[typing.Union[Stream, str], typing.Union[Stream, str]]]


class CollectionMeta(ComponentMeta):
    def __init__(
        cls, 
        name: str, 
        bases: typing.Tuple[type, ...], 
        fields: typing.Dict[str, typing.Any], 
        **kwargs: typing.Any
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
    """Collections can contain subunits and connect them together"""

    def __init__(self, settings: typing.Optional[Settings] = None):
        super(Collection, self).__init__(settings)

        self._components = deepcopy(self.__class__.__components__)
        for comp_name, comp in self.components.items():
            setattr(self, comp_name, comp)

    def configure(self) -> None:
        """This is where to percolate apply_settings to subnodes"""
        ...

    def network(self) -> NetworkDefinition:
        return ()

    def process_components(self) -> typing.Collection[Component]:
        return (self,)
