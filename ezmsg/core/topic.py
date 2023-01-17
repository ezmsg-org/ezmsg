from .addressable import Addressable


class Topic(Addressable):
    def __repr__(self) -> str:
        return f'Topic:{self.address}'


class InputTopic(Topic):
    def __repr__(self) -> str:
        return f'Input{super().__repr__()}()'


class OutputTopic(Topic):
    def __repr__(self) -> str:
        return f'Output{super().__repr__()}'
