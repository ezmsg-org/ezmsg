from typing import Type

from .addressable import Addressable


class Stream(Addressable):
    msg_type: Type

    def __init__(self, msg_type: Type):
        super().__init__()
        self.msg_type = msg_type


class InputStream(Stream):
    ...


class OutputStream(Stream):
    ...
