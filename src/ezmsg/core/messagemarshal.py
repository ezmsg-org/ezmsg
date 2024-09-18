import pickle
from contextlib import contextmanager

from .netprotocol import uint64_to_bytes, bytes_to_uint, UINT64_SIZE

from typing import Any, List, Tuple, Generator, Optional

_PREAMBLE = b"EZ"
_PREAMBLE_LEN = len(_PREAMBLE)


class UndersizedMemory(Exception):
    req_size: int

    def __init__(self, *args: object, req_size: int = 0) -> None:
        super().__init__(*args)
        self.req_size = req_size


class UninitializedMemory(Exception): ...


class Marshal:
    """codec for byte-level representations of objects

    This base namespace defines a marshal that uses pickle to dump
    objects into a series of memoryviews for serialization.

    It may be that serialization to a cross-platform/language format
    is desireable in the future; in which case, this class can be
    sub-classed and the dump/load fuctions can be overloaded.
    """

    @classmethod
    def to_mem(cls, msg_id: int, obj: Any, mem: memoryview) -> None:
        with cls.serialize(msg_id, obj) as ser_obj:
            total_size, header, buffers = ser_obj

            if total_size >= len(mem):
                raise UndersizedMemory(req_size=total_size)

            sidx = len(header)
            mem[:sidx] = header[:]
            for buf in buffers:
                blen = len(buf)
                mem[sidx : sidx + blen] = buf[:]
                sidx += blen

    @classmethod
    def _assert_initialized(cls, mem: memoryview) -> None:
        if mem[:_PREAMBLE_LEN] != _PREAMBLE:
            raise UninitializedMemory

    @classmethod
    def msg_id(cls, mem: memoryview) -> Optional[int]:
        """get msg_id currently written in mem; if uninitialized, return None"""
        try:
            cls._assert_initialized(mem)
            return bytes_to_uint(mem[_PREAMBLE_LEN : _PREAMBLE_LEN + UINT64_SIZE])
        except UninitializedMemory:
            return None

    @classmethod
    @contextmanager
    def obj_from_mem(cls, mem: memoryview) -> Generator[Any, None, None]:
        cls._assert_initialized(mem)

        sidx = _PREAMBLE_LEN + UINT64_SIZE
        num_buffers = bytes_to_uint(mem[sidx : sidx + UINT64_SIZE])
        sidx += UINT64_SIZE
        buf_sizes = [0] * num_buffers
        for i in range(num_buffers):
            buf_sizes[i] = bytes_to_uint(mem[sidx : sidx + UINT64_SIZE])
            sidx += UINT64_SIZE

        buffers: List[memoryview] = list()
        for bsz in buf_sizes:
            buffers.append(mem[sidx : sidx + bsz])
            sidx += bsz

        obj = cls.load(buffers)

        try:
            yield obj
        finally:
            del obj
            for buf in buffers:
                buf.release()

    @classmethod
    @contextmanager
    def serialize(
        cls, msg_id: int, obj: Any
    ) -> Generator[Tuple[int, bytes, List[memoryview]], None, None]:
        buffers = cls.dump(obj)
        header = uint64_to_bytes(len(buffers))
        buf_lengths = [len(buf) for buf in buffers]
        header_chunks = [header] + [uint64_to_bytes(b) for b in buf_lengths]
        header = _PREAMBLE + uint64_to_bytes(msg_id) + b"".join(header_chunks)
        header_len = len(header)
        total_size = header_len + sum(buf_lengths)

        try:
            yield (total_size, header, buffers)
        finally:
            for buffer in buffers:
                buffer.release()

    @staticmethod
    def dump(obj: Any) -> List[memoryview]:
        obj_buffers: List[pickle.PickleBuffer] = list()
        ser_obj = pickle.dumps(obj, protocol=5, buffer_callback=obj_buffers.append)
        buffers = [memoryview(ser_obj)] + [b.raw() for b in obj_buffers]
        return buffers

    @staticmethod
    def load(buffers: List[memoryview]) -> Any:
        return pickle.loads(buffers[0], buffers=buffers[1:])

    @classmethod
    def copy_obj(cls, from_mem: memoryview, to_mem: memoryview) -> None:
        """copy obj in from_mem (if initialized) to to_mem"""
        msg_id = cls.msg_id(from_mem)
        if msg_id is not None:
            with MessageMarshal.obj_from_mem(from_mem) as obj:
                MessageMarshal.to_mem(msg_id, obj, to_mem)


MessageMarshal = Marshal
