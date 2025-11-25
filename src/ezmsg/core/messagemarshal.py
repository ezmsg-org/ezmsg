import pickle
from contextlib import contextmanager

from .netprotocol import uint64_to_bytes, bytes_to_uint, UINT64_SIZE

from collections.abc import Generator
from typing import Any

_PREAMBLE = b"EZ"
_PREAMBLE_LEN = len(_PREAMBLE)
NO_MESSAGE = _PREAMBLE + (b"\xff" * 8) + (b"\x00" * 8)


class UndersizedMemory(Exception):
    """
    Exception raised when target memory buffer is too small for serialization.

    Contains the required size needed to successfully serialize the object.
    """

    req_size: int

    def __init__(self, *args: object, req_size: int = 0) -> None:
        """
        Initialize UndersizedMemory exception.

        :param args: Exception arguments.
        :param req_size: Required memory size in bytes.
        :type req_size: int
        """
        super().__init__(*args)
        self.req_size = req_size


class UninitializedMemory(Exception):
    """
    Exception raised when attempting to read from uninitialized memory.

    This occurs when trying to deserialize from a memory buffer that
    doesn't contain valid serialized data with the expected preamble.
    """

    ...


class Marshal:
    """
    Codec for byte-level representations of objects.

    This base namespace defines a marshal that uses pickle to dump
    objects into a series of memoryviews for serialization.

    It may be that serialization to a cross-platform/language format
    is desirable in the future; in which case, this class can be
    sub-classed and the dump/load functions can be overloaded.
    """

    @classmethod
    def to_mem(cls, msg_id: int, obj: Any, mem: memoryview) -> None:
        """
        Serialize an object with message ID into a memory buffer.

        :param msg_id: Unique message identifier.
        :type msg_id: int
        :param obj: Object to serialize.
        :type obj: Any
        :param mem: Target memory buffer.
        :type mem: memoryview
        :raises UndersizedMemory: If memory buffer is too small.
        """
        with cls.serialize(msg_id, obj) as ser_obj:
            total_size, header, buffers = ser_obj

            if total_size >= len(mem):
                raise UndersizedMemory(req_size=total_size)

            cls._write(mem, header, buffers)

    @classmethod
    def _write(cls, mem: memoryview, header: bytes, buffers: list[memoryview]):
        sidx = len(header)
        mem[:sidx] = header[:]
        for buf in buffers:
            blen = len(buf)
            mem[sidx : sidx + blen] = buf[:]
            sidx += blen

    @classmethod
    def _assert_initialized(cls, raw: memoryview | bytes) -> None:
        if raw[:_PREAMBLE_LEN] != _PREAMBLE:
            raise UninitializedMemory

    @classmethod
    def msg_id(cls, raw: memoryview | bytes) -> int:
        """
        Get msg_id from a buffer; if uninitialized, return None.

        :param mem: buffer to read from.
        :type mem: memoryview | bytes
        :return: Message ID of encoded message
        :rtype: int
        :raises UninitializedMemory: If buffer is not initialized.
        """
        cls._assert_initialized(raw)
        return bytes_to_uint(raw[_PREAMBLE_LEN : _PREAMBLE_LEN + UINT64_SIZE])

    @classmethod
    @contextmanager
    def obj_from_mem(cls, mem: memoryview) -> Generator[Any, None, None]:
        """
        Deserialize an object from a memory buffer.

        Provides a context manager for safe access to deserialized objects
        with automatic cleanup of memory views.

        :param mem: Memory buffer containing serialized object.
        :type mem: memoryview
        :return: Context manager yielding the deserialized object.
        :rtype: Generator[Any, None, None]
        :raises UninitializedMemory: If memory buffer is not properly initialized.
        """
        cls._assert_initialized(mem)

        sidx = _PREAMBLE_LEN + UINT64_SIZE
        num_buffers = bytes_to_uint(mem[sidx : sidx + UINT64_SIZE])
        if num_buffers == 0:
            raise ValueError("invalid message in memory")

        sidx += UINT64_SIZE
        buf_sizes = [0] * num_buffers
        for i in range(num_buffers):
            buf_sizes[i] = bytes_to_uint(mem[sidx : sidx + UINT64_SIZE])
            sidx += UINT64_SIZE

        buffers: list[memoryview] = list()
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
    ) -> Generator[tuple[int, bytes, list[memoryview]], None, None]:
        """
        Serialize an object for network transmission.

        Creates a complete serialization package with header and buffers
        suitable for network transmission.

        :param msg_id: Unique message identifier.
        :type msg_id: int
        :param obj: Object to serialize.
        :type obj: Any
        :return: Context manager yielding (total_size, header, buffers) tuple.
        :rtype: Generator[tuple[int, bytes, list[memoryview]], None, None]
        """
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
    def dump(obj: Any) -> list[memoryview]:
        """
        Serialize an object to a list of memory buffers using pickle.

        :param obj: Object to serialize.
        :type obj: Any
        :return: List of memory views containing serialized data.
        :rtype: list[memoryview]
        """
        obj_buffers: list[pickle.PickleBuffer] = list()
        ser_obj = pickle.dumps(obj, protocol=5, buffer_callback=obj_buffers.append)
        buffers = [memoryview(ser_obj)] + [b.raw() for b in obj_buffers]
        return buffers

    @staticmethod
    def load(buffers: list[memoryview]) -> Any:
        """
        Deserialize an object from a list of memory buffers using pickle.

        :param buffers: List of memory views containing serialized data.
        :type buffers: list[memoryview]
        :return: Deserialized object.
        :rtype: Any
        """
        return pickle.loads(buffers[0], buffers=buffers[1:])

    @classmethod
    def copy_obj(cls, from_mem: memoryview, to_mem: memoryview) -> None:
        """
        Copy obj in from_mem (if initialized) to to_mem.

        :param from_mem: Source memory buffer containing serialized object.
        :type from_mem: memoryview
        :param to_mem: Target memory buffer for copying.
        :type to_mem: memoryview
        :raises UninitializedMemory: If from_mem buffer is not properly initialized.
        """
        msg_id = cls.msg_id(from_mem)
        with MessageMarshal.obj_from_mem(from_mem) as obj:
            MessageMarshal.to_mem(msg_id, obj, to_mem)


# If some other byte-level representation is desired, you can just
# monkeypatch the module at runtime with a different Marhsal subclass
# TODO: This could also be done with environment variables
MessageMarshal = Marshal
