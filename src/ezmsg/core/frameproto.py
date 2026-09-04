"""
Framed :class:`asyncio.Protocol` support for ezmsg's per-message hot path.

The high-level streams API (:func:`asyncio.open_connection`) delivers incoming
bytes by resolving a future and scheduling the reading task, so every message
costs a full event-loop iteration before any work happens. A
:class:`asyncio.Protocol` has ``data_received`` called synchronously from the
transport's read callback, so the message can be handled in that callback
instead. On stock asyncio this is worth roughly 25us per hop.

Only public asyncio API is used here, which keeps uvloop a drop-in replacement
on POSIX: uvloop implements the same ``AbstractEventLoop.create_connection`` /
``create_server`` contract for :class:`asyncio.Protocol`, and under uvloop the
streams path is already as fast, so the same code is optimal on both loops.

A connection runs in two phases:

* **handshake** -- sequential request/response, driven by ``await read_exactly()``
  and friends. One task wakeup per read, which is fine: it happens once.
* **dispatch** -- entered via :meth:`FramedProtocol.start_dispatch`. From then on
  :meth:`FramedProtocol.frames_available` is called synchronously from
  ``data_received`` and consumes whole frames out of :attr:`buffer`.
"""

import asyncio
import logging
import socket

from .netprotocol import UINT64_SIZE, BYTEORDER

logger = logging.getLogger("ezmsg")


class FramedProtocol(asyncio.Protocol):
    """
    Base protocol that buffers incoming bytes and supports a handshake phase
    followed by an inline dispatch phase.

    Subclasses implement :meth:`frames_available` to consume complete frames
    from :attr:`buffer` during the dispatch phase.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._transport: asyncio.Transport | None = None
        self._read_waiter: "asyncio.Future[None] | None" = None
        self._need = 0
        self._dispatching = False
        self._closed: "asyncio.Future[None] | None" = None
        self._close_exc: BaseException | None = None

    # ------------------------------------------------------------------ #
    # asyncio.Protocol interface
    # ------------------------------------------------------------------ #

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport  # type: ignore[assignment]
        self._closed = asyncio.get_running_loop().create_future()
        sock = transport.get_extra_info("socket")
        if sock is not None:
            # Notifications are tiny and latency-critical; never coalesce them.
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except (OSError, AttributeError):
                pass

    def data_received(self, data: bytes) -> None:
        self._buffer.extend(data)

        if self._dispatching:
            self._drain_frames()
        else:
            self._wake_reader()

    def eof_received(self) -> bool:
        return False  # let the transport close us

    def connection_lost(self, exc: BaseException | None) -> None:
        self._close_exc = exc
        waiter, self._read_waiter = self._read_waiter, None
        if waiter is not None and not waiter.done():
            waiter.set_exception(
                exc if exc is not None else asyncio.IncompleteReadError(b"", None)
            )
        if self._closed is not None and not self._closed.done():
            self._closed.set_result(None)

    # ------------------------------------------------------------------ #
    # dispatch phase
    # ------------------------------------------------------------------ #

    def start_dispatch(self) -> None:
        """
        Leave the handshake phase. :meth:`frames_available` will be called
        inline from ``data_received`` from now on, starting with whatever is
        already buffered.
        """
        self._dispatching = True
        if self._buffer:
            self._drain_frames()

    def frames_available(self) -> None:
        """
        Consume as many whole frames as :attr:`buffer` holds.

        Called synchronously from the transport's read callback, so it must not
        block and must leave any partial trailing frame in the buffer.
        """
        raise NotImplementedError

    def _drain_frames(self) -> None:
        try:
            self.frames_available()
        except Exception as exc:
            # A raise here would be swallowed by the transport, silently
            # wedging the connection, so surface it and tear down instead.
            logger.exception("%s: error dispatching frame", type(self).__name__)
            self._close_exc = exc
            self.close()

    # ------------------------------------------------------------------ #
    # handshake phase
    # ------------------------------------------------------------------ #

    def _wake_reader(self) -> None:
        waiter = self._read_waiter
        if waiter is not None and not waiter.done() and len(self._buffer) >= self._need:
            self._read_waiter = None
            waiter.set_result(None)

    async def read_exactly(self, n: int) -> bytes:
        """Await exactly ``n`` bytes. Only valid during the handshake phase."""
        while len(self._buffer) < n:
            if self._closed is not None and self._closed.done():
                raise asyncio.IncompleteReadError(bytes(self._buffer), n)
            self._need = n
            self._read_waiter = asyncio.get_running_loop().create_future()
            await self._read_waiter
        out = bytes(self._buffer[:n])
        del self._buffer[:n]
        return out

    async def read_uint64(self) -> int:
        return int.from_bytes(await self.read_exactly(UINT64_SIZE), BYTEORDER)

    async def read_str(self) -> str:
        return (await self.read_exactly(await self.read_uint64())).decode("utf-8")

    # ------------------------------------------------------------------ #
    # misc
    # ------------------------------------------------------------------ #

    @property
    def buffer(self) -> bytearray:
        """Unconsumed bytes. Subclasses consume from the front of this."""
        return self._buffer

    @property
    def transport(self) -> asyncio.Transport:
        assert self._transport is not None, "connection_made has not run"
        return self._transport

    def write(self, data: bytes) -> None:
        if self._transport is not None and not self._transport.is_closing():
            self._transport.write(data)

    def pause_reading(self) -> None:
        if self._transport is not None and not self._transport.is_closing():
            self._transport.pause_reading()

    def resume_reading(self) -> None:
        if self._transport is not None and not self._transport.is_closing():
            self._transport.resume_reading()

    def close(self) -> None:
        if self._transport is not None and not self._transport.is_closing():
            self._transport.close()

    async def wait_closed(self) -> None:
        if self._closed is not None:
            await self._closed
