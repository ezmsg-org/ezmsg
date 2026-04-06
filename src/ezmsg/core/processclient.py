import asyncio
import logging
import os
import pickle
import socket
import time

from uuid import UUID
from contextlib import suppress
from collections.abc import Awaitable, Callable

from .graphmeta import (
    ProcessProfilingSnapshot,
    ProcessProfilingTraceBatch,
    ProcessControlErrorCode,
    ProcessControlOperation,
    ProcessControlRequest,
    ProcessControlResponse,
    ProfilingTraceControl,
    ProcessPing,
    ProcessRegistration,
    ProcessStats,
    ProcessOwnershipUpdate,
    ProcessSettingsUpdate,
    SettingsSnapshotValue,
)
from .profiling import PROFILES
from .graphserver import GraphService
from .netprotocol import (
    AddressType,
    Command,
    close_stream_writer,
    read_int,
    read_str,
    uint64_to_bytes,
)

logger = logging.getLogger("ezmsg")


class ProcessControlClient:
    _graph_address: AddressType | None
    _client_id: UUID | None
    _reader: asyncio.StreamReader | None
    _writer: asyncio.StreamWriter | None
    _write_lock: asyncio.Lock
    _ack_queue: asyncio.Queue[bytes]
    _io_task: asyncio.Task[None] | None
    _request_handler: Callable[
        [ProcessControlRequest], ProcessControlResponse | Awaitable[ProcessControlResponse]
    ] | None
    _owned_units: set[str]
    _trace_push_task: asyncio.Task[None] | None
    _trace_push_interval_s: float
    _trace_push_max_samples: int

    def __init__(self, graph_address: AddressType | None = None) -> None:
        self._graph_address = graph_address
        self._client_id = None
        self._reader = None
        self._writer = None
        self._write_lock = asyncio.Lock()
        self._ack_queue = asyncio.Queue()
        self._io_task = None
        self._request_handler = None
        self._owned_units = set()
        self._trace_push_task = None
        self._trace_push_interval_s = float(
            os.environ.get("EZMSG_PROFILE_TRACE_PUSH_INTERVAL_S", "0.02")
        )
        self._trace_push_max_samples = int(
            os.environ.get("EZMSG_PROFILE_TRACE_PUSH_MAX_SAMPLES", "5000")
        )

    def _require_client_id(self) -> UUID:
        if self._client_id is None:
            raise RuntimeError("Process control connection is not active")
        return self._client_id

    @property
    def process_id(self) -> UUID:
        return self._require_client_id()

    @property
    def client_id(self) -> UUID | None:
        return self._client_id

    async def connect(self) -> None:
        if self._writer is not None:
            return

        reader, writer = await GraphService(self._graph_address).open_connection()
        writer.write(Command.PROCESS.value)
        await writer.drain()

        client_id = UUID(await read_str(reader))
        response = await reader.read(1)
        if response != Command.COMPLETE.value:
            await close_stream_writer(writer)
            raise RuntimeError("Failed to create process control connection")

        self._client_id = client_id
        PROFILES.set_process_id(client_id)
        self._reader = reader
        self._writer = writer
        self._io_task = asyncio.create_task(
            self._io_loop(),
            name=f"process-control-{client_id}",
        )

    def set_request_handler(
        self,
        handler: Callable[
            [ProcessControlRequest], ProcessControlResponse | Awaitable[ProcessControlResponse]
        ]
        | None,
    ) -> None:
        self._request_handler = handler

    async def register(self, units: list[str]) -> None:
        await self.connect()
        normalized_units = sorted(set(units))
        payload = ProcessRegistration(
            pid=os.getpid(),
            host=socket.gethostname(),
            units=normalized_units,
        )
        await self._payload_command(Command.PROCESS_REGISTER, payload)
        self._owned_units = set(normalized_units)

    async def update_ownership(
        self,
        added_units: list[str] | None = None,
        removed_units: list[str] | None = None,
    ) -> None:
        await self.connect()
        added = sorted(set(added_units or []))
        removed = sorted(set(removed_units or []))
        payload = ProcessOwnershipUpdate(
            added_units=added,
            removed_units=removed,
        )
        await self._payload_command(Command.PROCESS_UPDATE_OWNERSHIP, payload)
        self._owned_units.update(added)
        self._owned_units.difference_update(removed)

    async def report_settings_update(
        self,
        component_address: str,
        value: SettingsSnapshotValue,
        timestamp: float | None = None,
    ) -> None:
        await self.connect()
        payload = ProcessSettingsUpdate(
            component_address=component_address,
            value=value,
            timestamp=timestamp if timestamp is not None else time.time(),
        )
        await self._payload_command(Command.PROCESS_SETTINGS_UPDATE, payload)

    async def close(self) -> None:
        writer = self._writer
        if writer is None:
            return

        trace_task = self._trace_push_task
        self._trace_push_task = None
        if trace_task is not None:
            trace_task.cancel()
            with suppress(asyncio.CancelledError):
                await trace_task

        io_task = self._io_task
        self._io_task = None
        if io_task is not None:
            io_task.cancel()
            with suppress(asyncio.CancelledError):
                await io_task

        self._reader = None
        self._writer = None
        self._client_id = None
        await close_stream_writer(writer)

    async def _payload_command(self, command: Command, payload_obj: object) -> None:
        await self._write_payload(command, payload_obj, expect_complete=True)

    async def _write_payload(
        self,
        command: Command,
        payload_obj: object,
        *,
        expect_complete: bool,
    ) -> None:
        reader = self._reader
        writer = self._writer
        if reader is None or writer is None:
            raise RuntimeError("Process control connection is not active")

        payload = pickle.dumps(payload_obj)
        async with self._write_lock:
            writer.write(command.value)
            writer.write(uint64_to_bytes(len(payload)))
            writer.write(payload)
            await writer.drain()

        if not expect_complete:
            return

        try:
            response = await asyncio.wait_for(self._ack_queue.get(), timeout=5.0)
        except asyncio.TimeoutError as exc:
            raise RuntimeError(
                f"Timed out waiting for response to process control command: {command.name}"
            ) from exc

        if response != Command.COMPLETE.value:
            if response == Command.ERROR.value:
                raise RuntimeError(
                    f"Process control command failed: {command.name}"
                )
            raise RuntimeError(
                f"Unexpected response to process control command: {command.name}"
            )

    async def _io_loop(self) -> None:
        reader = self._reader
        writer = self._writer
        if reader is None or writer is None:
            return

        try:
            while True:
                req = await reader.read(1)
                if not req:
                    break

                if req in (Command.COMPLETE.value, Command.ERROR.value):
                    self._ack_queue.put_nowait(req)
                    continue

                if req != Command.PROCESS_ROUTE_REQUEST.value:
                    logger.warning(
                        "Process control %s received unknown command: %s",
                        self._client_id,
                        req,
                    )
                    continue

                payload_size = await read_int(reader)
                payload = await reader.readexactly(payload_size)
                request: ProcessControlRequest | None = None
                try:
                    request_obj = pickle.loads(payload)
                    if isinstance(request_obj, ProcessControlRequest):
                        request = request_obj
                    else:
                        raise RuntimeError(
                            "process route request payload was not ProcessControlRequest"
                        )
                except Exception as exc:
                    logger.warning(
                        "Process control %s failed to parse route request: %s",
                        self._client_id,
                        exc,
                    )

                if request is None:
                    continue

                response = await self._handle_route_request(request)
                await self._write_payload(
                    Command.PROCESS_ROUTE_RESPONSE,
                    response,
                    expect_complete=False,
                )

        except asyncio.CancelledError:
            raise
        except (ConnectionResetError, BrokenPipeError) as exc:
            logger.debug(f"Process control {self._client_id} disconnected: {exc}")

    async def _handle_route_request(
        self, request: ProcessControlRequest
    ) -> ProcessControlResponse:
        operation: ProcessControlOperation | None = None
        if isinstance(request.operation, ProcessControlOperation):
            operation = request.operation
        elif isinstance(request.operation, str):
            with suppress(ValueError):
                operation = ProcessControlOperation(request.operation)

        if operation == ProcessControlOperation.PING:
            return ProcessControlResponse(
                request_id=request.request_id,
                ok=True,
                payload=pickle.dumps(
                    ProcessPing(
                        process_id=self.process_id,
                        pid=os.getpid(),
                        host=socket.gethostname(),
                        timestamp=time.time(),
                    )
                ),
                process_id=self.process_id,
            )

        if operation == ProcessControlOperation.GET_PROCESS_STATS:
            return ProcessControlResponse(
                request_id=request.request_id,
                ok=True,
                payload=pickle.dumps(
                    ProcessStats(
                        process_id=self.process_id,
                        pid=os.getpid(),
                        host=socket.gethostname(),
                        owned_units=sorted(self._owned_units),
                        timestamp=time.time(),
                    )
                ),
                process_id=self.process_id,
            )

        if operation == ProcessControlOperation.GET_PROFILING_SNAPSHOT:
            snapshot: ProcessProfilingSnapshot = PROFILES.snapshot()
            return ProcessControlResponse(
                request_id=request.request_id,
                ok=True,
                payload=pickle.dumps(snapshot),
                process_id=self.process_id,
            )

        if operation == ProcessControlOperation.SET_PROFILING_TRACE:
            control: ProfilingTraceControl | None = None
            try:
                if request.payload is not None:
                    control_obj = pickle.loads(request.payload)
                    if isinstance(control_obj, ProfilingTraceControl):
                        control = control_obj
            except Exception as exc:
                return ProcessControlResponse(
                    request_id=request.request_id,
                    ok=False,
                    error=f"Invalid profiling trace control payload: {exc}",
                    error_code=ProcessControlErrorCode.INVALID_RESPONSE,
                    process_id=self.process_id,
                )

            if control is None:
                return ProcessControlResponse(
                    request_id=request.request_id,
                    ok=False,
                    error="Missing profiling trace control payload",
                    error_code=ProcessControlErrorCode.INVALID_RESPONSE,
                    process_id=self.process_id,
                )

            PROFILES.set_trace_control(control)
            if control.enabled:
                await self._ensure_trace_push_task()
            else:
                await self._cancel_trace_push_task()

            return ProcessControlResponse(
                request_id=request.request_id,
                ok=True,
                process_id=self.process_id,
            )

        if operation == ProcessControlOperation.GET_PROFILING_TRACE_BATCH:
            max_samples = 1000
            if request.payload is not None:
                try:
                    max_samples_obj = pickle.loads(request.payload)
                    if isinstance(max_samples_obj, int):
                        max_samples = max(1, max_samples_obj)
                except Exception:
                    pass

            batch: ProcessProfilingTraceBatch = PROFILES.trace_batch(
                max_samples=max_samples
            )
            return ProcessControlResponse(
                request_id=request.request_id,
                ok=True,
                payload=pickle.dumps(batch),
                process_id=self.process_id,
            )

        if self._request_handler is None:
            return ProcessControlResponse(
                request_id=request.request_id,
                ok=False,
                error=f"Unsupported process control operation: {request.operation}",
                error_code=ProcessControlErrorCode.HANDLER_NOT_CONFIGURED,
                process_id=self.process_id,
            )

        try:
            result = self._request_handler(request)
            if asyncio.iscoroutine(result):
                result = await result
        except Exception as exc:
            return ProcessControlResponse(
                request_id=request.request_id,
                ok=False,
                error=f"process request handler failed: {exc}",
                error_code=ProcessControlErrorCode.HANDLER_ERROR,
                process_id=self.process_id,
            )

        if not isinstance(result, ProcessControlResponse):
            return ProcessControlResponse(
                request_id=request.request_id,
                ok=False,
                error=(
                    "process request handler returned invalid response type: "
                    f"{type(result).__name__}"
                ),
                error_code=ProcessControlErrorCode.INVALID_RESPONSE,
                process_id=self.process_id,
            )

        if result.request_id != request.request_id:
            result = ProcessControlResponse(
                request_id=request.request_id,
                ok=False,
                error=(
                    "process request handler returned mismatched request_id: "
                    f"{result.request_id}"
                ),
                error_code=ProcessControlErrorCode.INVALID_RESPONSE,
                process_id=self.process_id,
            )

        if result.process_id is None:
            result.process_id = self.process_id

        return result

    async def _ensure_trace_push_task(self) -> None:
        task = self._trace_push_task
        if task is not None and not task.done():
            return
        self._trace_push_task = asyncio.create_task(
            self._trace_push_loop(),
            name=f"proc-trace-push-{self.process_id}",
        )

    async def _cancel_trace_push_task(self) -> None:
        task = self._trace_push_task
        self._trace_push_task = None
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _trace_push_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(max(0.01, self._trace_push_interval_s))
                batch: ProcessProfilingTraceBatch = PROFILES.trace_batch(
                    max_samples=max(1, self._trace_push_max_samples)
                )
                if len(batch.samples) > 0:
                    await self._write_payload(
                        Command.PROCESS_PROFILING_TRACE_UPDATE,
                        batch,
                        expect_complete=False,
                    )

                if not PROFILES.trace_enabled():
                    break
        except asyncio.CancelledError:
            raise
        except (ConnectionResetError, BrokenPipeError):
            logger.debug("Process trace push loop disconnected")
        except Exception as exc:
            logger.warning(f"Process trace push loop failed: {exc}")
        finally:
            if asyncio.current_task() is self._trace_push_task:
                self._trace_push_task = None
