import asyncio
import pytest

from ezmsg.core.graphserver import GraphService
from ezmsg.core import util


@pytest.mark.asyncio
async def test_invalid_name() -> None:
    service = GraphService()
    server = service.create_server()

    with pytest.raises(ValueError):
        await service.attach_shm("JERRY")

    server.stop()


@pytest.mark.asyncio
async def test_rw() -> None:
    service = GraphService()
    server = service.create_server()

    shm = await service.create_shm(4, 2**16)
    attach_shm = await service.attach_shm(shm.name)

    content = b"HELLO"
    with attach_shm.buffer(0) as mem:
        mem[0 : len(content)] = content[:]

    with shm.buffer(0, readonly=True) as ro_mem:
        assert content == ro_mem[0 : len(content)]

    shm.close()
    await shm.wait_closed()
    attach_shm.close()
    await attach_shm.wait_closed()

    server.stop()


@pytest.mark.asyncio
async def test_shm_detach_order() -> None:
    service = GraphService()
    server = service.create_server()

    shm = await service.create_shm(4, 2**16)
    attach_shm = await service.attach_shm(shm.name)

    content = b"HELLO"
    with attach_shm.buffer(0) as mem:
        mem[0 : len(content)] = content[:]

    attach_shm.close()
    await attach_shm.wait_closed()

    with shm.buffer(0) as mem:
        assert content == mem[0 : len(content)]

    shm.close()
    await shm.wait_closed()

    # Close created SHM first
    shm = await service.create_shm(4, 2**16)
    attach_shm = await service.attach_shm(shm.name)

    content = b"BONJOUR"
    with shm.buffer(0) as mem:
        mem[0 : len(content)] = content[:]

    shm.close()
    await shm.wait_closed()

    with attach_shm.buffer(0) as mem:
        assert content == mem[0 : len(content)]

    attach_shm.close()
    await attach_shm.wait_closed()

    server.stop()


@pytest.mark.asyncio
async def test_shutdown() -> None:
    service = GraphService()
    server = service.create_server()

    shm = await service.create_shm(4, 2**16)
    attach_shm = await service.attach_shm(shm.name)

    content = b"HELLO"
    with shm.buffer(0) as mem:
        mem[0 : len(content)] = content[:]

    server.stop()
    await asyncio.sleep(0.1)

    with pytest.raises(BufferError):
        with attach_shm.buffer(0) as mem:
            assert content == mem[0 : len(content)]


@pytest.mark.skipif(
    util.resource is None or not hasattr(util.resource, "RLIMIT_NOFILE"),
    reason="RLIMIT_NOFILE unsupported on this platform",
)
def test_elevated_fd_limit_raises_and_restores(monkeypatch):
    calls = []

    class FakeResource:
        RLIMIT_NOFILE = object()

        def __init__(self):
            self.soft = 256
            self.hard = 4096

        def getrlimit(self, which):
            assert which is self.RLIMIT_NOFILE
            return (self.soft, self.hard)

        def setrlimit(self, which, limits):
            assert which is self.RLIMIT_NOFILE
            calls.append(limits)
            self.soft, self.hard = limits

    fake = FakeResource()
    monkeypatch.setattr(util, "resource", fake)
    monkeypatch.setenv("EZMSG_FD_LIMIT", "1024")

    with util.elevated_fd_limit():
        assert fake.soft == 1024

    assert fake.soft == 256
    assert calls == [(1024, 4096), (256, 4096)]


@pytest.mark.skipif(
    util.resource is None or not hasattr(util.resource, "RLIMIT_NOFILE"),
    reason="RLIMIT_NOFILE unsupported on this platform",
)
def test_elevated_fd_limit_uses_safe_config(monkeypatch):
    calls = []

    class FakeResource:
        RLIMIT_NOFILE = object()

        def __init__(self):
            self.soft = 512
            self.hard = 2048

        def getrlimit(self, which):
            return (self.soft, self.hard)

        def setrlimit(self, which, limits):
            calls.append(limits)
            self.soft, self.hard = limits

    fake = FakeResource()
    monkeypatch.setattr(util, "resource", fake)
    monkeypatch.setenv("EZMSG_FD_LIMIT", "bogus")

    with util.elevated_fd_limit():
        assert fake.soft == 2048

    assert calls == [(2048, 2048), (512, 2048)]


def test_runtime_entrypoints_wrap_fd_limit(monkeypatch):
    from ezmsg.core.backend import GraphRunner

    calls = []

    class DummyContext:
        def __enter__(self):
            calls.append("enter")

        def __exit__(self, exc_type, exc, tb):
            calls.append("exit")

    monkeypatch.setattr("ezmsg.core.graphserver.elevated_fd_limit", lambda: DummyContext())
    monkeypatch.setattr("ezmsg.core.backend.elevated_fd_limit", lambda: DummyContext())

    class DummyServer:
        address = ("127.0.0.1", 1234)

        def __init__(self, name):
            assert name == "GraphServer"

        def start(self, address):
            return None

    monkeypatch.setattr("ezmsg.core.graphserver.GraphServer", DummyServer)

    service = GraphService()
    service.create_server()

    runner = GraphRunner(components={"SYSTEM": object()})
    monkeypatch.setattr(runner, "_initialize", lambda **kwargs: False)
    runner.start()

    runner = GraphRunner(components={"SYSTEM": object()})
    monkeypatch.setattr(runner, "_initialize", lambda **kwargs: False)
    runner.run_blocking()

    assert calls == ["enter", "exit", "enter", "exit", "enter", "exit"]


if __name__ == "__main__":
    asyncio.run(test_invalid_name())
    asyncio.run(test_rw())
    asyncio.run(test_shm_detach_order())
    asyncio.run(test_shutdown())
