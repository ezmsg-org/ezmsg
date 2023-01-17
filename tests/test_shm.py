import asyncio
import pytest

from ezmsg.core.shmserver import SHMContext, SHMServer


@pytest.mark.asyncio
async def test_invalid_name() -> None:
    await SHMServer.ensure_running()

    with pytest.raises(ValueError):
        await SHMContext.attach('JERRY')


@pytest.mark.asyncio
async def test_rw() -> None:

    await SHMServer.ensure_running()

    shm = await SHMContext.create(4, 2**16)
    attach_shm = await SHMContext.attach(shm.name)

    content = b'HELLO'
    with attach_shm.buffer(0) as mem:
        mem[0: len(content)] = content[:]

    with shm.buffer(0, readonly=True) as ro_mem:
        assert content == ro_mem[0: len(content)]

    shm.close()
    await shm.wait_closed()
    attach_shm.close()
    await attach_shm.wait_closed()


@pytest.mark.asyncio
async def test_shm_detach_order() -> None:

    await SHMServer.ensure_running()

    shm = await SHMContext.create(4, 2**16)
    attach_shm = await SHMContext.attach(shm.name)

    content = b'HELLO'
    with attach_shm.buffer(0) as mem:
        mem[0: len(content)] = content[:]

    attach_shm.close()
    await attach_shm.wait_closed()

    with shm.buffer(0) as mem:
        assert content == mem[0: len(content)]

    shm.close()
    await shm.wait_closed()

    # Close created SHM first

    shm = await SHMContext.create(4, 2**16)
    attach_shm = await SHMContext.attach(shm.name)

    content = b'BONJOUR'
    with shm.buffer(0) as mem:
        mem[0: len(content)] = content[:]

    shm.close()
    await shm.wait_closed()

    with attach_shm.buffer(0) as mem:
        assert content == mem[0: len(content)]

    attach_shm.close()
    await attach_shm.wait_closed()


@pytest.mark.asyncio
async def test_shmserver_shutdown() -> None:

    await SHMServer.ensure_running()

    shm = await SHMContext.create(4, 2**16)
    attach_shm = await SHMContext.attach(shm.name)

    content = b'HELLO'
    with shm.buffer(0) as mem:
        mem[0: len(content)] = content[:]

    await SHMServer.shutdown_server()
    await asyncio.sleep(0.1)

    with pytest.raises(BufferError):
        with attach_shm.buffer(0) as mem:
            assert content == mem[0: len(content)]


if __name__ == '__main__':
    asyncio.run(test_invalid_name())
    asyncio.run(test_rw())
    asyncio.run(test_shm_detach_order())
    asyncio.run(test_shmserver_shutdown())
