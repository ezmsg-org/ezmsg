import numpy as np
from dataclasses import dataclass
from multiprocessing.synchronize import Barrier as BarrierType
from multiprocessing import Process, Barrier
import time
import asyncio
import pytest

from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.dag import CyclicException

simple_graph_1 = [
    ("a", "b"),
    ("a", "c"),
    ("b", "d"),
    ("d", "e"),
]

simple_graph_2 = [("w", "x"), ("w", "y"), ("x", "z"), ("y", "z")]


@pytest.mark.asyncio
async def test_graph(event_loop: asyncio.AbstractEventLoop):
    async with GraphContext() as context:
        a_pub = await context.publisher("a")
        e_sub = await context.subscriber("e")

        for edge in simple_graph_1:
            await context.connect(*edge)

        c_sub = await context.subscriber("c")
        b_pub = await context.publisher("b")

        for edge in simple_graph_2:
            await context.connect(*edge)

        await context.disconnect("a", "b")
        await context.connect("e", "w")
        await context.connect("e", "c")
        await context.disconnect("a", "c")
        await context.connect("z", "a")

        with pytest.raises(CyclicException):
            await context.connect("a", "b")

        await context.disconnect("z", "a")
        await context.connect("a", "b")

        await context.connect("a", "c")
        await context.disconnect("e", "c")

        with pytest.raises(CyclicException):
            await context.connect("c", "a")


@pytest.mark.asyncio
async def test_comms(event_loop: asyncio.AbstractEventLoop):
    async with GraphContext() as context:
        a_pub = await context.publisher("a")
        e_sub = await context.subscriber("e")

        for edge in simple_graph_1:
            await context.connect(*edge)

        c_sub = await context.subscriber("c")
        b_pub = await context.publisher("b")

        # At this point
        # * e is subscribed to a and b
        # * c is subscribed to a

        await b_pub.broadcast("HELLO")

        msg = await e_sub.recv()
        assert msg == "HELLO"

        await a_pub.broadcast("BROADCAST_A")
        await b_pub.broadcast("BROADCAST_B")

        # By publishing two things that e is subscribed to simultaneously
        # there is no pre-set delivery order, so we don't know which will
        # arrive first.
        e_msgs = set(["BROADCAST_A", "BROADCAST_B"])
        rx_e = await e_sub.recv()
        assert rx_e in e_msgs
        e_msgs.remove(rx_e)

        rx_c = await c_sub.recv()
        assert rx_c == "BROADCAST_A"

        rx_e = await e_sub.recv()
        assert rx_e in e_msgs
        e_msgs.remove(rx_e)

        assert len(e_msgs) == 0


@pytest.mark.asyncio
async def test_order(event_loop: asyncio.AbstractEventLoop):
    async with GraphContext() as context:
        b_pub = await context.publisher("b")
        c_sub = await context.subscriber("c")
        e_sub = await context.subscriber("e")

        for edge in simple_graph_1:
            await context.connect(*edge)

        # Publishing multiple things from the same publisher should maintain order.
        send_msgs = [f"MSG{i}" for i in range(10)]

        for msg in send_msgs:
            await b_pub.broadcast(msg)

        for msg in send_msgs:
            rx_msg = await e_sub.recv()
            assert msg == rx_msg


@pytest.mark.asyncio
async def test_disconnect(event_loop: asyncio.AbstractEventLoop):
    async with GraphContext() as context:
        a_pub = await context.publisher("a")
        e_sub = await context.subscriber("e")
        c_sub = await context.subscriber("c")

        for edge in simple_graph_1:
            await context.connect(*edge)

        # Send a bunch of messages
        for msg in range(5):
            await a_pub.broadcast(msg)

        for msg in range(2):
            rx_e = await e_sub.recv()
            assert rx_e == msg

        # Disconnect e_sub; pending messages dropped!
        await context.disconnect("b", "d")

        # Send a few more now that e_sub is disconnected
        for msg in range(5, 10):
            await a_pub.broadcast(msg)

        for msg in range(10):
            rx_c = await c_sub.recv()
            assert rx_c == msg


@dataclass
class ArrayMessage:
    int_val: int
    arr: np.ndarray


class AsyncProcess(Process):
    stop_barrier: BarrierType
    barrier: BarrierType
    n_msgs: int

    def __init__(
        self, barrier: BarrierType, stop_barrier: BarrierType, n_msgs: int
    ) -> None:
        super().__init__()
        self.barrier = barrier
        self.stop_barrier = stop_barrier
        self.n_msgs = n_msgs

    def run(self) -> None:
        asyncio.run(self.run_async())

    async def run_async(self) -> None:
        raise NotImplementedError


class Sender(AsyncProcess):
    async def run_async(self) -> None:
        async with GraphContext() as context:
            tx = await context.publisher("sender")
            await asyncio.get_running_loop().run_in_executor(None, self.barrier.wait)

            assert len(tx._subscribers) != 0

            msg_size = 2**20 * 10  # 10 MB
            arr = np.zeros(int(msg_size // 8), dtype=np.float32)

            start = None
            for idx in range(self.n_msgs):
                await tx.broadcast(ArrayMessage(idx, arr))
                if start is None:
                    start = time.perf_counter()

            if start is not None:
                delta = time.perf_counter() - start
                message_rate = int(self.n_msgs / delta)
                print(f"{ message_rate } msgs/sec")
                print(f"{ msg_size * message_rate / 1024 / 1024 / 1024 } GB/sec")

            await asyncio.get_running_loop().run_in_executor(
                None, self.stop_barrier.wait
            )


class Receiver(AsyncProcess):
    async def run_async(self) -> None:
        async with GraphContext() as context:
            rx = await context.subscriber("receiver")
            await asyncio.get_running_loop().run_in_executor(None, self.barrier.wait)

            assert len(rx._publishers) != 0

            for msg_idx in range(self.n_msgs):
                async with rx.recv_zero_copy() as rx_msg:
                    assert msg_idx == rx_msg.int_val
                    del rx_msg

            print("Received all messages")

            await asyncio.get_running_loop().run_in_executor(
                None, self.stop_barrier.wait
            )


@pytest.mark.asyncio
async def test_multiprocess(event_loop: asyncio.AbstractEventLoop):
    async with GraphContext() as context:
        await context.connect("sender", "receiver")

        barrier = Barrier(2)
        stop_barrier = Barrier(2)
        n_msgs = 50  # 2**10

        sender_proc = Sender(barrier, stop_barrier, n_msgs)
        sender_proc.start()

        receiver_proc = Receiver(barrier, stop_barrier, n_msgs)
        receiver_proc.start()

        await event_loop.run_in_executor(None, sender_proc.join)
        await event_loop.run_in_executor(None, receiver_proc.join)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(test_graph(loop))
    finally:
        loop.close()
