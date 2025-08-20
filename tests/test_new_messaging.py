import asyncio

from ezmsg.core.graphserver import GraphServer, GraphService

from ezmsg.core.subclient import Subscriber
from ezmsg.core.pubclient import Publisher

# ADDR = ('127.0.0.1', 12345)
ADDR = ('0.0.0.0', 12345)
MAX_COUNT = 100
TOPIC = '/TEST'

async def handle_pub(pub: Publisher) -> None:
    print('Publisher Task Launched')

    count = 0

    while True:
        await pub.broadcast(f'{count=}')
        await asyncio.sleep(0.1)
        count += 1
        if count >= MAX_COUNT: break

    print('Publisher Task Concluded')

async def handle_sub(sub: Subscriber) -> None:
    print('Subscriber Task Launched')

    rx_count = 0
    while True:
        async with sub.recv_zero_copy() as msg:
            print(msg)
        rx_count += 1
        if rx_count >= MAX_COUNT: break
    
    print('Subscriber Task Concluded')


async def host():
    # Manually create a GraphServer
    server = GraphServer()
    server.start(ADDR)

    print(f'Created GraphServer @ {server.address}')

    # Create a graph_service that will interact with this GraphServer
    graph_service = GraphService(ADDR)
    await graph_service.ensure()

    test_pub = await Publisher.create(TOPIC, ADDR, host="0.0.0.0")
    test_sub1 = await Subscriber.create(TOPIC, ADDR)
    test_sub2 = await Subscriber.create(TOPIC, ADDR)

    pub_task = asyncio.Task(handle_pub(test_pub))
    sub_task_1 = asyncio.Task(handle_sub(test_sub1))
    sub_task_2 = asyncio.Task(handle_sub(test_sub2))

    try:
        await asyncio.wait([pub_task, sub_task_1, sub_task_2])

    finally:
        server.stop()

    print('Done')


async def attach_client():
    # Attach to a running GraphServer
    graph_service = GraphService(ADDR)
    await graph_service.ensure()

    print(f'Connected to GraphServer @ {graph_service.address}')

    sub = await Subscriber.create(TOPIC, ADDR)

    while True:
        async with sub.recv_zero_copy() as msg:
            print(msg)


if __name__ == '__main__':
    from dataclasses import dataclass
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('--attach', action = 'store_true', help = 'attach to running graph')

    @dataclass
    class Args:
        attach: bool

    args = Args(**vars(parser.parse_args()))

    if args.attach:
        asyncio.run(attach_client())
    else:
        asyncio.run(host())