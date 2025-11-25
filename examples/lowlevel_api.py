import asyncio

import ezmsg.core as ez

PORT = 12345
MAX_COUNT = 100
TOPIC = "/TEST"


async def handle_pub(pub: ez.Publisher) -> None:
    print("Publisher Task Launched")

    count = 0

    while True:
        await pub.broadcast(f"{count=}")
        await asyncio.sleep(0.1)
        count += 1
        if count >= MAX_COUNT:
            break

    print("Publisher Task Concluded")


async def handle_sub(sub: ez.Subscriber) -> None:
    print("Subscriber Task Launched")

    rx_count = 0
    while True:
        async with sub.recv_zero_copy() as msg:
            # Uncomment if you want to witness backpressure!
            # await asyncio.sleep(0.15)
            print(msg)

        rx_count += 1
        if rx_count >= MAX_COUNT:
            break

    print("Subscriber Task Concluded")


async def host(host: str = "127.0.0.1"):
    # Manually create a GraphServer
    server = ez.GraphServer()
    server.start((host, PORT))

    print(f"Created GraphServer @ {server.address}")

    try:
        test_pub = await ez.Publisher.create(TOPIC, (host, PORT), host=host)
        test_sub1 = await ez.Subscriber.create(TOPIC, (host, PORT))
        test_sub2 = await ez.Subscriber.create(TOPIC, (host, PORT))

        await asyncio.sleep(1.0)

        pub_task = asyncio.Task(handle_pub(test_pub))
        sub_task_1 = asyncio.Task(handle_sub(test_sub1))
        sub_task_2 = asyncio.Task(handle_sub(test_sub2))

        await asyncio.wait([pub_task, sub_task_1, sub_task_2])

        test_pub.close()
        test_sub1.close()
        test_sub2.close()

        for future in asyncio.as_completed(
            [
                test_pub.wait_closed(),
                test_sub1.wait_closed(),
                test_sub2.wait_closed(),
            ]
        ):
            await future

    finally:
        server.stop()

    print("Done")


async def attach_client(host: str = "127.0.0.1"):

    sub = await ez.Subscriber.create(TOPIC, (host, PORT))

    try:
        while True:
            async with sub.recv_zero_copy() as msg:
                # Uncomment if you want to see EXTREME backpressure!
                # await asyncio.sleep(1.0)
                print(msg)

    except asyncio.CancelledError:
        pass

    finally:
        sub.close()
        await sub.wait_closed()
        print("Detached")


if __name__ == "__main__":
    from dataclasses import dataclass
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("--attach", action="store_true", help="attach to running graph")
    parser.add_argument("--host", default="0.0.0.0", help="hostname for graphserver")

    @dataclass
    class Args:
        attach: bool
        host: str

    args = Args(**vars(parser.parse_args()))

    if args.attach:
        asyncio.run(attach_client(host=args.host))
    else:
        asyncio.run(host(host=args.host))
