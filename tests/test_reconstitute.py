import asyncio
from copy import deepcopy
import timeit
import matplotlib.pyplot as plt

from ezmsg.core.graphserver import GraphServer, GraphService
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.core.messagemarshal import MessageMarshal

import numpy as np

ADDR = ('127.0.0.1', 12345)
ITERS = 10000

def setup_graph_and_shm(msg_size):
    server = GraphServer()
    server.start(ADDR)
    graph_service = GraphService(ADDR)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(graph_service.ensure())
    aa = AxisArray(data=np.random.normal(size=(msg_size,)), dims=['a'])
    with MessageMarshal.serialize(0, aa) as (size, _, _):
        msg_size_bytes = size
    shm = loop.run_until_complete(graph_service.create_shm(num_buffers=32, buf_size=msg_size_bytes + 100))
    with shm.buffer(0) as mem:
        MessageMarshal.to_mem(0, aa, mem)
    return server, shm, msg_size_bytes

def recon_with_deepcopy_func(shm):
    with shm.buffer(0, readonly=True) as mem:
        with MessageMarshal.obj_from_mem(mem) as obj:
            deepcopy(obj)

def recon_without_deepcopy_func(shm):
    with shm.buffer(0, readonly=True) as mem:
        with MessageMarshal.obj_from_mem(mem) as obj:
            pass


def main():
    msg_sizes = [2**i for i in range(8, 25, 2)]  # 2**8, 2**10, ..., 2**24
    fanouts = [1, 2, 4, 8, 16, 32]
    results = {}
    msg_size_bytes_list = []

    for msg_size in msg_sizes:
        print(f"\nTesting MSG_SIZE={msg_size}")
        server, shm, msg_size_bytes = setup_graph_and_shm(msg_size)
        msg_size_bytes_list.append(msg_size_bytes)

        # Warm up
        for _ in range(100):
            recon_with_deepcopy_func(shm)
            recon_without_deepcopy_func(shm)

        # Time a single reconstitution and a single deepcopy
        recon_time = timeit.timeit(
            stmt=lambda: recon_without_deepcopy_func(shm),
            number=ITERS
        ) / ITERS * 1e9  # ns
        deepcopy_time = timeit.timeit(
            stmt=lambda: recon_with_deepcopy_func(shm),
            number=ITERS
        ) / ITERS * 1e9  # ns

        print(f'Per reconstitution (ns): {recon_time}')
        print(f'Per deepcopy (ns): {deepcopy_time}')

        # For each fanout, compute total time for both strategies
        total_recon = []
        total_deepcopy = []
        for fanout in fanouts:
            # Strategy 1: 1 reconstitution + 1 deepcopy, rest are free
            total_deepcopy.append(recon_time + deepcopy_time)
            # Strategy 2: reconstitute N times (no deepcopy)
            total_recon.append(fanout * recon_time)

        results[msg_size_bytes] = {
            'fanouts': fanouts,
            'total_recon': total_recon,
            'total_deepcopy': total_deepcopy,
        }

        server.stop()

    # Plot results
    plt.figure(figsize=(12, 8))
    for msg_size_bytes in msg_size_bytes_list:
        fanouts = results[msg_size_bytes]['fanouts']
        plt.plot(
            fanouts,
            results[msg_size_bytes]['total_recon'],
            label=f'Recon N times ({msg_size_bytes} bytes)',
            marker='o', linestyle='--', alpha=0.7
        )
        plt.plot(
            fanouts,
            results[msg_size_bytes]['total_deepcopy'],
            label=f'Recon 1 + deepcopy N-1 ({msg_size_bytes} bytes)',
            marker='x', linestyle='-', alpha=0.7
        )
    plt.xscale('log', base=2)
    plt.yscale('log')
    plt.xlabel('Fanout (number of consumers)')
    plt.ylabel('Total time (ns)')
    plt.title('Fanout Tradeoff: Reconstitute N times vs Reconstitute+Deepcopy')
    plt.legend(fontsize='small', ncol=2)
    plt.grid(True, which='both', ls='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('fanout_tradeoff.png')
    print('Plot saved as fanout_tradeoff.png')

if __name__ == '__main__':
    main()