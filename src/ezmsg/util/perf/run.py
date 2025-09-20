import json
import datetime
import itertools
import argparse

from dataclasses import dataclass

from ..messagecodec import MessageEncoder
from .envinfo import TestEnvironmentInfo
from .impl import (
    TestParameters, 
    perform_test, 
    Communication,
    CONFIGS,
)

import ezmsg.core as ez

def get_datestamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

@dataclass
class PerfRunArgs:
    duration: float
    num_buffers: int

def perf_run(args: PerfRunArgs) -> None:
    """
    Configurations (config):
    - fanin: Many publishers to one subscriber
    - fanout: one publisher to many subscribers
    - relay: one publisher to one subscriber through many relays

    Communication strategies (comms):
    - local: all subs, relays, and pubs are in the SAME process
    - shm / tcp: some clients move to a second process; comms via shared memory / TCP
        * fanin: all publishers moved
        * fanout: all subscribers moved
        * relay: the publisher and all relay nodes moved
    - shm_spread / tcp_spread: each client in its own process; comms via SHM / TCP respectively

    Variables:
    - n_clients: pubs (fanin), subs (fanout), or relays (relay)  
    - msg_size: nominal message size (bytes)

    Metrics:
    - sample_rate: messages/sec at the sink (higher = better)
    - data_rate: bytes/sec at the sink (higher = better)
    - latency_mean: average send -> receive latency in seconds (lower = better)
    """
    
    msg_sizes = [2 ** exp for exp in range(4, 25, 8)]
    n_clients = [2 ** exp for exp in range(0, 6, 2)]
    comms = [c for c in Communication]

    test_list = list(itertools.product(msg_sizes, n_clients, CONFIGS, comms))

    with open(f'perf_{get_datestamp()}.txt', 'w') as out_f:

        out_f.write(json.dumps(TestEnvironmentInfo(), cls = MessageEncoder) + "\n")

        for test_idx, (msg_size, n_clients, config, comms) in enumerate(test_list):

            ez.logger.info(f"RUNNING TEST {test_idx + 1} / {len(test_list)} ({(test_idx / len(test_list)) * 100.0:0.2f} %)")
            
            params = TestParameters(
                msg_size = msg_size,
                n_clients = n_clients,
                config = config.__name__,
                comms = comms.value,
                duration = args.duration,
                num_buffers = args.num_buffers
            )
            
            results = perform_test(
                n_clients = n_clients,
                duration = args.duration, 
                msg_size = msg_size, 
                buffers = args.num_buffers,
                comms = comms,
                config = config,
            )

            output = dict(
                params = params,
                results = results
            )

            out_f.write(json.dumps(output, cls = MessageEncoder) + "\n")

def setup_run_cmdline(subparsers: argparse._SubParsersAction) -> None:

    p_run = subparsers.add_parser("run", help="run performance test")
    p_run.add_argument(
        "--duration",
        type=float,
        default=2.0,
        help="individual test duration in seconds (default = 2.0)",
    )
    p_run.add_argument(
        "--num-buffers",
        type=int,
        default=32,
        help="shared memory buffers (default = 32)",
    )

    p_run.set_defaults(_handler=lambda ns: perf_run(
        PerfRunArgs(
            duration = ns.duration, 
            num_buffers = ns.num_buffers
        )
    ))