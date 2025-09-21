import json
import datetime
import itertools
import argparse

from dataclasses import dataclass

from ..messagecodec import MessageEncoder
from .envinfo import TestEnvironmentInfo
from .impl import (
    TestParameters,
    TestLogEntry, 
    perform_test, 
    Communication,
    CONFIGS,
)

import ezmsg.core as ez

def get_datestamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


def perf_run(    
    duration: float,
    num_buffers: int,
    iters: int,
) -> None:
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
                duration = duration,
                num_buffers = num_buffers
            )
            
            results = [
                perform_test(
                    n_clients = n_clients,
                    duration = duration, 
                    msg_size = msg_size, 
                    buffers = num_buffers,
                    comms = comms,
                    config = config,
                ) for _ in range(iters)
            ]

            output = TestLogEntry(
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
    p_run.add_argument(
        "--iters", "-i",
        type = int,
        default = 3,
        help = "number of times to run each test"
    )

    p_run.set_defaults(_handler=lambda ns: perf_run(
        duration = ns.duration, 
        num_buffers = ns.num_buffers,
        iters = ns.iters
    ))