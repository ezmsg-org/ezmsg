import json
import datetime
import itertools
import argparse
import typing

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

DEFAULT_MSG_SIZES = [2 ** exp for exp in range(4, 25, 8)]
DEFAULT_N_CLIENTS = [2 ** exp for exp in range(0, 6, 2)]
DEFAULT_COMMS = [c for c in Communication]

def get_datestamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


def perf_run(    
    duration: float,
    num_buffers: int,
    iters: int,
    msg_sizes: typing.Iterable[int] | None,
    n_clients: typing.Iterable[int] | None,
    comms: typing.Iterable[str] | None,
    configs: typing.Iterable[str] | None,
) -> None:
    
    if n_clients is None:
        n_clients = DEFAULT_N_CLIENTS
    if any(c < 0 for c in n_clients):
        ez.logger.error('All tests must have >=0 clients')
        return

    if msg_sizes is None:
        msg_sizes = DEFAULT_MSG_SIZES
    if any(s <= 0 for s in msg_sizes):
        ez.logger.error('All msg_sizes must be >0 bytes')

    try:
        communications = DEFAULT_COMMS if comms is None else [Communication(c) for c in comms]
    except ValueError:
        ez.logger.error(f"Invalid test communications requested. Valid communications: {', '.join([c.value for c in Communication])}")
        return
    
    try:
        configurators = list(CONFIGS.values()) if configs is None else [CONFIGS[c] for c in configs]
    except ValueError:
        ez.logger.error(f"Invalid test configuration requested. Valid configurations: {', '.join([c for c in CONFIGS])}")
        return

    test_list = list(itertools.product(msg_sizes, n_clients, configurators, communications))

    with open(f'perf_{get_datestamp()}.txt', 'w') as out_f:

        out_f.write(json.dumps(TestEnvironmentInfo(), cls = MessageEncoder) + "\n")

        for test_idx, (msg_size, clients, conf, comm) in enumerate(test_list):

            ez.logger.info(f"RUNNING TEST {test_idx + 1} / {len(test_list)} ({(test_idx / len(test_list)) * 100.0:0.2f} %)")
            
            params = TestParameters(
                msg_size = msg_size,
                n_clients = clients,
                config = conf.__name__,
                comms = comm.value,
                duration = duration,
                num_buffers = num_buffers
            )
            
            results = [
                perform_test(
                    n_clients = clients,
                    duration = duration, 
                    msg_size = msg_size, 
                    buffers = num_buffers,
                    comms = comm,
                    config = conf,
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
        help = "number of times to run each test (default = 3)"
    )

    p_run.add_argument(
        "--msg-sizes",
        type = int,
        default = None,
        nargs = "*",
        help = f"message sizes in bytes (default = {DEFAULT_MSG_SIZES})"
    )

    p_run.add_argument(
        "--n-clients",
        type = int,
        default = None,
        nargs = "*",
        help = f"number of clients (default = {DEFAULT_N_CLIENTS})"
    )

    p_run.add_argument(
        "--comms",
        type = str,
        default = None,
        nargs = "*",
        help = f"communication strategies to test (default = {[c.value for c in DEFAULT_COMMS]})"
    )

    p_run.add_argument(
        "--configs",
        type = str,
        default = None,
        nargs = "*",
        help = f"configurations to test (default = {[c for c in CONFIGS]})"
    )


    p_run.set_defaults(_handler=lambda ns: perf_run(
        duration = ns.duration, 
        num_buffers = ns.num_buffers,
        iters = ns.iters,
        msg_sizes = ns.msg_sizes,
        n_clients = ns.n_clients,
        comms = ns.comms,
        configs = ns.configs,
    ))