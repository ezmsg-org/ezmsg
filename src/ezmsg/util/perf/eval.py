import json
import datetime
import itertools

from dataclasses import dataclass

from ..messagecodec import MessageEncoder
from .util import (
    TestEnvironmentInfo, 
    TestParameters, 
    perform_test, 
    Communication,
    CONFIGS,
)

import ezmsg.core as ez

def get_datestamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

@dataclass
class PerfEvalArgs:
    duration: float
    num_buffers: int

def perf_eval(args: PerfEvalArgs) -> None:
    
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


def command() -> None:
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--duration",
        type=float,
        default=2.0,
        help="How long to run each load test (seconds) (default = 2.0)",
    )

    parser.add_argument(
        "--num-buffers", 
        type=int, 
        default=32, 
        help="shared memory buffers (default = 32)"
    )

    args = parser.parse_args(namespace=PerfEvalArgs)

    perf_eval(args)


