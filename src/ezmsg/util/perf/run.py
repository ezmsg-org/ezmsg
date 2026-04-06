import os
import sys
import json
import itertools
import argparse
import typing
import random
import time

from datetime import datetime, timedelta
from contextlib import contextmanager, redirect_stdout, redirect_stderr
from pathlib import Path

import ezmsg.core as ez
from ezmsg.core.graphserver import GraphServer

from ..messagecodec import MessageEncoder
from .envinfo import TestEnvironmentInfo
from .util import warmup
from .impl import (
    TestParameters,
    TestLogEntry,
    perform_test,
    Communication,
    CONFIGS,
)

DEFAULT_MSG_SIZES = [2**4, 2**20]
DEFAULT_N_CLIENTS = [1, 16]
DEFAULT_COMMS = [c for c in Communication]


# --- Output Suppression Context Manager ---
@contextmanager
def suppress_output(verbose: bool = False):
    """Context manager to redirect stdout and stderr to os.devnull"""
    if verbose:
        yield
    else:
        # Open the null device for writing
        with open(os.devnull, "w") as fnull:
            # Redirect both stdout and stderr to the null device
            with redirect_stderr(fnull):
                with redirect_stdout(fnull):
                    yield


def _check_for_quit_default() -> bool:
    return False


CHECK_FOR_QUIT = _check_for_quit_default

if sys.platform.startswith("win"):
    import msvcrt

    def _check_for_quit_win() -> bool:
        """
        Checks for the 'q' key press in a non-blocking way.
        Returns True if 'q' is pressed (case-insensitive), False otherwise.
        """
        # Windows: Use msvcrt for non-blocking keyboard hit detection
        if msvcrt.kbhit():  # type: ignore
            # Read the key press (returns bytes)
            key = msvcrt.getch()  # type: ignore
            try:
                # Decode and check for 'q'
                return key.decode().lower() == "q"
            except UnicodeDecodeError:
                # Handle potential non-text key presses gracefully
                return False
        return False

    CHECK_FOR_QUIT = _check_for_quit_win

else:
    import select

    def _check_for_quit() -> bool:
        """
        Checks for the 'q' key press in a non-blocking way.
        Returns True if 'q' is pressed (case-insensitive), False otherwise.
        """
        # Linux/macOS: Use select to check if stdin has data
        # select.select(rlist, wlist, xlist, timeout)
        # timeout=0 makes it non-blocking
        if sys.stdin.isatty():
            i, o, e = select.select([sys.stdin], [], [], 0)  # type: ignore
            if i:
                # Read the buffered character
                key = sys.stdin.read(1)
                return key.lower() == "q"
        return False

    CHECK_FOR_QUIT = _check_for_quit


def get_datestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def output_paths_for_name(name: str) -> tuple[Path, Path]:
    return Path(f"perf_{name}.txt"), Path(f"report_{name}.html")


def benchmark(
    max_duration: float,
    num_msgs: int,
    num_buffers: int,
    iters: int,
    repeats: int,
    msg_sizes: list[int] | None,
    n_clients: list[int] | None,
    comms: typing.Iterable[str] | None,
    configs: typing.Iterable[str] | None,
    grid: bool,
    warmup_dur: float,
    name: str | None = None,
    open_browser: bool = True,
) -> tuple[Path, Path | None]:
    if n_clients is None:
        n_clients = DEFAULT_N_CLIENTS
    if any(c < 0 for c in n_clients):
        ez.logger.error("All tests must have >=0 clients")
        raise ValueError("All tests must have >=0 clients")

    if msg_sizes is None:
        msg_sizes = DEFAULT_MSG_SIZES
    if any(s < 0 for s in msg_sizes):
        ez.logger.error("All msg_sizes must be >=0 bytes")
        raise ValueError("All msg_sizes must be >=0 bytes")

    if not grid and len(list(n_clients)) != len(list(msg_sizes)):
        ez.logger.warning(
            "Not performing a grid test of all combinations of n_clients and msg_sizes, but "
            + f"{len(n_clients)=} which is not equal to {len(msg_sizes)=}. "
        )

    try:
        communications = (
            DEFAULT_COMMS if comms is None else [Communication(c) for c in comms]
        )
    except ValueError:
        ez.logger.error(
            f"Invalid test communications requested. Valid communications: {', '.join([c.value for c in Communication])}"
        )
        raise ValueError("Invalid test communications requested")

    try:
        configurators = (
            list(CONFIGS.values()) if configs is None else [CONFIGS[c] for c in configs]
        )
    except ValueError:
        ez.logger.error(
            f"Invalid test configuration requested. Valid configurations: {', '.join([c for c in CONFIGS])}"
        )
        raise ValueError("Invalid test configuration requested")

    subitr = itertools.product if grid else zip

    test_list = [
        (msg_size, clients, conf, comm)
        for msg_size, clients in subitr(msg_sizes, n_clients)
        for conf, comm in itertools.product(configurators, communications)
    ] * iters

    random.shuffle(test_list)

    server = GraphServer()
    server.start()

    ez.logger.info(
        f"About to run {len(test_list)} tests (repeated {repeats} times) of {max_duration} sec (max) each."
    )
    ez.logger.info(
        f"During each test, source will attempt to send {num_msgs} messages to the sink."
    )
    ez.logger.info(
        "Please try to avoid running other taxing software while this perf test runs."
    )
    ez.logger.info(
        "NOTE: Tests swallow interrupt. After warmup, use 'q' then [enter] to quit tests early."
    )

    quitting = False

    start_time = time.time()
    if name is not None:
        output_path, html_out = output_paths_for_name(name)
    else:
        output_path = Path(f"perf_{get_datestamp()}.txt")
        html_out = None

    try:
        ez.logger.info(f"Warming up for {warmup_dur} seconds...")
        warmup(warmup_dur)

        with open(output_path, "w") as out_f:
            for _ in range(repeats):
                out_f.write(
                    json.dumps(TestEnvironmentInfo(), cls=MessageEncoder) + "\n"
                )

                for test_idx, (msg_size, clients, conf, comm) in enumerate(test_list):
                    if CHECK_FOR_QUIT():
                        ez.logger.info("Stopping tests early...")
                        quitting = True
                        break

                    ez.logger.info(
                        f"TEST {test_idx + 1}/{len(test_list)}: "
                        f"{clients=}, {msg_size=}, conf={conf.__name__}, "
                        f"comm={comm.value}"
                    )

                    output = TestLogEntry(
                        params=TestParameters(
                            msg_size=msg_size,
                            num_msgs=num_msgs,
                            n_clients=clients,
                            config=conf.__name__,
                            comms=comm.value,
                            max_duration=max_duration,
                            num_buffers=num_buffers,
                        ),
                        results=perform_test(
                            n_clients=clients,
                            max_duration=max_duration,
                            num_msgs=num_msgs,
                            msg_size=msg_size,
                            buffers=num_buffers,
                            comms=comm,
                            config=conf,
                            graph_address=server.address,
                        ),
                    )

                    out_f.write(json.dumps(output, cls=MessageEncoder) + "\n")

                if quitting:
                    break

    finally:
        server.stop()
        d = datetime(1, 1, 1) + timedelta(seconds=time.time() - start_time)
        dur_str = ":".join(
            [str(n) for n in [d.day - 1, d.hour, d.minute, d.second] if n != 0]
        )
        ez.logger.info(f"Tests concluded.  Wallclock Runtime: {dur_str}s")

    html_path = None
    try:
        from .analysis import write_html_report

        html_path = write_html_report(
            perf_path=output_path,
            output_path=html_out,
            open_browser=open_browser,
        )
        ez.logger.info(f"Wrote benchmark log to {output_path}")
        ez.logger.info(f"Wrote benchmark report to {html_path}")
    except ImportError:
        ez.logger.warning("Could not generate benchmark HTML report; analysis dependencies are unavailable.")

    return output_path, html_path


def setup_benchmark_cmdline(subparsers: argparse._SubParsersAction) -> None:
    p_run = subparsers.add_parser("benchmark", help="run the legacy benchmark matrix")

    p_run.add_argument(
        "--max-duration",
        type=float,
        default=5.0,
        help="maximum individual test duration in seconds (default = 5.0)",
    )

    p_run.add_argument(
        "--num-msgs",
        type=int,
        default=1000,
        help="number of messages to send per-test (default = 1000)",
    )

    # NOTE: We default num-buffers = 1 because this degenerate perf test scenario (blasting
    # messages as fast as possible through the system) results in one of two scenerios:
    # 1. A (few) messages is/are enqueued and dequeued before another message is posted
    # 2. The buffer fills up before being FULLY emptied resulting in longer latency.
    #    (once a channel enters this condition, it tends to stay in this condition)
    #
    # This _indeterminate_ behavior results in bimodal distributions of runtimes that make
    # A/B performance comparisons difficult.  The perf test is not representative of the vast
    # majority of production ezmsg systems where publishing is generally rate-limited.
    #
    # A flow-control algorithm could stabilize perf-test results with num_buffers > 1, but is
    # generally implemented by enforcing delays on the publish side which simply degrades
    # performance in the vast majority of ezmsg systems. - Griff
    p_run.add_argument(
        "--num-buffers",
        type=int,
        default=1,
        help="shared memory buffers (default = 1)",
    )

    p_run.add_argument(
        "--iters",
        "-i",
        type=int,
        default=5,
        help="number of times to run each test (default = 5)",
    )

    p_run.add_argument(
        "--repeats",
        "-r",
        type=int,
        default=10,
        help="number of times to repeat the perf (default = 10)",
    )

    p_run.add_argument(
        "--msg-sizes",
        type=int,
        default=None,
        nargs="*",
        help=f"message sizes in bytes (default = {DEFAULT_MSG_SIZES})",
    )

    p_run.add_argument(
        "--n-clients",
        type=int,
        default=None,
        nargs="*",
        help=f"number of clients (default = {DEFAULT_N_CLIENTS})",
    )

    p_run.add_argument(
        "--comms",
        type=str,
        default=None,
        nargs="*",
        help=f"communication strategies to test (default = {[c.value for c in DEFAULT_COMMS]})",
    )

    p_run.add_argument(
        "--configs",
        type=str,
        default=None,
        nargs="*",
        help=f"configurations to test (default = {[c for c in CONFIGS]})",
    )

    p_run.add_argument(
        "--warmup",
        type=float,
        default=60.0,
        help="warmup CPU with busy task for some number of seconds (default = 60.0)",
    )

    p_run.add_argument(
        "--name",
        type=str,
        default=None,
        help="optional short name used for perf_<name>.txt and report_<name>.html",
    )

    p_run.add_argument(
        "--no-browser",
        action="store_true",
        help="write the generated HTML report without opening it in a browser",
    )

    p_run.set_defaults(
        _handler=lambda ns: benchmark(
            max_duration=ns.max_duration,
            num_msgs=ns.num_msgs,
            num_buffers=ns.num_buffers,
            iters=ns.iters,
            repeats=ns.repeats,
            msg_sizes=ns.msg_sizes,
            n_clients=ns.n_clients,
            comms=ns.comms,
            configs=ns.configs,
            grid=True,
            warmup_dur=ns.warmup,
            name=ns.name,
            open_browser=not ns.no_browser,
        )
    )
