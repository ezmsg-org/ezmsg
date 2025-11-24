import contextlib
import os
import tempfile
from pathlib import Path

import pytest

from ezmsg.core.graphserver import GraphServer
from ezmsg.util.perf.impl import Communication, CONFIGS, perform_test


PERF_MAX_DURATION = 0.5
PERF_NUM_MSGS = 8
PERF_MSG_SIZES = [64, 2**20]
PERF_NUM_BUFFERS = 2
CLIENTS_PER_CONFIG = {
    "fanout": 2,
    "fanin": 2,
    "relay": 2,
}


def _run_perf_case(
    config_name: str,
    comm: Communication,
    msg_size: int,
    server: GraphServer,
) -> None:
    metrics = perform_test(
        n_clients=CLIENTS_PER_CONFIG[config_name],
        max_duration=PERF_MAX_DURATION,
        num_msgs=PERF_NUM_MSGS,
        msg_size=msg_size,
        buffers=PERF_NUM_BUFFERS,
        comms=comm,
        config=CONFIGS[config_name],
        graph_address=server.address,
    )
    assert metrics.num_msgs > 0, (
        f"Failed to exchange messages for {config_name}/{comm.value}/msg={msg_size}"
    )


@contextlib.contextmanager
def _file_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w+") as lock_file:
        lock_file.write("0")
        lock_file.flush()
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            if os.name == "nt":
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock_file, fcntl.LOCK_UN)


@pytest.fixture(scope="module")
def perf_graph_server():
    """
    Spin up a dedicated graph server on an ephemeral port for the perf smoke tests.
    The shared instance keeps startup/shutdown overhead down while isolating it
    from the canonical graph server that other tests rely on.
    """
    lock_path = Path(tempfile.gettempdir()) / "ezmsg_perf_smoke.lock"
    with _file_lock(lock_path):
        server = GraphServer()
        server.start()
        try:
            yield server
        finally:
            server.stop()


@pytest.mark.parametrize("msg_size", PERF_MSG_SIZES, ids=lambda s: f"msg={s}")
@pytest.mark.parametrize("comm", list(Communication), ids=lambda c: f"comm={c.value}")
def test_fanout_perf(perf_graph_server, comm, msg_size):
    _run_perf_case("fanout", comm, msg_size, perf_graph_server)


@pytest.mark.parametrize("msg_size", PERF_MSG_SIZES, ids=lambda s: f"msg={s}")
@pytest.mark.parametrize("comm", list(Communication), ids=lambda c: f"comm={c.value}")
def test_fanin_perf(perf_graph_server, comm, msg_size):
    _run_perf_case("fanin", comm, msg_size, perf_graph_server)


@pytest.mark.parametrize("msg_size", PERF_MSG_SIZES, ids=lambda s: f"msg={s}")
@pytest.mark.parametrize("comm", list(Communication), ids=lambda c: f"comm={c.value}")
def test_relay_perf(perf_graph_server, comm, msg_size):
    _run_perf_case("relay", comm, msg_size, perf_graph_server)
