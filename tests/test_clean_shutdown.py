import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
RUNNER = Path(__file__).with_name("shutdown_runner.py")
EXAMPLE_RUNNER = Path(__file__).with_name("clean_shutdown_examples_runner.py")


def _run_process(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    signals: int = 0,
    allowed_returncodes: set[int] | None = None,
    use_inband_sigint: bool | None = None,
    ready_token: str | None = None,
    start_delay: float = 0.5,
    signal_delay: float = 0.5,
    timeout: float = 15.0,
) -> None:
    if env is None:
        env = os.environ.copy()
    env.setdefault("EZMSG_LOGLEVEL", "WARNING")
    env.pop("EZMSG_STRICT_SHUTDOWN", None)
    pythonpath = env.get("PYTHONPATH", "")
    paths = [str(ROOT), str(ROOT / "src")]
    if pythonpath:
        env["PYTHONPATH"] = os.pathsep.join(paths + [pythonpath])
    else:
        env["PYTHONPATH"] = os.pathsep.join(paths)

    if use_inband_sigint is None:
        use_inband_sigint = os.name == "nt" and signals > 0
    if use_inband_sigint:
        env["EZMSG_INBAND_SIGINT"] = "1"

    kwargs: dict = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
        "env": env,
        "bufsize": 1,
    }
    if use_inband_sigint:
        kwargs["stdin"] = subprocess.PIPE
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["preexec_fn"] = os.setsid

    proc = subprocess.Popen(cmd, **kwargs)

    ready = threading.Event()
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []

    def _reader(stream, sink: list[str]) -> None:
        for line in iter(stream.readline, ""):
            sink.append(line)
            if ready_token and ready_token in line:
                ready.set()
        stream.close()

    t_out = threading.Thread(target=_reader, args=(proc.stdout, stdout_lines))
    t_err = threading.Thread(target=_reader, args=(proc.stderr, stderr_lines))
    t_out.start()
    t_err.start()

    if ready_token:
        if not ready.wait(timeout=5.0):
            if proc.poll() is not None:
                out = "".join(stdout_lines)
                err = "".join(stderr_lines)
                raise AssertionError(
                    f"Process exited before {ready_token}. rc={proc.returncode}\n"
                    f"stdout:\n{out}\n"
                    f"stderr:\n{err}"
                )
    else:
        time.sleep(start_delay)

    for _ in range(signals):
        if proc.poll() is not None:
            break
        if use_inband_sigint:
            if proc.stdin is not None:
                proc.stdin.write("SIGINT\n")
                proc.stdin.flush()
        else:
            if os.name == "nt":
                proc.send_signal(signal.CTRL_C_EVENT)
            else:
                os.killpg(proc.pid, signal.SIGINT)
        time.sleep(signal_delay)

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5.0)
        out = "".join(stdout_lines)
        err = "".join(stderr_lines)
        raise AssertionError(
            "Process did not exit in time\n"
            f"stdout:\n{out}\n"
            f"stderr:\n{err}"
        )

    t_out.join(timeout=2.0)
    t_err.join(timeout=2.0)

    if allowed_returncodes is None:
        if signals <= 0:
            allowed_returncodes = {0}
        elif signals == 1:
            allowed_returncodes = {0}
        else:
            if os.name == "nt":
                allowed_returncodes = {3221225786}
            else:
                allowed_returncodes = {-signal.SIGINT, 130}

    if proc.returncode not in allowed_returncodes:
        out = "".join(stdout_lines)
        err = "".join(stderr_lines)
        raise AssertionError(
            f"Unexpected return code: {proc.returncode}\n"
            f"stdout:\n{out}\n"
            f"stderr:\n{err}"
        )


def _run_shutdown_case(
    target: str,
    *,
    signals: int = 1,
    timeout: float = 15.0,
    allowed_returncodes: set[int] | None = None,
) -> None:
    env = os.environ.copy()
    env.pop("EZMSG_STRICT_SHUTDOWN", None)
    env["EZMSG_SHUTDOWN_TEST"] = target
    _run_process(
        [sys.executable, "-u", str(RUNNER)],
        env=env,
        signals=signals,
        ready_token="READY",
        timeout=timeout,
        allowed_returncodes=allowed_returncodes,
    )


def _available_start_methods() -> list[str]:
    import multiprocessing as mp

    available = set(mp.get_all_start_methods())
    return [method for method in ("spawn", "fork") if method in available]


def _run_example_case(
    case: str,
    *,
    start_method: str,
    signals: int = 0,
    timeout: float = 20.0,
    allowed_returncodes: set[int] | None = None,
) -> None:
    env = os.environ.copy()
    env["EZMSG_SHUTDOWN_EXAMPLE"] = case
    env["EZMSG_MP_START"] = start_method
    _run_process(
        [sys.executable, "-u", str(EXAMPLE_RUNNER)],
        env=env,
        signals=signals,
        ready_token="READY",
        timeout=timeout,
        start_delay=1.0,
        allowed_returncodes=allowed_returncodes,
    )


def _sigint_returncodes() -> set[int]:
    if os.name == "nt":
        return {1, 3221225786}
    return {-signal.SIGINT, 130}


def test_shutdown_blocking_disk():
    _run_shutdown_case("blocking_disk", allowed_returncodes=_sigint_returncodes())


def test_shutdown_blocking_socket():
    _run_shutdown_case("blocking_socket", allowed_returncodes=_sigint_returncodes())


def test_shutdown_exception_on_cancel():
    _run_shutdown_case("exception_on_cancel", allowed_returncodes=_sigint_returncodes())


def test_shutdown_ignore_cancel():
    _run_shutdown_case("ignore_cancel", signals=2)


@pytest.mark.parametrize("case", ["complete", "normalterm", "normalterm_thread"])
@pytest.mark.parametrize("start_method", _available_start_methods())
def test_examples_complete_without_sigint(case: str, start_method: str) -> None:
    _run_example_case(case, start_method=start_method)


@pytest.mark.parametrize("start_method", _available_start_methods())
def test_infinite_requires_sigint(start_method: str) -> None:
    _run_example_case(
        "infinite",
        start_method=start_method,
        signals=1,
        allowed_returncodes={0},
    )
