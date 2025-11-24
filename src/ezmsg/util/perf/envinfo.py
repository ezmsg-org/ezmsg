import dataclasses
import datetime
import platform
import typing
import sys
import subprocess

import ezmsg.core as ez

try:
    import numpy as np
except ImportError:
    ez.logger.error("ezmsg perf requires numpy")
    raise

try:
    import psutil
except ImportError:
    ez.logger.error("ezmsg perf requires psutil")
    raise


def _git_commit() -> str:
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def _git_branch() -> str:
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


@dataclasses.dataclass
class TestEnvironmentInfo:
    ezmsg_version: str = dataclasses.field(default_factory=lambda: ez.__version__)
    numpy_version: str = dataclasses.field(default_factory=lambda: np.__version__)
    python_version: str = dataclasses.field(
        default_factory=lambda: sys.version.replace("\n", " ")
    )
    os: str = dataclasses.field(default_factory=lambda: platform.system())
    os_version: str = dataclasses.field(default_factory=lambda: platform.version())
    machine: str = dataclasses.field(default_factory=lambda: platform.machine())
    processor: str = dataclasses.field(default_factory=lambda: platform.processor())
    cpu_count_logical: int | None = dataclasses.field(
        default_factory=lambda: psutil.cpu_count(logical=True)
    )
    cpu_count_physical: int | None = dataclasses.field(
        default_factory=lambda: psutil.cpu_count(logical=False)
    )
    memory_gb: float = dataclasses.field(
        default_factory=lambda: round(psutil.virtual_memory().total / (1024**3), 2)
    )
    start_time: str = dataclasses.field(
        default_factory=lambda: datetime.datetime.now().isoformat(timespec="seconds")
    )
    git_commit: str = dataclasses.field(default_factory=_git_commit)
    git_branch: str = dataclasses.field(default_factory=_git_branch)

    def __str__(self) -> str:
        fields = dataclasses.asdict(self)
        width = max(len(k) for k in fields)
        lines = ["TestEnvironmentInfo:"]
        for key, value in fields.items():
            lines.append(f"  {key.ljust(width)} : {value}")
        return "\n".join(lines)

    def diff(
        self, other: "TestEnvironmentInfo"
    ) -> dict[str, tuple[typing.Any, typing.Any]]:
        """Return a structured diff: {field: (self_value, other_value)} for changed fields."""
        a = dataclasses.asdict(self)
        b = dataclasses.asdict(other)
        keys = set(a) | set(b)
        return {k: (a.get(k), b.get(k)) for k in keys if a.get(k) != b.get(k)}


def format_env_diff(diffs: dict[str, tuple[typing.Any, typing.Any]]) -> str:
    """Pretty-print the structured diff in the same aligned style."""
    if not diffs:
        return "No differences."
    width = max(len(k) for k in diffs)
    lines = ["Differences in TestEnvironmentInfo:"]
    for k in sorted(diffs):
        left, right = diffs[k]
        lines.append(f"  {k.ljust(width)} : {left}  !=  {right}")
    return "\n".join(lines)


def diff_envs(a: TestEnvironmentInfo, b: TestEnvironmentInfo) -> str:
    return format_env_diff(a.diff(b))
