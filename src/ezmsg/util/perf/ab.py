from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile

from dataclasses import asdict, dataclass
from pathlib import Path


DEFAULT_PAIR_SEED = 0
DEFAULT_REF_A = "dev"
DEFAULT_REF_B = "CURRENT"
VENV_DIR_CANDIDATES = (".venv", "venv", ".env", "env")


@dataclass(frozen=True)
class ABEnvironmentInfo:
    label: str
    ref: str
    tree: str
    python: str
    python_version: str
    ezmsg_version: str
    numpy_version: str
    git_commit: str
    git_branch: str
    dirty: bool
    env_mode: str
    pyproject_hash: str | None
    uv_lock_hash: str | None
    env_overrides: dict[str, str]


@dataclass(frozen=True)
class ABCaseSummary:
    case_id: str
    a_us_per_message_median: float
    b_us_per_message_median: float
    delta_pct_median: float
    delta_pct_mean: float
    pair_count: int
    b_faster_pairs: int


@dataclass(frozen=True)
class ABRunSummary:
    ref_a: str
    ref_b: str
    rounds: int
    seed: int
    env_a: ABEnvironmentInfo
    env_b: ABEnvironmentInfo
    warnings: list[str]
    cases: list[ABCaseSummary]


def build_pair_order(rounds: int, seed: int) -> list[tuple[str, str]]:
    base = [("A", "B"), ("B", "A")] * ((rounds + 1) // 2)
    order = base[:rounds]
    random.Random(seed).shuffle(order)
    return order


def _hotpath_json_arg(path: Path) -> list[str]:
    return ["--json-out", str(path)]


def build_hotpath_command(
    python: str | Path,
    output_path: Path,
    count: int,
    warmup: int,
    payload_sizes: list[int],
    transports: list[str],
    apis: list[str],
    num_buffers: int,
    quiet: bool,
) -> list[str]:
    cmd = [
        str(python),
        "-m",
        "ezmsg.util.perf.hotpath",
        "--count",
        str(count),
        "--warmup",
        str(warmup),
        "--samples",
        "1",
        "--num-buffers",
        str(num_buffers),
        "--payload-sizes",
        *[str(payload_size) for payload_size in payload_sizes],
        "--transports",
        *transports,
        "--apis",
        *apis,
        *_hotpath_json_arg(output_path),
    ]
    if quiet:
        cmd.append("--quiet")
    return cmd


def parse_env_assignments(values: list[str]) -> dict[str, str]:
    assignments: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"Environment override must use KEY=VALUE format: {value}")
        key, env_value = value.split("=", 1)
        if not key:
            raise ValueError(f"Environment override is missing a key: {value}")
        assignments[key] = env_value
    return assignments


def load_hotpath_summary(path: Path) -> dict[str, float]:
    payload = json.loads(path.read_text())
    return {
        entry["case_id"]: float(entry["summary"]["us_per_message_median"])
        for entry in payload["results"]
    }


def summarize_ab_results(
    ref_a: str,
    ref_b: str,
    rounds: int,
    seed: int,
    paired_runs: list[tuple[dict[str, float], dict[str, float]]],
    env_a: ABEnvironmentInfo | None = None,
    env_b: ABEnvironmentInfo | None = None,
    warnings: list[str] | None = None,
) -> ABRunSummary:
    case_ids = sorted(paired_runs[0][0].keys())
    cases: list[ABCaseSummary] = []

    for case_id in case_ids:
        a_values = [pair[0][case_id] for pair in paired_runs]
        b_values = [pair[1][case_id] for pair in paired_runs]
        deltas = [((b / a) - 1.0) * 100.0 for a, b in zip(a_values, b_values)]
        cases.append(
            ABCaseSummary(
                case_id=case_id,
                a_us_per_message_median=_median(a_values),
                b_us_per_message_median=_median(b_values),
                delta_pct_median=_median(deltas),
                delta_pct_mean=sum(deltas) / len(deltas),
                pair_count=len(deltas),
                b_faster_pairs=sum(1 for a, b in zip(a_values, b_values) if b < a),
            )
        )

    placeholder = ABEnvironmentInfo(
        label="?",
        ref="unknown",
        tree="unknown",
        python="unknown",
        python_version="unknown",
        ezmsg_version="unknown",
        numpy_version="unknown",
        git_commit="unknown",
        git_branch="unknown",
        dirty=False,
        env_mode="unknown",
        pyproject_hash=None,
        uv_lock_hash=None,
        env_overrides={},
    )
    return ABRunSummary(
        ref_a=ref_a,
        ref_b=ref_b,
        rounds=rounds,
        seed=seed,
        env_a=env_a or placeholder,
        env_b=env_b or placeholder,
        warnings=warnings or [],
        cases=cases,
    )


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _run_checked(cmd: list[str], cwd: Path, env: dict[str, str] | None = None) -> None:
    completed = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, env=env)
    if completed.returncode == 0:
        return

    raise RuntimeError(
        f"Command failed in {cwd}:\n"
        f"$ {' '.join(cmd)}\n\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )


def _run_json(cmd: list[str], cwd: Path, env: dict[str, str]) -> dict[str, str]:
    completed = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, env=env)
    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed in {cwd}:\n"
            f"$ {' '.join(cmd)}\n\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return json.loads(completed.stdout)


def _is_current_ref(ref: str) -> bool:
    return ref.upper() == "CURRENT"


@contextlib.contextmanager
def _provision_tree(
    repo_root: Path,
    ref: str,
    label: str,
    keep: bool,
) -> Path:
    if _is_current_ref(ref):
        yield repo_root
        return

    parent = Path(tempfile.mkdtemp(prefix=f"ezmsg-perf-{label.lower()}-"))
    tree_path = parent / "tree"
    _run_checked(
        ["git", "worktree", "add", "--detach", str(tree_path), ref],
        cwd=repo_root,
    )

    try:
        yield tree_path
    finally:
        if keep:
            return
        try:
            _run_checked(["git", "worktree", "remove", "--force", str(tree_path)], cwd=repo_root)
        finally:
            shutil.rmtree(parent, ignore_errors=True)


def _mirror_hotpath_module(source_root: Path, target_tree: Path) -> None:
    source = source_root / "src" / "ezmsg" / "util" / "perf" / "hotpath.py"
    target = target_tree / "src" / "ezmsg" / "util" / "perf" / "hotpath.py"
    shutil.copy2(source, target)


def _ensure_json_files_match(
    left: dict[str, float],
    right: dict[str, float],
    label_left: str,
    label_right: str,
) -> None:
    if left.keys() == right.keys():
        return

    raise RuntimeError(
        f"Benchmark cases differ between {label_left} and {label_right}: "
        f"{sorted(left.keys())} != {sorted(right.keys())}"
    )


def _python_from_venv(venv_dir: Path) -> Path | None:
    candidates = [venv_dir / "bin" / "python", venv_dir / "Scripts" / "python.exe"]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def discover_tree_python(tree: Path) -> Path | None:
    for dirname in VENV_DIR_CANDIDATES:
        python = _python_from_venv(tree / dirname)
        if python is not None:
            return python
    return None


def _virtual_env_from_python(python: Path) -> str | None:
    parent = python.parent
    if parent.name in {"bin", "Scripts"}:
        return str(parent.parent)
    return None


def _tree_env(
    base_env: dict[str, str],
    tree: Path,
    python: Path,
    overrides: dict[str, str],
) -> dict[str, str]:
    env = base_env.copy()
    env.update(overrides)
    path_parts = [str(tree / "src")]
    if env.get("PYTHONPATH"):
        path_parts.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(path_parts)
    venv = _virtual_env_from_python(python)
    if venv is not None:
        env["VIRTUAL_ENV"] = venv
    return env


def _hash_file(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


def _git_value(tree: Path, args: list[str], default: str = "unknown") -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=tree,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return default
    return completed.stdout.strip()


def _git_dirty(tree: Path) -> bool:
    completed = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=tree,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return False
    return bool(completed.stdout.strip())


def _runtime_probe_env(
    tree: Path,
    python: Path,
    overrides: dict[str, str],
) -> dict[str, str]:
    return _tree_env(os.environ.copy(), tree, python, overrides)


def _probe_runtime(tree: Path, python: Path, overrides: dict[str, str]) -> dict[str, str]:
    script = (
        "import json, sys\n"
        "payload = {\n"
        "  'python': sys.executable,\n"
        "  'python_version': sys.version.replace('\\n', ' '),\n"
        "  'ezmsg_version': 'unknown',\n"
        "  'numpy_version': 'unknown',\n"
        "}\n"
        "try:\n"
        "  import ezmsg.core as ez\n"
        "  payload['ezmsg_version'] = ez.__version__\n"
        "except Exception:\n"
        "  pass\n"
        "try:\n"
        "  import numpy as np\n"
        "  payload['numpy_version'] = np.__version__\n"
        "except Exception:\n"
        "  pass\n"
        "print(json.dumps(payload))\n"
    )
    return _run_json(
        [str(python), "-c", script],
        cwd=tree,
        env=_runtime_probe_env(tree, python, overrides),
    )


def _build_env_info(
    label: str,
    ref: str,
    tree: Path,
    python: Path,
    env_mode: str,
    overrides: dict[str, str],
) -> ABEnvironmentInfo:
    runtime = _probe_runtime(tree, python, overrides)
    return ABEnvironmentInfo(
        label=label,
        ref=ref,
        tree=str(tree),
        python=runtime["python"],
        python_version=runtime["python_version"],
        ezmsg_version=runtime["ezmsg_version"],
        numpy_version=runtime["numpy_version"],
        git_commit=_git_value(tree, ["rev-parse", "HEAD"]),
        git_branch=_git_value(tree, ["rev-parse", "--abbrev-ref", "HEAD"]),
        dirty=_git_dirty(tree),
        env_mode=env_mode,
        pyproject_hash=_hash_file(tree / "pyproject.toml"),
        uv_lock_hash=_hash_file(tree / "uv.lock"),
        env_overrides=overrides,
    )


def _shared_env_warnings(info_a: ABEnvironmentInfo, info_b: ABEnvironmentInfo) -> list[str]:
    warnings: list[str] = []
    if info_a.pyproject_hash != info_b.pyproject_hash:
        warnings.append(
            "pyproject.toml differs between A and B while using a shared environment."
        )
    if info_a.uv_lock_hash != info_b.uv_lock_hash:
        warnings.append(
            "uv.lock differs between A and B while using a shared environment."
        )
    return warnings


def _print_env_info(env_info: ABEnvironmentInfo) -> None:
    print(
        f"{env_info.label}: ref={env_info.ref} tree={env_info.tree} "
        f"python={env_info.python} env_mode={env_info.env_mode}"
    )
    print(
        f"   python_version={env_info.python_version} "
        f"ezmsg={env_info.ezmsg_version} numpy={env_info.numpy_version}"
    )
    print(
        f"   branch={env_info.git_branch} commit={env_info.git_commit} dirty={env_info.dirty}"
    )
    print(
        f"   pyproject_hash={env_info.pyproject_hash} uv_lock_hash={env_info.uv_lock_hash}"
    )
    if env_info.env_overrides:
        pairs = ", ".join(f"{key}={value}" for key, value in sorted(env_info.env_overrides.items()))
        print(f"   env_overrides={pairs}")


def _print_summary(summary: ABRunSummary) -> None:
    print(
        f"Interleaved hot-path comparison: A={summary.ref_a}, "
        f"B={summary.ref_b}, rounds={summary.rounds}, seed={summary.seed}"
    )
    _print_env_info(summary.env_a)
    _print_env_info(summary.env_b)
    for warning in summary.warnings:
        print(f"WARNING: {warning}")
    for case in summary.cases:
        sign = "regression" if case.delta_pct_median > 0 else "improvement"
        print(
            f"{case.case_id:<36} "
            f"A={case.a_us_per_message_median:>10.2f} us/msg "
            f"B={case.b_us_per_message_median:>10.2f} us/msg "
            f"delta={case.delta_pct_median:>7.2f}% ({sign}) "
            f"wins={case.b_faster_pairs}/{case.pair_count}"
        )


def dump_ab_json(summary: ABRunSummary, path: Path) -> None:
    payload = {
        "suite": "hotpath-ab",
        "ref_a": summary.ref_a,
        "ref_b": summary.ref_b,
        "rounds": summary.rounds,
        "seed": summary.seed,
        "env_a": asdict(summary.env_a),
        "env_b": asdict(summary.env_b),
        "warnings": summary.warnings,
        "cases": [asdict(case) for case in summary.cases],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def _default_ref(ref: str | None, fallback: str) -> str:
    return fallback if ref is None else ref


def _resolve_python_for_shared(
    python_a: str | None,
    python_b: str | None,
) -> Path:
    if python_a is not None and python_b is not None and python_a != python_b:
        raise ValueError(
            "shared env mode requires a single interpreter; --python-a and --python-b must match"
        )
    if python_a is not None:
        return Path(python_a)
    if python_b is not None:
        return Path(python_b)
    return Path(sys.executable)


def _resolve_python_for_existing(
    tree: Path,
    label: str,
    explicit_python: str | None,
    repo_root: Path,
) -> Path:
    if explicit_python is not None:
        return Path(explicit_python)

    discovered = discover_tree_python(tree)
    if discovered is not None:
        return discovered

    if tree.resolve() == repo_root.resolve():
        return Path(sys.executable)

    raise ValueError(
        f"Could not locate a Python interpreter for side {label} in {tree}. "
        "Prepare the environment yourself and rerun with --env-mode existing "
        "plus --python-a/--python-b or local .venv/venv directories."
    )


def perf_ab(
    ref_a: str | None,
    ref_b: str | None,
    dir_a: Path | None,
    dir_b: Path | None,
    python_a: str | None,
    python_b: str | None,
    env_mode: str,
    force_shared_env: bool,
    env: list[str],
    env_a: list[str],
    env_b: list[str],
    rounds: int,
    count: int,
    warmup: int,
    prewarm: int,
    payload_sizes: list[int],
    transports: list[str],
    apis: list[str],
    num_buffers: int,
    seed: int,
    json_out: Path | None,
    keep_worktrees: bool,
    quiet: bool,
) -> None:
    if rounds <= 0:
        raise ValueError("rounds must be > 0")
    if dir_a is not None and ref_a is not None:
        raise ValueError("Use either --ref-a or --dir-a, not both")
    if dir_b is not None and ref_b is not None:
        raise ValueError("Use either --ref-b or --dir-b, not both")
    if force_shared_env and env_mode != "shared":
        raise ValueError("--force-shared-env only applies to --env-mode shared")

    shared_overrides = parse_env_assignments(env)
    env_overrides_a = {**shared_overrides, **parse_env_assignments(env_a)}
    env_overrides_b = {**shared_overrides, **parse_env_assignments(env_b)}

    resolved_ref_a = dir_a.name if dir_a is not None else _default_ref(ref_a, DEFAULT_REF_A)
    resolved_ref_b = dir_b.name if dir_b is not None else _default_ref(ref_b, DEFAULT_REF_B)

    repo_root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    pair_order = build_pair_order(rounds, seed)

    with contextlib.ExitStack() as stack:
        tree_a = (
            dir_a.resolve()
            if dir_a is not None
            else stack.enter_context(_provision_tree(repo_root, resolved_ref_a, "A", keep_worktrees))
        )
        tree_b = (
            dir_b.resolve()
            if dir_b is not None
            else stack.enter_context(_provision_tree(repo_root, resolved_ref_b, "B", keep_worktrees))
        )

        if dir_a is None and tree_a != repo_root:
            _mirror_hotpath_module(repo_root, tree_a)
        if dir_b is None and tree_b != repo_root and tree_b != tree_a:
            _mirror_hotpath_module(repo_root, tree_b)

        if env_mode == "shared":
            shared_python = _resolve_python_for_shared(python_a, python_b)
            python_path_a = shared_python
            python_path_b = shared_python
        else:
            python_path_a = _resolve_python_for_existing(tree_a, "A", python_a, repo_root)
            python_path_b = _resolve_python_for_existing(tree_b, "B", python_b, repo_root)

        env_info_a = _build_env_info(
            "A", resolved_ref_a, tree_a, python_path_a, env_mode, env_overrides_a
        )
        env_info_b = _build_env_info(
            "B", resolved_ref_b, tree_b, python_path_b, env_mode, env_overrides_b
        )

        warnings = []
        if env_mode == "shared":
            warnings = _shared_env_warnings(env_info_a, env_info_b)
            if warnings and not force_shared_env:
                raise ValueError(
                    "Shared-environment comparison detected project metadata mismatches:\n"
                    + "\n".join(f"- {warning}" for warning in warnings)
                    + "\n\nRe-run with --force-shared-env to continue anyway, or prepare "
                    "side-specific environments and use --env-mode existing."
                )

        with tempfile.TemporaryDirectory(prefix="ezmsg-perf-ab-runs-") as tmpdir_name:
            tmpdir = Path(tmpdir_name)
            cmd_by_label = {
                "A": lambda path: build_hotpath_command(
                    python_path_a,
                    path,
                    count=count,
                    warmup=warmup,
                    payload_sizes=payload_sizes,
                    transports=transports,
                    apis=apis,
                    num_buffers=num_buffers,
                    quiet=quiet,
                ),
                "B": lambda path: build_hotpath_command(
                    python_path_b,
                    path,
                    count=count,
                    warmup=warmup,
                    payload_sizes=payload_sizes,
                    transports=transports,
                    apis=apis,
                    num_buffers=num_buffers,
                    quiet=quiet,
                ),
            }
            tree_by_label = {"A": tree_a, "B": tree_b}
            env_by_label = {
                "A": _tree_env(os.environ.copy(), tree_a, python_path_a, env_overrides_a),
                "B": _tree_env(os.environ.copy(), tree_b, python_path_b, env_overrides_b),
            }

            for idx in range(prewarm):
                for label in ("A", "B"):
                    if label == "B" and tree_b == tree_a and env_by_label["B"] == env_by_label["A"]:
                        continue
                    warm_path = tmpdir / f"warm-{label}-{idx}.json"
                    _run_checked(
                        cmd_by_label[label](warm_path),
                        cwd=tree_by_label[label],
                        env=env_by_label[label],
                    )

            paired_runs: list[tuple[dict[str, float], dict[str, float]]] = []
            for round_idx, (first, second) in enumerate(pair_order, start=1):
                outputs: dict[str, dict[str, float]] = {}
                for label in (first, second):
                    output_path = tmpdir / f"round-{round_idx:02d}-{label}.json"
                    _run_checked(
                        cmd_by_label[label](output_path),
                        cwd=tree_by_label[label],
                        env=env_by_label[label],
                    )
                    outputs[label] = load_hotpath_summary(output_path)

                _ensure_json_files_match(outputs["A"], outputs["B"], resolved_ref_a, resolved_ref_b)
                paired_runs.append((outputs["A"], outputs["B"]))

    summary = summarize_ab_results(
        resolved_ref_a,
        resolved_ref_b,
        rounds,
        seed,
        paired_runs,
        env_a=env_info_a,
        env_b=env_info_b,
        warnings=warnings,
    )
    _print_summary(summary)
    if json_out is not None:
        dump_ab_json(summary, json_out)
        print(f"Wrote JSON results to {json_out}")


def setup_ab_cmdline(subparsers: argparse._SubParsersAction) -> None:
    p_ab = subparsers.add_parser(
        "ab",
        help="run interleaved A/B hot-path comparisons using worktrees or prepared directories",
    )
    p_ab.add_argument("--ref-a", default=None, help=f"baseline git ref (default = {DEFAULT_REF_A})")
    p_ab.add_argument("--ref-b", default=None, help=f"candidate git ref (default = {DEFAULT_REF_B})")
    p_ab.add_argument("--dir-a", type=Path, default=None, help="use an existing directory for side A")
    p_ab.add_argument("--dir-b", type=Path, default=None, help="use an existing directory for side B")
    p_ab.add_argument("--python-a", default=None, help="explicit Python interpreter for side A")
    p_ab.add_argument("--python-b", default=None, help="explicit Python interpreter for side B")
    p_ab.add_argument(
        "--env-mode",
        choices=["shared", "existing"],
        default="shared",
        help="shared = reuse one interpreter for both sides; existing = use prepared per-tree environments",
    )
    p_ab.add_argument(
        "--force-shared-env",
        action="store_true",
        help="continue shared-env comparisons even when pyproject.toml or uv.lock differ",
    )
    p_ab.add_argument(
        "--env",
        action="append",
        default=[],
        help="environment override for both sides (KEY=VALUE). Repeatable.",
    )
    p_ab.add_argument(
        "--env-a",
        action="append",
        default=[],
        help="environment override for side A only (KEY=VALUE). Repeatable.",
    )
    p_ab.add_argument(
        "--env-b",
        action="append",
        default=[],
        help="environment override for side B only (KEY=VALUE). Repeatable.",
    )
    p_ab.add_argument(
        "--rounds",
        type=int,
        default=6,
        help="number of A/B pairs to run (default = 6)",
    )
    p_ab.add_argument(
        "--count",
        type=int,
        default=2_000,
        help="messages per hot-path sample (default = 2000)",
    )
    p_ab.add_argument(
        "--warmup",
        type=int,
        default=200,
        help="warmup messages per hot-path sample (default = 200)",
    )
    p_ab.add_argument(
        "--prewarm",
        type=int,
        default=1,
        help="unmeasured warmup invocations per side (default = 1)",
    )
    p_ab.add_argument(
        "--payload-sizes",
        nargs="*",
        type=int,
        default=[64, 4096],
        help="payload sizes in bytes (default = [64, 4096])",
    )
    p_ab.add_argument(
        "--transports",
        nargs="*",
        choices=["local", "shm", "tcp"],
        default=["local", "shm", "tcp"],
        help="transports to compare (default = ['local', 'shm', 'tcp'])",
    )
    p_ab.add_argument(
        "--apis",
        nargs="*",
        choices=["async", "sync"],
        default=["async"],
        help="apis to compare (default = ['async'])",
    )
    p_ab.add_argument(
        "--num-buffers",
        type=int,
        default=1,
        help="publisher buffers (default = 1)",
    )
    p_ab.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_PAIR_SEED,
        help="pair-order shuffle seed (default = 0)",
    )
    p_ab.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="optional JSON output path",
    )
    p_ab.add_argument(
        "--keep-worktrees",
        action="store_true",
        help="leave auto-provisioned worktrees on disk for inspection",
    )
    p_ab.add_argument(
        "--quiet",
        action="store_true",
        help="suppress ezmsg runtime logs in child benchmark runs",
    )
    p_ab.set_defaults(
        _handler=lambda ns: perf_ab(
            ref_a=ns.ref_a,
            ref_b=ns.ref_b,
            dir_a=ns.dir_a,
            dir_b=ns.dir_b,
            python_a=ns.python_a,
            python_b=ns.python_b,
            env_mode=ns.env_mode,
            force_shared_env=ns.force_shared_env,
            env=ns.env,
            env_a=ns.env_a,
            env_b=ns.env_b,
            rounds=ns.rounds,
            count=ns.count,
            warmup=ns.warmup,
            prewarm=ns.prewarm,
            payload_sizes=ns.payload_sizes,
            transports=ns.transports,
            apis=ns.apis,
            num_buffers=ns.num_buffers,
            seed=ns.seed,
            json_out=ns.json_out,
            keep_worktrees=ns.keep_worktrees,
            quiet=ns.quiet,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run interleaved ezmsg hot-path A/B comparisons."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    setup_ab_cmdline(subparsers)
    ns = parser.parse_args(["ab", *sys.argv[1:]])
    ns._handler(ns)
