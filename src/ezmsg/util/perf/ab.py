from __future__ import annotations

import argparse
import contextlib
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
    cases: list[ABCaseSummary]


def build_pair_order(rounds: int, seed: int) -> list[tuple[str, str]]:
    base = [("A", "B"), ("B", "A")] * ((rounds + 1) // 2)
    order = base[:rounds]
    random.Random(seed).shuffle(order)
    return order


def _hotpath_json_arg(path: Path) -> list[str]:
    return ["--json-out", str(path)]


def build_hotpath_command(
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
        "uv",
        "run",
        "python",
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

    return ABRunSummary(
        ref_a=ref_a,
        ref_b=ref_b,
        rounds=rounds,
        seed=seed,
        cases=cases,
    )


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _run_checked(cmd: list[str], cwd: Path) -> None:
    env = os.environ.copy()
    env.pop("VIRTUAL_ENV", None)
    completed = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, env=env)
    if completed.returncode == 0:
        return

    raise RuntimeError(
        f"Command failed in {cwd}:\n"
        f"$ {' '.join(cmd)}\n\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )


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


def _maybe_sync(tree: Path) -> None:
    _run_checked(["uv", "sync", "--group", "dev"], cwd=tree)


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


def _print_summary(summary: ABRunSummary) -> None:
    print(
        f"Interleaved hot-path comparison: A={summary.ref_a}, "
        f"B={summary.ref_b}, rounds={summary.rounds}, seed={summary.seed}"
    )
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
        "cases": [asdict(case) for case in summary.cases],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def perf_ab(
    ref_a: str,
    ref_b: str,
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
    sync: bool,
    quiet: bool,
) -> None:
    if rounds <= 0:
        raise ValueError("rounds must be > 0")

    repo_root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    pair_order = build_pair_order(rounds, seed)

    with _provision_tree(repo_root, ref_a, "A", keep_worktrees) as tree_a:
        with _provision_tree(repo_root, ref_b, "B", keep_worktrees) as tree_b:
            if tree_a != repo_root:
                _mirror_hotpath_module(repo_root, tree_a)
            if tree_b != repo_root:
                _mirror_hotpath_module(repo_root, tree_b)

            if sync:
                _maybe_sync(tree_a)
                if tree_b != tree_a:
                    _maybe_sync(tree_b)

            with tempfile.TemporaryDirectory(prefix="ezmsg-perf-ab-runs-") as tmpdir_name:
                tmpdir = Path(tmpdir_name)
                cmd_by_label = {
                    "A": lambda path: build_hotpath_command(
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

                for idx in range(prewarm):
                    for label in ("A", "B"):
                        if label == "B" and tree_b == tree_a:
                            continue
                        warm_path = tmpdir / f"warm-{label}-{idx}.json"
                        _run_checked(cmd_by_label[label](warm_path), cwd=tree_by_label[label])

                paired_runs: list[tuple[dict[str, float], dict[str, float]]] = []
                for round_idx, (first, second) in enumerate(pair_order, start=1):
                    outputs: dict[str, dict[str, float]] = {}
                    for label in (first, second):
                        output_path = tmpdir / f"round-{round_idx:02d}-{label}.json"
                        _run_checked(cmd_by_label[label](output_path), cwd=tree_by_label[label])
                        outputs[label] = load_hotpath_summary(output_path)

                    _ensure_json_files_match(outputs["A"], outputs["B"], ref_a, ref_b)
                    paired_runs.append((outputs["A"], outputs["B"]))

    summary = summarize_ab_results(ref_a, ref_b, rounds, seed, paired_runs)
    _print_summary(summary)
    if json_out is not None:
        dump_ab_json(summary, json_out)
        print(f"Wrote JSON results to {json_out}")


def setup_ab_cmdline(subparsers: argparse._SubParsersAction) -> None:
    p_ab = subparsers.add_parser(
        "ab",
        help="run interleaved A/B hot-path comparisons using git worktrees",
    )
    p_ab.add_argument("--ref-a", default="dev", help="baseline git ref or CURRENT")
    p_ab.add_argument("--ref-b", default="CURRENT", help="candidate git ref or CURRENT")
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
        "--sync",
        action="store_true",
        help="run 'uv sync --group dev' in each provisioned worktree first",
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
            sync=ns.sync,
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
