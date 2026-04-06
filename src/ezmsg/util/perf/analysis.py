import argparse
import dataclasses
import html
import json
import math
import webbrowser

from pathlib import Path

from ..messagecodec import MessageDecoder
from .envinfo import TestEnvironmentInfo, format_env_diff
from .impl import Metrics, TestLogEntry, TestParameters

import ezmsg.core as ez

try:
    import xarray as xr
    import pandas as pd  # xarray depends on pandas
except ImportError:
    ez.logger.error("ezmsg perf analysis requires xarray")
    raise

try:
    import numpy as np
except ImportError:
    ez.logger.error("ezmsg perf analysis requires numpy")
    raise

TEST_DESCRIPTION = """
Configurations (config):
- fanin: many publishers to one subscriber
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

KEY_COLUMNS = ["config", "comms", "n_clients", "msg_size"]
DISPLAY_METRICS = [
    "sample_rate_mean",
    "sample_rate_median",
    "data_rate",
    "latency_mean",
    "latency_median",
]
METRIC_LABELS = {
    "sample_rate_mean": "sample_rate_mean",
    "sample_rate_median": "sample_rate_median",
    "data_rate": "data_rate",
    "latency_mean": "latency_mean",
    "latency_median": "latency_median",
}
METRIC_GROUPS = {
    "sample_rate_mean": "throughput",
    "sample_rate_median": "throughput",
    "data_rate": "throughput",
    "latency_mean": "latency",
    "latency_median": "latency",
}
ABSOLUTE_UNITS = {
    "sample_rate_mean": "msgs/s",
    "sample_rate_median": "msgs/s",
    "data_rate": "MB/s",
    "latency_mean": "us",
    "latency_median": "us",
}
RELATIVE_UNITS = {metric: "%" for metric in DISPLAY_METRICS}
NOISE_BAND_PCT = 10.0


@dataclasses.dataclass
class MetricDelta:
    metric: str
    config: str
    comms: str
    n_clients: int
    msg_size: int
    value: float
    score: float


@dataclasses.dataclass
class ReportBundle:
    perf_path: Path
    baseline_path: Path | None
    info: TestEnvironmentInfo
    baseline_info: TestEnvironmentInfo | None
    env_diff: str | None
    relative: bool
    terminal_df: pd.DataFrame
    candidate_df: pd.DataFrame
    baseline_df: pd.DataFrame | None
    relative_df: pd.DataFrame | None
    delta_counts: dict[str, int]
    top_improvements: list[MetricDelta]
    top_regressions: list[MetricDelta]


def load_perf(perf: Path) -> xr.Dataset:
    all_results: dict[TestParameters, dict[int, list[Metrics]]] = dict()
    run_idx = 0

    with open(perf, "r") as perf_f:
        info: TestEnvironmentInfo = json.loads(next(perf_f), cls=MessageDecoder)
        for line in perf_f:
            obj = json.loads(line, cls=MessageDecoder)
            if isinstance(obj, TestEnvironmentInfo):
                run_idx += 1
            elif isinstance(obj, TestLogEntry):
                runs = all_results.get(obj.params, dict())
                metrics = runs.get(run_idx, list())
                metrics.append(obj.results)
                runs[run_idx] = metrics
                all_results[obj.params] = runs

    n_clients_axis = list(sorted({p.n_clients for p in all_results}))
    msg_size_axis = list(sorted({p.msg_size for p in all_results}))
    comms_axis = list(sorted({p.comms for p in all_results}))
    config_axis = list(sorted({p.config for p in all_results}))

    dims = ["n_clients", "msg_size", "comms", "config"]
    coords = {
        "n_clients": n_clients_axis,
        "msg_size": msg_size_axis,
        "comms": comms_axis,
        "config": config_axis,
    }

    data_vars = {}
    for field in dataclasses.fields(Metrics):
        metric_values = (
            np.zeros(
                (
                    len(n_clients_axis),
                    len(msg_size_axis),
                    len(comms_axis),
                    len(config_axis),
                )
            )
            * np.nan
        )
        for params, runs in all_results.items():
            metric_values[
                n_clients_axis.index(params.n_clients),
                msg_size_axis.index(params.msg_size),
                comms_axis.index(params.comms),
                config_axis.index(params.config),
            ] = np.median(
                [np.mean([getattr(v, field.name) for v in run]) for run in runs.values()]
            )
        data_vars[field.name] = xr.DataArray(metric_values, dims=dims, coords=coords)

    return xr.Dataset(data_vars, attrs=dict(info=info))


def default_report_html_path(perf_path: Path) -> Path:
    return perf_path.with_suffix(".html")


def default_compare_html_path(perf_path: Path, baseline_path: Path) -> Path:
    return perf_path.with_name(f"{perf_path.stem}.vs_{baseline_path.stem}.html")


def _escape(value: object) -> str:
    return html.escape(str(value), quote=True)


def _frame_from_dataset(dataset: xr.Dataset) -> pd.DataFrame:
    frame = dataset.to_dataframe().dropna(how="all")
    frame = frame.reset_index()
    frame = frame.dropna(subset=DISPLAY_METRICS, how="all")
    return frame.sort_values(KEY_COLUMNS).reset_index(drop=True)


def _display_frame(frame: pd.DataFrame, relative: bool) -> pd.DataFrame:
    out = frame.copy()
    out = out[KEY_COLUMNS + DISPLAY_METRICS]
    if not relative:
        out["data_rate"] = out["data_rate"] / 2**20
        out["latency_mean"] = out["latency_mean"] * 1e6
        out["latency_median"] = out["latency_median"] * 1e6
    return out


def _metric_score(metric: str, value: float) -> float:
    if not (isinstance(value, (int, float)) and math.isfinite(value)):
        return 0.0
    if "latency" in metric:
        return 100.0 - value
    return value - 100.0


def _classify_metric(metric: str, value: float, noise_band_pct: float = NOISE_BAND_PCT) -> str:
    score = _metric_score(metric, value)
    if abs(score) <= noise_band_pct:
        return "neutral"
    return "improvement" if score > 0 else "regression"


def _collect_metric_deltas(
    relative_df: pd.DataFrame | None, noise_band_pct: float = NOISE_BAND_PCT
) -> tuple[dict[str, int], list[MetricDelta], list[MetricDelta]]:
    if relative_df is None:
        return {"improvement": 0, "neutral": 0, "regression": 0}, [], []

    counts = {"improvement": 0, "neutral": 0, "regression": 0}
    improvements: list[MetricDelta] = []
    regressions: list[MetricDelta] = []

    for _, row in relative_df.iterrows():
        for metric in DISPLAY_METRICS:
            value = float(row[metric])
            classification = _classify_metric(metric, value, noise_band_pct=noise_band_pct)
            counts[classification] += 1
            score = _metric_score(metric, value)
            if classification == "improvement":
                improvements.append(
                    MetricDelta(
                        metric=metric,
                        config=str(row["config"]),
                        comms=str(row["comms"]),
                        n_clients=int(row["n_clients"]),
                        msg_size=int(row["msg_size"]),
                        value=value,
                        score=score,
                    )
                )
            elif classification == "regression":
                regressions.append(
                    MetricDelta(
                        metric=metric,
                        config=str(row["config"]),
                        comms=str(row["comms"]),
                        n_clients=int(row["n_clients"]),
                        msg_size=int(row["msg_size"]),
                        value=value,
                        score=score,
                    )
                )

    improvements.sort(key=lambda item: item.score, reverse=True)
    regressions.sort(key=lambda item: item.score)
    return counts, improvements, regressions


def _format_terminal_value(metric: str, value: float, relative: bool) -> str:
    if pd.isna(value):
        return "nan"
    if relative:
        return f"{float(value):.1f}%"
    if metric in {"latency_mean", "latency_median"}:
        return f"{float(value):,.3f}"
    if metric == "data_rate":
        return f"{float(value):,.3f}"
    return f"{float(value):,.2f}"


def _terminal_table(frame: pd.DataFrame, relative: bool) -> str:
    formatted = frame.copy()
    for metric in DISPLAY_METRICS:
        formatted[metric] = formatted[metric].map(
            lambda value, metric=metric: _format_terminal_value(metric, value, relative)
        )

    sections: list[str] = []
    for (config, comms), group in formatted.groupby(["config", "comms"], sort=False):
        sections.append(f"{config} / {comms}")
        sections.append(
            group[["n_clients", "msg_size", *DISPLAY_METRICS]].to_string(index=False)
        )
        sections.append("")
    return "\n".join(sections).strip()


def _terminal_delta_summary(
    counts: dict[str, int],
    improvements: list[MetricDelta],
    regressions: list[MetricDelta],
    limit: int = 5,
) -> str:
    lines = [
        "COMPARISON OVERVIEW",
        (
            f"  improvements: {counts['improvement']}, neutral: {counts['neutral']}, "
            f"regressions: {counts['regression']}"
        ),
        "",
    ]

    if regressions:
        lines.append("BIGGEST REGRESSIONS")
        for delta in regressions[:limit]:
            lines.append(
                "  "
                f"{delta.metric}: {delta.value:.1f}% "
                f"({delta.config}/{delta.comms}, n_clients={delta.n_clients}, msg_size={delta.msg_size})"
            )
        lines.append("")

    if improvements:
        lines.append("BIGGEST IMPROVEMENTS")
        for delta in improvements[:limit]:
            lines.append(
                "  "
                f"{delta.metric}: {delta.value:.1f}% "
                f"({delta.config}/{delta.comms}, n_clients={delta.n_clients}, msg_size={delta.msg_size})"
            )
        lines.append("")

    return "\n".join(lines).strip()


def build_report_bundle(perf_path: Path, baseline_path: Path | None = None) -> ReportBundle:
    candidate = load_perf(perf_path)
    info: TestEnvironmentInfo = candidate.attrs["info"]
    candidate_frame = _display_frame(_frame_from_dataset(candidate), relative=False)

    baseline_info: TestEnvironmentInfo | None = None
    env_diff: str | None = None
    relative = baseline_path is not None
    baseline_frame: pd.DataFrame | None = None
    relative_frame: pd.DataFrame | None = None

    if baseline_path is not None:
        baseline = load_perf(baseline_path)
        baseline_info = baseline.attrs["info"]
        env_diff = format_env_diff(info.diff(baseline_info))
        baseline_frame = _display_frame(_frame_from_dataset(baseline), relative=False)
        relative_dataset = (candidate / baseline) * 100.0
        relative_dataset = relative_dataset.drop_vars(["latency_total", "num_msgs"])
        relative_frame = _display_frame(_frame_from_dataset(relative_dataset), relative=True)
        terminal_df = relative_frame
    else:
        terminal_df = candidate_frame

    delta_counts, top_improvements, top_regressions = _collect_metric_deltas(relative_frame)

    return ReportBundle(
        perf_path=perf_path,
        baseline_path=baseline_path,
        info=info,
        baseline_info=baseline_info,
        env_diff=env_diff,
        relative=relative,
        terminal_df=terminal_df,
        candidate_df=candidate_frame,
        baseline_df=baseline_frame,
        relative_df=relative_frame,
        delta_counts=delta_counts,
        top_improvements=top_improvements,
        top_regressions=top_regressions,
    )


def _build_terminal_output(bundle: ReportBundle) -> str:
    lines = [str(bundle.info), ""]

    if bundle.relative:
        lines.extend(
            [
                "PERFORMANCE COMPARISON",
                "",
                bundle.env_diff or "No differences.",
                "",
                _terminal_delta_summary(
                    bundle.delta_counts, bundle.top_improvements, bundle.top_regressions
                ),
                "",
            ]
        )
    else:
        lines.extend(["PERFORMANCE REPORT", ""])

    lines.extend([_terminal_table(bundle.terminal_df, relative=bundle.relative), ""])
    return "\n".join(line for line in lines if line is not None).strip()


def _format_html_number(metric: str, value: float, mode: str) -> str:
    if not (isinstance(value, (int, float)) and math.isfinite(value)):
        return "n/a"
    if mode == "relative":
        return f"{value:.1f}%"
    if metric in {"latency_mean", "latency_median"}:
        return f"{value:,.3f}"
    if metric == "data_rate":
        return f"{value:,.3f}"
    return f"{value:,.2f}"


def _color_for_comparison(
    value: float, metric: str, noise_band_pct: float = NOISE_BAND_PCT
) -> str:
    if not (isinstance(value, (int, float)) and math.isfinite(value)):
        return ""
    score = _metric_score(metric, value)
    magnitude = abs(score)
    if magnitude <= noise_band_pct:
        return ""

    scale = max(0.0, min(1.0, (magnitude - noise_band_pct) / 45.0))
    hue = "var(--green)" if score > 0 else "var(--red)"
    alpha = 0.15 + 0.35 * scale
    return f"background-color: hsla({hue}, 70%, 45%, {alpha});"


def _base_css() -> str:
    return """
    <style>
      :root {
        --bg: #f4f0e8;
        --fg: #18202a;
        --muted: #5d6a76;
        --card: rgba(255, 255, 255, 0.82);
        --card-strong: rgba(255, 255, 255, 0.94);
        --accent: #0d6b61;
        --accent-soft: #d9efe9;
        --border: rgba(24, 32, 42, 0.12);
        --green: 130;
        --red: 8;
        --shadow: 0 14px 38px rgba(32, 40, 54, 0.10);
      }
      html, body {
        margin: 0;
        padding: 0;
        background:
          radial-gradient(circle at top left, rgba(13, 107, 97, 0.12), transparent 28%),
          radial-gradient(circle at top right, rgba(199, 138, 64, 0.12), transparent 24%),
          linear-gradient(180deg, #faf7f1 0%, #f1ece2 100%);
        color: var(--fg);
        font: 14px/1.45 "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
      }
      .wrap {
        max-width: 1280px;
        margin: 24px auto 80px auto;
        padding: 0 18px;
      }
      header, section {
        background: var(--card);
        backdrop-filter: blur(10px);
        border: 1px solid var(--border);
        border-radius: 20px;
        box-shadow: var(--shadow);
      }
      header {
        padding: 22px 24px;
      }
      header h1 {
        margin: 0 0 6px 0;
        font-size: 28px;
        line-height: 1.1;
      }
      .sub {
        color: var(--muted);
      }
      .meta-grid, .overview-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 12px;
      }
      section {
        padding: 18px 20px;
        margin-top: 16px;
      }
      section h2 {
        margin: 0 0 10px 0;
        font-size: 18px;
      }
      .overview-card {
        background: var(--card-strong);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 14px 16px;
      }
      .overview-card .label {
        display: block;
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }
      .overview-card .value {
        display: block;
        font-size: 28px;
        font-weight: 700;
        margin-top: 4px;
      }
      .control-bar {
        position: sticky;
        top: 12px;
        z-index: 3;
        background: rgba(255, 255, 255, 0.90);
      }
      .control-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
        gap: 10px;
      }
      label.control {
        display: flex;
        flex-direction: column;
        gap: 4px;
        font-size: 12px;
        color: var(--muted);
      }
      select, button {
        font: inherit;
      }
      select {
        border-radius: 12px;
        border: 1px solid var(--border);
        background: #fff;
        padding: 8px 10px;
        color: var(--fg);
      }
      .button-row {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }
      .mode-btn {
        border: 1px solid var(--border);
        background: #fff;
        border-radius: 999px;
        padding: 8px 12px;
        cursor: pointer;
      }
      .mode-btn.active {
        background: var(--accent);
        color: #fff;
        border-color: var(--accent);
      }
      .callout-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
        gap: 14px;
      }
      .callout-list {
        margin: 0;
        padding-left: 18px;
      }
      .callout-list li {
        margin-bottom: 8px;
      }
      pre {
        margin: 0;
        white-space: pre-wrap;
        word-break: break-word;
      }
      table {
        width: 100%;
        border-collapse: separate;
        border-spacing: 0;
      }
      thead th {
        position: sticky;
        top: 120px;
        z-index: 1;
        background: #f6f7f5;
        padding: 10px 12px;
        border-bottom: 1px solid var(--border);
        text-align: right;
        white-space: nowrap;
      }
      thead th:first-child,
      thead th:nth-child(2),
      thead th:nth-child(3),
      thead th:nth-child(4) {
        text-align: left;
      }
      tbody td {
        padding: 9px 12px;
        border-top: 1px solid var(--border);
        text-align: right;
      }
      tbody td.left {
        text-align: left;
      }
      tbody tr:nth-child(odd) td {
        background: rgba(255, 255, 255, 0.42);
      }
      .metric-cell {
        border-radius: 10px;
        transition: background-color 0.2s ease, opacity 0.2s ease;
      }
      .muted {
        color: var(--muted);
      }
      .hidden {
        display: none;
      }
      @media (max-width: 900px) {
        .wrap {
          padding: 0 12px;
        }
        header, section {
          border-radius: 16px;
        }
        thead th {
          top: 146px;
        }
      }
    </style>
    """


def _render_delta_list(title: str, deltas: list[MetricDelta], empty: str) -> str:
    if not deltas:
        return f"<section><h2>{_escape(title)}</h2><div class='muted'>{_escape(empty)}</div></section>"

    items = []
    for delta in deltas[:5]:
        items.append(
            "<li>"
            f"<b>{_escape(delta.metric)}</b>: {_escape(f'{delta.value:.1f}%')} "
            f"<span class='muted'>({_escape(delta.config)}/{_escape(delta.comms)}, "
            f"n_clients={delta.n_clients}, msg_size={delta.msg_size})</span>"
            "</li>"
        )
    return (
        f"<section><h2>{_escape(title)}</h2>"
        f"<ol class='callout-list'>{''.join(items)}</ol></section>"
    )


def _build_rows(bundle: ReportBundle) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    baseline_lookup: dict[tuple[str, str, int, int], dict[str, object]] = {}
    relative_lookup: dict[tuple[str, str, int, int], dict[str, object]] = {}

    if bundle.baseline_df is not None:
        for row in bundle.baseline_df.to_dict("records"):
            key = (str(row["config"]), str(row["comms"]), int(row["n_clients"]), int(row["msg_size"]))
            baseline_lookup[key] = row

    if bundle.relative_df is not None:
        for row in bundle.relative_df.to_dict("records"):
            key = (str(row["config"]), str(row["comms"]), int(row["n_clients"]), int(row["msg_size"]))
            relative_lookup[key] = row

    for row in bundle.candidate_df.to_dict("records"):
        key = (str(row["config"]), str(row["comms"]), int(row["n_clients"]), int(row["msg_size"]))
        baseline_row = baseline_lookup.get(key)
        relative_row = relative_lookup.get(key)
        severity = 0.0
        if relative_row is not None:
            severity = max(abs(_metric_score(metric, float(relative_row[metric]))) for metric in DISPLAY_METRICS)
        rows.append(
            {
                "config": key[0],
                "comms": key[1],
                "n_clients": key[2],
                "msg_size": key[3],
                "candidate": row,
                "baseline": baseline_row,
                "relative": relative_row,
                "severity": severity,
            }
        )
    return rows


def _options(values: list[object], label: str) -> str:
    options = ["<option value=''>All</option>"]
    for value in values:
        options.append(f"<option value='{_escape(value)}'>{_escape(value)}</option>")
    return (
        f"<label class='control'>{_escape(label)}"
        f"<select data-filter='{_escape(label.lower())}'>{''.join(options)}</select></label>"
    )


def _report_title(bundle: ReportBundle) -> str:
    return "ezmsg Performance Comparison" if bundle.relative else "ezmsg Performance Report"


def render_html_report(bundle: ReportBundle) -> str:
    rows = _build_rows(bundle)
    filters = {
        "config": sorted(bundle.candidate_df["config"].unique().tolist()),
        "comms": sorted(bundle.candidate_df["comms"].unique().tolist()),
        "n_clients": sorted(bundle.candidate_df["n_clients"].unique().tolist()),
        "msg_size": sorted(bundle.candidate_df["msg_size"].unique().tolist()),
    }
    mode = "relative" if bundle.relative else "candidate"

    parts: list[str] = []
    parts.append("<!doctype html><html><head><meta charset='utf-8'>")
    parts.append(
        "<meta name='viewport' content='width=device-width, initial-scale=1' />"
    )
    parts.append(f"<title>{_escape(_report_title(bundle))}</title>")
    parts.append(_base_css())
    parts.append("</head><body><div class='wrap'>")

    parts.append("<header>")
    parts.append(f"<h1>{_escape(_report_title(bundle))}</h1>")
    sub = str(bundle.perf_path)
    if bundle.baseline_path is not None:
        sub += f" relative to {bundle.baseline_path}"
    parts.append(f"<div class='sub'>{_escape(sub)}</div>")
    parts.append("</header>")

    if bundle.relative:
        parts.append("<section><h2>Comparison Overview</h2><div class='overview-grid'>")
        for label, value in [
            ("Improvements", bundle.delta_counts["improvement"]),
            ("Neutral", bundle.delta_counts["neutral"]),
            ("Regressions", bundle.delta_counts["regression"]),
        ]:
            parts.append(
                "<div class='overview-card'>"
                f"<span class='label'>{_escape(label)}</span>"
                f"<span class='value'>{_escape(value)}</span>"
                "</div>"
            )
        parts.append("</div></section>")

        parts.append("<div class='callout-grid'>")
        parts.append(
            _render_delta_list(
                "Biggest Regressions",
                bundle.top_regressions,
                "No regressions outside the noise band.",
            )
        )
        parts.append(
            _render_delta_list(
                "Biggest Improvements",
                bundle.top_improvements,
                "No improvements outside the noise band.",
            )
        )
        parts.append("</div>")

    parts.append("<section>")
    parts.append("<h2>Environment</h2>")
    parts.append(f"<pre>{_escape(str(bundle.info))}</pre>")
    parts.append("</section>")

    if bundle.env_diff is not None:
        parts.append("<section class='env-diff'>")
        parts.append("<h2>Environment Differences vs Baseline</h2>")
        parts.append(f"<pre>{_escape(bundle.env_diff)}</pre>")
        parts.append("</section>")

    parts.append("<section><h2>Test Details</h2>")
    parts.append(f"<pre>{_escape(TEST_DESCRIPTION.strip())}</pre>")
    parts.append("</section>")

    parts.append("<section class='control-bar'>")
    parts.append("<h2>Explore</h2>")
    parts.append("<div class='control-grid'>")
    parts.append(_options(filters["config"], "Config"))
    parts.append(_options(filters["comms"], "Comms"))
    parts.append(_options(filters["n_clients"], "N_clients"))
    parts.append(_options(filters["msg_size"], "Msg_size"))
    parts.append(
        "<label class='control'>Sort"
        "<select id='sort-by'>"
        "<option value='config'>Config</option>"
        "<option value='comms'>Comms</option>"
        "<option value='n_clients'>n_clients</option>"
        "<option value='msg_size'>msg_size</option>"
        "<option value='severity'>Largest change</option>"
        "</select></label>"
    )
    parts.append("</div>")
    parts.append("<div class='button-row' style='margin-top:12px'>")
    for view, label in [("all", "All Metrics"), ("throughput", "Throughput"), ("latency", "Latency")]:
        active = " active" if view == "all" else ""
        parts.append(
            f"<button class='mode-btn metric-view{active}' data-view='{_escape(view)}'>{_escape(label)}</button>"
        )
    if bundle.relative:
        parts.append("<span style='width:12px'></span>")
        for display_mode, label in [
            ("relative", "Relative"),
            ("candidate", "Candidate"),
            ("baseline", "Baseline"),
        ]:
            active = " active" if display_mode == mode else ""
            parts.append(
                f"<button class='mode-btn display-mode{active}' data-mode='{_escape(display_mode)}'>{_escape(label)}</button>"
            )
    parts.append("</div>")
    parts.append("</section>")

    parts.append("<section><h2>Results</h2><table id='results-table'><thead><tr>")
    for column in ["config", "comms", "n_clients", "msg_size"]:
        parts.append(f"<th data-key='{_escape(column)}'>{_escape(column)}</th>")
    for metric in DISPLAY_METRICS:
        parts.append(
            f"<th class='metric-header metric-{_escape(METRIC_GROUPS[metric])}' data-metric='{_escape(metric)}'></th>"
        )
    parts.append("</tr></thead><tbody>")

    for row in rows:
        parts.append(
            "<tr "
            f"data-config='{_escape(row['config'])}' "
            f"data-comms='{_escape(row['comms'])}' "
            f"data-n_clients='{_escape(row['n_clients'])}' "
            f"data-msg_size='{_escape(row['msg_size'])}' "
            f"data-severity='{float(row['severity']):.6f}'>"
        )
        for column in ["config", "comms", "n_clients", "msg_size"]:
            parts.append(f"<td class='left'>{_escape(row[column])}</td>")

        candidate = row["candidate"]
        baseline = row["baseline"]
        relative_row = row["relative"]
        for metric in DISPLAY_METRICS:
            candidate_value = float(candidate[metric])
            baseline_value = (
                float(baseline[metric]) if baseline is not None else float("nan")
            )
            relative_value = (
                float(relative_row[metric]) if relative_row is not None else float("nan")
            )
            style = (
                _color_for_comparison(relative_value, metric)
                if bundle.relative and math.isfinite(relative_value)
                else ""
            )
            initial_value = (
                relative_value if bundle.relative else candidate_value
            )
            initial_mode = "relative" if bundle.relative else "candidate"
            parts.append(
                "<td "
                f"class='metric-cell metric-{_escape(METRIC_GROUPS[metric])}' "
                f"style='{style}' "
                f"data-metric='{_escape(metric)}' "
                f"data-candidate='{candidate_value}' "
                f"data-baseline='{baseline_value}' "
                f"data-relative='{relative_value}' "
                f"data-mode='{_escape(initial_mode)}'>"
                f"{_escape(_format_html_number(metric, initial_value, initial_mode))}"
                "</td>"
            )
        parts.append("</tr>")

    parts.append("</tbody></table></section>")
    parts.append(
        """
        <script>
        const metricLabels = %s;
        const absoluteUnits = %s;
        const relativeUnits = %s;
        let displayMode = %s;
        let metricView = "all";

        const rows = Array.from(document.querySelectorAll("#results-table tbody tr"));
        const headers = Array.from(document.querySelectorAll(".metric-header"));
        const displayButtons = Array.from(document.querySelectorAll(".display-mode"));
        const viewButtons = Array.from(document.querySelectorAll(".metric-view"));
        const filters = {
          config: document.querySelector("select[data-filter='config']"),
          comms: document.querySelector("select[data-filter='comms']"),
          n_clients: document.querySelector("select[data-filter='n_clients']"),
          msg_size: document.querySelector("select[data-filter='msg_size']"),
        };
        const sortBy = document.getElementById("sort-by");

        function formatValue(metric, raw, mode) {
          const value = Number(raw);
          if (!Number.isFinite(value)) return "n/a";
          if (mode === "relative") return `${value.toFixed(1)}%%`;
          if (metric.startsWith("latency")) return value.toLocaleString(undefined, {minimumFractionDigits: 3, maximumFractionDigits: 3});
          if (metric === "data_rate") return value.toLocaleString(undefined, {minimumFractionDigits: 3, maximumFractionDigits: 3});
          return value.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
        }

        function updateHeaders() {
          headers.forEach((header) => {
            const metric = header.dataset.metric;
            const units = displayMode === "relative" ? relativeUnits[metric] : absoluteUnits[metric];
            header.textContent = `${metricLabels[metric]} (${units})`;
          });
        }

        function updateCells() {
          document.querySelectorAll(".metric-cell").forEach((cell) => {
            const metric = cell.dataset.metric;
            const raw = cell.dataset[displayMode];
            cell.textContent = formatValue(metric, raw, displayMode);
          });
        }

        function updateView() {
          document.querySelectorAll(".metric-cell, .metric-header").forEach((element) => {
            const isLatency = element.classList.contains("metric-latency");
            const isThroughput = element.classList.contains("metric-throughput");
            const hide =
              metricView === "latency" ? isThroughput :
              metricView === "throughput" ? isLatency :
              false;
            element.classList.toggle("hidden", hide);
          });
        }

        function updateActiveButtons(buttons, current, attr) {
          buttons.forEach((button) => {
            button.classList.toggle("active", button.dataset[attr] === current);
          });
        }

        function rowMatchesFilters(row) {
          return Object.entries(filters).every(([key, element]) => {
            if (!element || !element.value) return true;
            return row.dataset[key] === element.value;
          });
        }

        function sortRows() {
          const value = sortBy ? sortBy.value : "config";
          const sorted = rows.slice().sort((a, b) => {
            if (value === "severity") {
              return Number(b.dataset.severity) - Number(a.dataset.severity);
            }
            if (value === "n_clients" || value === "msg_size") {
              return Number(a.dataset[value]) - Number(b.dataset[value]);
            }
            return a.dataset[value].localeCompare(b.dataset[value]);
          });
          const tbody = document.querySelector("#results-table tbody");
          sorted.forEach((row) => tbody.appendChild(row));
        }

        function applyFilters() {
          rows.forEach((row) => {
            row.classList.toggle("hidden", !rowMatchesFilters(row));
          });
        }

        Object.values(filters).forEach((element) => element && element.addEventListener("change", applyFilters));
        if (sortBy) sortBy.addEventListener("change", sortRows);

        displayButtons.forEach((button) => {
          button.addEventListener("click", () => {
            displayMode = button.dataset.mode;
            updateActiveButtons(displayButtons, displayMode, "mode");
            updateHeaders();
            updateCells();
          });
        });
        viewButtons.forEach((button) => {
          button.addEventListener("click", () => {
            metricView = button.dataset.view;
            updateActiveButtons(viewButtons, metricView, "view");
            updateView();
          });
        });

        sortRows();
        applyFilters();
        updateHeaders();
        updateCells();
        updateView();
        updateActiveButtons(viewButtons, metricView, "view");
        updateActiveButtons(displayButtons, displayMode, "mode");
        </script>
        """
        % (
            json.dumps(METRIC_LABELS),
            json.dumps(ABSOLUTE_UNITS),
            json.dumps(RELATIVE_UNITS),
            json.dumps(mode),
        )
    )
    parts.append("</div></body></html>")
    return "".join(parts)


def write_html_report(
    perf_path: Path,
    baseline_path: Path | None = None,
    output_path: Path | None = None,
    open_browser: bool = False,
) -> Path:
    bundle = build_report_bundle(perf_path, baseline_path=baseline_path)
    if output_path is None:
        output_path = (
            default_compare_html_path(perf_path, baseline_path)
            if baseline_path is not None
            else default_report_html_path(perf_path)
        )
    html_text = render_html_report(bundle)
    output_path.write_text(html_text, encoding="utf-8")
    if open_browser:
        webbrowser.open(output_path.resolve().as_uri())
    return output_path


def report(
    perf_path: Path,
    output_path: Path | None = None,
    open_browser: bool = True,
) -> None:
    bundle = build_report_bundle(perf_path)
    print(_build_terminal_output(bundle))
    out_path = write_html_report(
        perf_path=perf_path,
        output_path=output_path,
        open_browser=open_browser,
    )
    print(f"\nHTML report: {out_path}")


def compare(
    perf_path: Path,
    baseline_path: Path,
    output_path: Path | None = None,
    open_browser: bool = True,
) -> None:
    bundle = build_report_bundle(perf_path, baseline_path=baseline_path)
    print(_build_terminal_output(bundle))
    out_path = write_html_report(
        perf_path=perf_path,
        baseline_path=baseline_path,
        output_path=output_path,
        open_browser=open_browser,
    )
    print(f"\nComparison report: {out_path}")


def setup_report_cmdline(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "report", help="render an absolute report for a benchmark file"
    )
    parser.add_argument("perf", type=Path, help="benchmark file")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="optional explicit HTML output path",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="write HTML without opening it in a browser",
    )
    parser.set_defaults(
        _handler=lambda ns: report(
            perf_path=ns.perf,
            output_path=ns.output,
            open_browser=not ns.no_browser,
        )
    )


def setup_compare_cmdline(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "compare", help="compare one benchmark file against a baseline"
    )
    parser.add_argument("perf", type=Path, help="candidate benchmark file")
    parser.add_argument(
        "--baseline",
        "-b",
        type=Path,
        required=True,
        help="baseline benchmark file for comparison",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="optional explicit HTML output path",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="write HTML without opening it in a browser",
    )
    parser.set_defaults(
        _handler=lambda ns: compare(
            perf_path=ns.perf,
            baseline_path=ns.baseline,
            output_path=ns.output,
            open_browser=not ns.no_browser,
        )
    )
