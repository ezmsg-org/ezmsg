import json
import dataclasses
import argparse
import html
import math
import webbrowser

from pathlib import Path

from ..messagecodec import MessageDecoder
from .envinfo import TestEnvironmentInfo, format_env_diff
from .run import get_datestamp
from .impl import (
    TestParameters,
    Metrics,
    TestLogEntry,
)

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

    n_clients_axis = list(sorted(set([p.n_clients for p in all_results.keys()])))
    msg_size_axis = list(sorted(set([p.msg_size for p in all_results.keys()])))
    comms_axis = list(sorted(set([p.comms for p in all_results.keys()])))
    config_axis = list(sorted(set([p.config for p in all_results.keys()])))

    dims = ["n_clients", "msg_size", "comms", "config"]
    coords = {
        "n_clients": n_clients_axis,
        "msg_size": msg_size_axis,
        "comms": comms_axis,
        "config": config_axis,
    }

    data_vars = {}
    for field in dataclasses.fields(Metrics):
        m = (
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
        for p, a in all_results.items():
            # tests are run multiple times; get the median of means
            m[
                n_clients_axis.index(p.n_clients),
                msg_size_axis.index(p.msg_size),
                comms_axis.index(p.comms),
                config_axis.index(p.config),
            ] = np.median(
                [np.mean([getattr(v, field.name) for v in r]) for r in a.values()]
            )
        data_vars[field.name] = xr.DataArray(m, dims=dims, coords=coords)

    dataset = xr.Dataset(data_vars, attrs=dict(info=info))
    return dataset


def _escape(s: str) -> str:
    return html.escape(str(s), quote=True)


def _env_block(title: str, body: str) -> str:
    return f"""
    <section class="env">
      <h2>{_escape(title)}</h2>
      <pre>{_escape(body).strip()}</pre>
    </section>
    """


def _legend_block() -> str:
    return """
    <section class="legend">
      <h3>Legend</h3>
      <ul>
        <li><b>Comparison mode:</b> values are percentages (100 = no change).</li>
        <li><b>Green:</b> improvement (↑ sample/data rate, ↓ latency).</li>
        <li><b>Red:</b> regression (↓ sample/data rate, ↑ latency).</li>
      </ul>
    </section>
    """


def _base_css() -> str:
    # Minimal, print-friendly CSS + color scales for cells.
    return """
    <style>
      :root {
        --bg: #0b0b0c;
        --fg: #e7e7ea;
        --muted: #a0a0aa;
        --card: #141417;
        --accent: #7aa2f7;
        --green: 120; /* H hue for successes */
        --red: 0;     /* H hue for regressions */
      }
      @media (prefers-color-scheme: light) {
        :root {
          --bg: #ffffff;
          --fg: #16181c;
          --muted: #667085;
          --card: #f6f7fb;
          --accent: #1f6feb;
        }
      }
      html, body {
        background: var(--bg);
        color: var(--fg);
        font: 14px/1.45 system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
        margin: 0;
        padding: 0;
      }
      .wrap {
        max-width: 1100px;
        margin: 24px auto 80px auto;
        padding: 0 16px;
      }
      header h1 {
        font-size: 24px;
        margin: 0 0 4px 0;
      }
      header .sub {
        color: var(--muted);
        margin-bottom: 18px;
      }
      section.env, section.legend, section.group {
        background: var(--card);
        border-radius: 16px;
        padding: 16px;
        margin: 16px 0;
        box-shadow: 0 1px 0 rgba(0,0,0,0.05), 0 8px 24px rgba(0,0,0,0.12);
      }
      section.group h2 {
        margin: 0 0 10px 0;
        font-size: 18px;
      }
      pre {
        white-space: pre-wrap;
        word-wrap: break-word;
        margin: 0;
      }

      table {
        width: 100%;
        border-collapse: separate;
        border-spacing: 0;
        overflow: hidden;
        border-radius: 12px;
      }
      thead th {
        text-align: right;
        padding: 10px 12px;
        background: rgba(255,255,255,0.06);
        color: var(--fg);
        position: sticky;
        top: 0;
        z-index: 1;
      }
      thead th:first-child { text-align: left; }
      tbody td, tbody th {
        padding: 8px 12px;
        border-top: 1px solid rgba(255,255,255,0.06);
        text-align: right;
        vertical-align: middle;
        background: transparent;
      }
      tbody th { text-align: left; font-weight: 600; color: var(--fg); }
      tbody tr:nth-child(odd) td, tbody tr:nth-child(odd) th {
        background: rgba(255,255,255,0.02);
      }

      .pill {
        display: inline-block;
        font-size: 12px;
        padding: 2px 8px;
        border-radius: 999px;
        background: rgba(255,255,255,0.08);
        color: var(--fg);
        margin-right: 6px;
      }

      .metric-cell {
        border-radius: 8px;
      }
    </style>
    """


def _color_for_comparison(
    value: float, metric: str, noise_band_pct: float = 10.0
) -> str:
    """
    Returns inline CSS background for a comparison % value.
    value: e.g., 97.3, 104.8, etc.
    For sample_rate/data_rate: improvement > 100 (good).
    For latency_mean: improvement < 100 (good).
    Noise band ±10% around 100 is neutral.
    """
    if not (isinstance(value, (int, float)) and math.isfinite(value)):
        return ""

    delta = value - 100.0
    # Determine direction: + is good for sample/data; - is good for latency
    if "rate" in metric:
        # positive delta good, negative bad
        magnitude = abs(delta)
        sign_good = delta > 0
    elif "latency" in metric:
        # negative delta good (lower latency)
        magnitude = abs(delta)
        sign_good = delta < 0
    else:
        return ""

    # Noise band: keep neutral
    if magnitude <= noise_band_pct:
        return ""

    # Scale 5%..50% across 0..1; clamp
    scale = max(0.0, min(1.0, (magnitude - noise_band_pct) / 45.0))

    # Choose hue and lightness; use HSL with gentle saturation
    hue = "var(--green)" if sign_good else "var(--red)"
    # opacity via alpha blend on lightness via HSLa
    # Use saturation ~70%, lightness around 40–50% blended with table bg
    alpha = 0.15 + 0.35 * scale  # 0.15..0.50
    return f"background-color: hsla({hue}, 70%, 45%, {alpha});"


def _format_number(x) -> str:
    if isinstance(x, (int,)) and not isinstance(x, bool):
        return f"{x:d}"
    try:
        xf = float(x)
    except Exception:
        return _escape(str(x))
    # Heuristic: for comparison percentages, 1 decimal is nice; for absolute, 3 decimals for latency.
    return f"{xf:.3f}"


def summary(perf_path: Path, baseline_path: Path | None, html: bool = False) -> None:
    """print perf test results and comparisons to the console"""

    output = ""

    perf = load_perf(perf_path)
    info: TestEnvironmentInfo = perf.attrs["info"]
    output += str(info) + "\n\n"

    relative = False
    env_diff = None
    if baseline_path is not None:
        relative = True
        output += "PERFORMANCE COMPARISON\n\n"
        baseline = load_perf(baseline_path)
        perf = (perf / baseline) * 100.0
        baseline_info: TestEnvironmentInfo = baseline.attrs["info"]
        env_diff = format_env_diff(info.diff(baseline_info))
        output += env_diff + "\n\n"

        # These raw stats are still valuable to have, but are confusing
        # when making relative comparisons
        perf = perf.drop_vars(["latency_total", "num_msgs"])

    perf = perf.stack(params=["n_clients", "msg_size"]).dropna("params")
    df = perf.squeeze().to_dataframe()
    df = df.drop("n_clients", axis=1)
    df = df.drop("msg_size", axis=1)

    for _, config_ds in perf.groupby("config"):
        for _, comms_ds in config_ds.groupby("comms"):
            output += str(comms_ds.squeeze().to_dataframe()) + "\n\n"
        output += "\n"

    print(output)

    if html:
        # Ensure expected columns exist
        expected_cols = {
            "sample_rate_mean",
            "sample_rate_median",
            "data_rate",
            "latency_mean",
            "latency_median",
        }
        missing = expected_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing expected columns in dataset: {missing}")

        # We'll render a table per (config, comms) group.
        groups = (
            df.reset_index()
            .sort_values(by=["config", "comms", "n_clients", "msg_size"])
            .groupby(["config", "comms"], sort=False)
        )

        # Build HTML
        parts: list[str] = []
        parts.append("<!doctype html><html><head><meta charset='utf-8'>")
        parts.append(
            "<meta name='viewport' content='width=device-width, initial-scale=1' />"
        )
        parts.append("<title>ezmsg perf report</title>")
        parts.append(_base_css())
        parts.append("</head><body><div class='wrap'>")

        parts.append("<header>")
        parts.append("<h1>ezmsg Performance Report</h1>")
        sub = str(perf_path)
        if baseline_path is not None:
            sub += f" relative to {str(baseline_path)}"
        parts.append(f"<div class='sub'>{_escape(sub)}</div>")
        parts.append("</header>")

        if info is not None:
            parts.append(_env_block("Test Environment", str(info)))

        parts.append(_env_block("Test Details", TEST_DESCRIPTION))

        if env_diff is not None:
            # Show diffs using your helper
            parts.append("<section class='env'>")
            parts.append("<h2>Environment Differences vs Baseline</h2>")
            parts.append(f"<pre>{_escape(env_diff)}</pre>")
            parts.append("</section>")
            parts.append(_legend_block())

        # Render each group
        for (config, comms), g in groups:
            # Keep only expected columns in order
            cols = [
                "n_clients",
                "msg_size",
                "sample_rate_mean",
                "sample_rate_median",
                "data_rate",
                "latency_mean",
                "latency_median",
            ]
            g = g[cols].copy()

            # String format some columns (msg_size with separators)
            g["msg_size"] = g["msg_size"].map(
                lambda x: f"{int(x):,}" if pd.notna(x) else x
            )

            # Build table manually so we can inject inline cell styles easily
            # (pandas Styler is great but produces bulky HTML; manual keeps it clean)
            header = f"""
            <thead>
            <tr>
                <th>n_clients</th>
                <th>msg_size {"" if relative else "(b)"}</th>
                <th>sample_rate_mean {"" if relative else "(msgs/s)"}</th>
                <th>sample_rate_median {"" if relative else "(msgs/s)"}</th>
                <th>data_rate {"" if relative else "(MB/s)"}</th>
                <th>latency_mean {"" if relative else "(us)"}</th>
                <th>latency_median {"" if relative else "(us)"}<th>
            </tr>
            </thead>
            """
            body_rows: list[str] = []
            for _, row in g.iterrows():
                sr, srm, dr, lt, lm = (
                    row["sample_rate_mean"],
                    row["sample_rate_median"],
                    row["data_rate"],
                    row["latency_mean"],
                    row["latency_median"],
                )
                dr = dr if relative else dr / 2**20
                lt = lt if relative else lt * 1e6
                lm = lm if relative else lm * 1e6
                sr_style = (
                    _color_for_comparison(sr, "sample_rate_mean") if relative else ""
                )
                srm_style = (
                    _color_for_comparison(srm, "sample_rate_median") if relative else ""
                )
                dr_style = _color_for_comparison(dr, "data_rate") if relative else ""
                lt_style = _color_for_comparison(lt, "latency_mean") if relative else ""
                lm_style = (
                    _color_for_comparison(lm, "latency_median") if relative else ""
                )

                body_rows.append(
                    "<tr>"
                    f"<th>{_format_number(row['n_clients'])}</th>"
                    f"<td>{_escape(row['msg_size'])}</td>"
                    f"<td class='metric-cell' style='{sr_style}'>{_format_number(sr)}</td>"
                    f"<td class='metric-cell' style='{srm_style}'>{_format_number(srm)}</td>"
                    f"<td class='metric-cell' style='{dr_style}'>{_format_number(dr)}</td>"
                    f"<td class='metric-cell' style='{lt_style}'>{_format_number(lt)}</td>"
                    f"<td class='metric-cell' style='{lm_style}'>{_format_number(lm)}</td>"
                    "</tr>"
                )
            table_html = f"<table>{header}<tbody>{''.join(body_rows)}</tbody></table>"

            parts.append(
                f"<section class='group'><h2>"
                f"<span class='pill'>{_escape(config)}</span>"
                f"<span class='pill'>{_escape(comms)}</span>"
                f"</h2>{table_html}</section>"
            )

        parts.append("</div></body></html>")
        html_text = "".join(parts)

        out_path = Path(f"report_{get_datestamp()}.html")
        out_path.write_text(html_text, encoding="utf-8")
        webbrowser.open(out_path.resolve().as_uri())


def setup_summary_cmdline(subparsers: argparse._SubParsersAction) -> None:
    p_summary = subparsers.add_parser("summary", help="summarize performance results")
    p_summary.add_argument(
        "perf",
        type=Path,
        help="perf test",
    )
    p_summary.add_argument(
        "--baseline",
        "-b",
        type=Path,
        default=None,
        help="baseline perf test for comparison",
    )
    p_summary.add_argument(
        "--html",
        action="store_true",
        help="generate an html output file and render results in browser",
    )

    p_summary.set_defaults(
        _handler=lambda ns: summary(
            perf_path=ns.perf, baseline_path=ns.baseline, html=ns.html
        )
    )
