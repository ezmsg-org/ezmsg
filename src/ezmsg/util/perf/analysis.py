import json
import typing
import dataclasses
import argparse

from pathlib import Path

from ..messagecodec import MessageDecoder
from .envinfo import TestEnvironmentInfo, format_env_diff
from .ai import chatgpt_analyze_results
from .impl import (
    TestParameters, 
    Metrics,
)

import ezmsg.core as ez

try:
    import xarray as xr
except ImportError:
    ez.logger.error('ezmsg perf analysis requires xarray')
    raise

try:
    import numpy as np
except ImportError:
    ez.logger.error('ezmsg perf analysis requires numpy')
    raise


def load_perf(perf: Path) -> xr.Dataset:

    params: typing.List[TestParameters] = []
    results: typing.List[Metrics] = []

    with open(perf, 'r') as perf_f:
        info: TestEnvironmentInfo = json.loads(next(perf_f), cls = MessageDecoder)

        for line in perf_f:
            obj = json.loads(line, cls = MessageDecoder)
            params.append(obj['params'])
            results.append(obj['results'])

    n_clients_axis = list(sorted(set([p.n_clients for p in params])))
    msg_size_axis = list(sorted(set([p.msg_size for p in params])))
    comms_axis = list(sorted(set([p.comms for p in params])))
    config_axis = list(sorted(set([p.config for p in params])))

    dims = ['n_clients', 'msg_size', 'comms', 'config']
    coords = {
        'n_clients': n_clients_axis,
        'msg_size': msg_size_axis,
        'comms': comms_axis,
        'config': config_axis
    }
    
    data_vars = {}
    for field in dataclasses.fields(Metrics):
        m = np.zeros((
            len(n_clients_axis), 
            len(msg_size_axis), 
            len(comms_axis), 
            len(config_axis)
        )) * np.nan
        for p, r in zip(params, results):
            m[
                n_clients_axis.index(p.n_clients),
                msg_size_axis.index(p.msg_size),
                comms_axis.index(p.comms),
                config_axis.index(p.config)
            ] = getattr(r, field.name)
        data_vars[field.name] = xr.DataArray(m, dims = dims, coords = coords)

    dataset = xr.Dataset(data_vars, attrs = dict(info = info))
    return dataset


def summary(perf_path: Path, baseline_path: Path | None, ai: bool = False) -> None:
    """ print perf test results and comparisons to the console """

    output = ''

    perf = load_perf(perf_path)
    info: TestEnvironmentInfo = perf.attrs['info']
    output += str(info) + '\n\n'


    if baseline_path is not None:
        output += "PERFORMANCE COMPARISON\n\n"
        baseline = load_perf(baseline_path)
        perf = (perf / baseline) * 100.0
        baseline_info: TestEnvironmentInfo = baseline.attrs['info']
        output += format_env_diff(info.diff(baseline_info)) + '\n\n'

    # These raw stats are still valuable to have, but are confusing 
    # when making relative comparisons
    perf = perf.drop_vars(['latency_total', 'num_msgs'])

    for _, config_ds in perf.groupby('config'):
        for _, comms_ds in config_ds.groupby('comms'):
            output += str(comms_ds.squeeze().to_dataframe()) + '\n\n'
        output += '\n'

    print(output)

    if ai:
        print('Querying ChatGPT for AI-assisted analysis of performance test results')
        print(chatgpt_analyze_results(output))


def setup_summary_cmdline(subparsers: argparse._SubParsersAction) -> None:
    p_summary = subparsers.add_parser("summary", help = "summarize performance results")
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
        "--ai",
        action="store_true",
        help="ask chatgpt for an analysis of the results.  requires OPENAI_API_KEY set in environment"
    )

    p_summary.set_defaults(_handler=lambda ns: summary(
        perf_path = ns.perf,
        baseline_path = ns.baseline,
        ai = ns.ai
    ))