import json
import typing
import dataclasses

from pathlib import Path

from ..messagecodec import MessageDecoder
from .util import (
    TestEnvironmentInfo, 
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

@dataclasses.dataclass
class SummaryArgs:
    perf: Path
    baseline: Path | None

def summary(args: SummaryArgs):

    perf_dataset = load_perf(args.perf)

    for config, config_ds in perf_dataset.groupby('config'):
        for comms, comms_ds in config_ds.groupby('comms'):
            print(f'{config}: {comms}')
            print(comms_ds.sample_rate.data)

    if args.baseline is not None:
        baseline_dataset = load_perf(args.baseline)


def command() -> None:
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "perf",
        type=lambda x: Path(x),
        help="perf test",
    )

    parser.add_argument(
        "--baseline", "-b",
        type=lambda x: Path(x),
        default = None,
        help="baseline perf test for comparison"
    )

    args = parser.parse_args(namespace=SummaryArgs)

    summary(args)
