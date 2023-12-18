# <img src="https://raw.githubusercontent.com/iscoe/ezmsg/main/docs/source/logo.png" width="50"/> ezmsg

Messaging and Multiprocessing.

`ezmsg` is a pure-Python implementation of a directed acyclic graph (DAG) pub/sub messaging pattern based on [`labgraph`](https://github.com/facebookresearch/labgraph) which is optimized and intended for use in constructing real-time software. `ezmsg` implements much of the `labgraph` API (with a few notable differences), and owes a lot of its design to the `labgraph` developers/project.

`ezmsg` is very fast and uses Python's `multiprocessing.shared_memory` module to facilitate efficient message passing without C++ or any compilation/build tooling.

## Installation

`pip install ezmsg`

## Dependencies

- Due to reliance on `multiprocessing.shared_memory`, `ezmsg` requires **minimum Python 3.8**.
- `typing_extensions`

Testing `ezmsg` requires:

- `pytest`
- `pytest-cov`
- `pytest-asyncio`
- `numpy`

## Setup (Development)

```bash
$ python3 -m venv env
$ source env/bin/activate
(env) $ pip install --upgrade pip
(env) $ pip install -e ".[test]"

(env) $ python -m pytest tests # Optionally, Perform tests
```

## Documentation

https://ezmsg.readthedocs.io/en/latest/

`ezmsg` is very similar to [`labgraph`](https://www.github.com/facebookresearch/labgraph), so you might get a primer with their documentation and examples. Additionally, there are many examples provided in the examples/tests directories strewn throughout this repository.

## Extensions

See the extension directory for more details

- `ezmsg-sigproc` -- Timeseries signal processing modules
- `ezmsg-websocket` -- Websocket server and client nodes for `ezmsg` graphs
- `ezmsg-zmq` -- ZeroMQ pub and sub nodes for `ezmsg` graphs
- ... More to come!

Additionally, the following extensions are contained in external repositories:

- [ezmsg-panel](https://github.com/griffinmilsap/ezmsg-panel) -- Plotting tools for `ezmsg` that use [panel](https://github.com/holoviz/panel)
- [ezmsg-blackrock](https://github.com/griffinmilsap/ezmsg-blackrock) -- Interface for Blackrock Cerebus ecosystem (incl. Neuroport) using `pycbsdk`
- [ezmsg-unicorn](https://github.com/griffinmilsap/ezmsg-unicorn) -- g.tec Unicorn Hybrid Black integration for `ezmsg`
- [ezmsg-gadget](https://github.com/griffinmilsap/ezmsg-gadget) -- USB-gadget with HID control integration for Raspberry Pi (Zero/W/2W, 4, CM4)
- [ezmsg-openbci](https://github.com/griffinmilsap/ezmsg-openbci) -- OpenBCI Cyton serial interface for `ezmsg`
- [ezmsg-ssvep](https://github.com/griffinmilsap/ezmsg-ssvep) -- Tools for running SSVEP experiments with `ezmsg`
- [ezmsg-vispy](https://github.com/pperanich/ezmsg-vispy) -- `ezmsg` visualization toolkit using PyQt6 and vispy.

## Publications

A collection of academic papers, journals, and other publications that have cited or utilized `ezmsg` in research and development.
These publications provide insights into the practical applications and impact of `ezmsg` in various fields.

- [A click-based electrocorticographic brain-computer interface enables long-term high-performance switch-scan spelling](https://doi.org/10.21203/rs.3.rs-3158792/v1)
- [Stable Decoding from a Speech BCI Enables Control for an Individual with ALS without Recalibration for 3 Months](https://doi.org/10.1002/advs.202304853)
