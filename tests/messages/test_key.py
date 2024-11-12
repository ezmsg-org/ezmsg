import copy
import json
import os
import typing

import numpy as np

import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.messages.key import set_key, SetKey, FilterOnKey
from ..ez_test_utils import (
    get_test_fn,
    MessageGeneratorSettings,
    MessageReceiverSettings,
    MessageReceiverState,
)


def test_set_key():
    input_ax_arr = AxisArray(
        data=np.arange(60).reshape(3, 5, 4),
        dims=["step", "freq", "ch"],
        axes={"step": AxisArray.TimeAxis(fs=10.0, offset=0.0)},
        key="old key",
    )
    backup = copy.deepcopy(input_ax_arr)

    gen = set_key(key="new key")
    res = gen.send(input_ax_arr)

    # Make sure the input hasn't changed
    assert np.array_equal(input_ax_arr.data, backup.data)
    assert input_ax_arr.dims == backup.dims
    assert list(input_ax_arr.axes.keys()) == list(backup.axes.keys())
    for k, v in input_ax_arr.axes.items():
        assert v == backup.axes[k]

    # We don't want the data to be copied
    assert res.data is input_ax_arr.data

    assert res.key == "new key"


class KeyedAxarrGenerator(ez.Unit):
    SETTINGS = MessageGeneratorSettings
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    @ez.publisher(OUTPUT_SIGNAL)
    async def spawn(self) -> typing.AsyncGenerator:
        for i in range(self.SETTINGS.num_msgs):
            yield (
                self.OUTPUT_SIGNAL,
                AxisArray(
                    data=np.arange(i), dims=["time"], key="odd" if i % 2 else "even"
                ),
            )
        raise ez.Complete


class AxarrReceiver(ez.Unit):
    STATE = MessageReceiverState
    SETTINGS = MessageReceiverSettings
    INPUT_SIGNAL = ez.InputStream(AxisArray)

    @ez.subscriber(INPUT_SIGNAL)
    async def on_message(self, msg: AxisArray) -> None:
        self.STATE.num_received += 1
        with open(self.SETTINGS.output_fn, "a") as output_file:
            payload = {self.STATE.num_received: msg.key}
            output_file.write(json.dumps(payload) + "\n")
        if self.STATE.num_received == self.SETTINGS.num_msgs:
            raise ez.NormalTermination


def test_set_key_unit():
    num_msgs = 20
    test_fn_raw = get_test_fn()
    test_fn_keyed = test_fn_raw.parent / (
        test_fn_raw.stem + "_keyed" + test_fn_raw.suffix
    )

    comps = {
        "SRC": KeyedAxarrGenerator(num_msgs=num_msgs),
        "KEYSETTER": SetKey(key="new key"),
        "SINK_RAW": AxarrReceiver(num_msgs=1e9, output_fn=test_fn_raw),
        "SINK_KEYED": AxarrReceiver(num_msgs=num_msgs, output_fn=test_fn_keyed),
    }
    conns = [
        (comps["SRC"].OUTPUT_SIGNAL, comps["SINK_RAW"].INPUT_SIGNAL),
        (comps["SRC"].OUTPUT_SIGNAL, comps["KEYSETTER"].INPUT_SIGNAL),
        (comps["KEYSETTER"].OUTPUT_SIGNAL, comps["SINK_KEYED"].INPUT_SIGNAL),
    ]
    ez.run(components=comps, connections=conns)

    with open(test_fn_raw, "r") as file:
        raw_results = [json.loads(_) for _ in file.readlines()]
    os.remove(test_fn_raw)
    assert len(raw_results) == num_msgs
    assert all(
        [
            _[str(ix + 1)] == ("odd" if ix % 2 else "even")
            for ix, _ in enumerate(raw_results)
        ]
    )

    with open(test_fn_keyed, "r") as file:
        keyed_results = [json.loads(_) for _ in file.readlines()]
    os.remove(test_fn_keyed)
    assert len(keyed_results) == num_msgs
    assert all([_[str(ix + 1)] == "new key" for ix, _ in enumerate(keyed_results)])


def test_filter_key():
    num_msgs = 20
    test_fn_raw = get_test_fn()
    test_fn_filtered = test_fn_raw.parent / (
        test_fn_raw.stem + "_keyed" + test_fn_raw.suffix
    )

    comps = {
        "SRC": KeyedAxarrGenerator(num_msgs=num_msgs),
        "FILTER": FilterOnKey(key="odd"),
        "SINK_RAW": AxarrReceiver(num_msgs=1e9, output_fn=test_fn_raw),
        "SINK_FILTERED": AxarrReceiver(
            num_msgs=num_msgs // 2, output_fn=test_fn_filtered
        ),
    }
    conns = [
        (comps["SRC"].OUTPUT_SIGNAL, comps["SINK_RAW"].INPUT_SIGNAL),
        (comps["SRC"].OUTPUT_SIGNAL, comps["FILTER"].INPUT_SIGNAL),
        (comps["FILTER"].OUTPUT_SIGNAL, comps["SINK_FILTERED"].INPUT_SIGNAL),
    ]
    ez.run(components=comps, connections=conns)

    with open(test_fn_raw, "r") as file:
        raw_results = [json.loads(_) for _ in file.readlines()]
    os.remove(test_fn_raw)
    assert len(raw_results) == num_msgs

    with open(test_fn_filtered, "r") as file:
        filtered_results = [json.loads(_) for _ in file.readlines()]
    os.remove(test_fn_filtered)
    assert len(filtered_results) == num_msgs // 2
    assert all([_[str(ix + 1)] == "odd" for ix, _ in enumerate(filtered_results)])
