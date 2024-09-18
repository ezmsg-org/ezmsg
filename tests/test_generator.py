import copy
import json
import os
import typing

import numpy as np
import pytest

import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.generator import consumer
from ezmsg.util.gen_to_unit import gen_to_unit

from ez_test_utils import (
    get_test_fn,
    MessageGenerator,
    MessageGeneratorSettings,
    MessageReceiverSettings,
    MessageReceiverState,
)


def undecorated_generator(
    arg_untyped, arg_typed: int, kwarg_untyped=None, kwarg_typed: bool = True
) -> typing.Generator[typing.Any, typing.Any, None]:
    """
    Do-nothing generator to test func inspection.
    """
    msg_in = None
    msg_out = None

    while True:
        msg_in = yield msg_out
        msg_out = msg_in


@consumer
def decorated_generator(
    arg_untyped, arg_typed: int, kwarg_untyped=None, kwarg_typed: bool = True
) -> typing.Generator[typing.Any, typing.Any, None]:
    """
    Do-nothing generator to test func inspection.
    """
    msg_in = None
    msg_out = None

    while True:
        msg_in = yield msg_out
        msg_out = msg_in


def test_gen_to_unit_settings():
    for _gen in (decorated_generator, undecorated_generator):
        MySettings, MyUnit = gen_to_unit(_gen)
        assert isinstance(MySettings, ez.settings.SettingsMeta)  # Why not ez.Settings?
        assert isinstance(MyUnit, ez.unit.UnitMeta)  # Why not ez.Unit?
        with pytest.raises(TypeError):
            # Fails on missing positional arguments
            MySettings()

        settings = MySettings(None, 0)
        assert settings.arg_untyped is None
        assert settings.arg_typed == 0
        assert settings.kwarg_untyped is None
        assert settings.kwarg_typed is True

        settings = MySettings("thing", -5, "thing2", False)
        assert settings.arg_untyped == "thing"
        assert settings.arg_typed == -5
        assert settings.kwarg_untyped == "thing2"
        assert settings.kwarg_typed is False


@consumer
def my_gen_func(
    append_right: bool = True,
) -> typing.Generator[typing.List[typing.Any], typing.Any, None]:
    """
    Basic generator function used for testing. It returns the accumulated inputs.
    """

    # state variables
    msg_in = None
    msg_out = []
    history: typing.List[typing.Any] = []

    while True:
        msg_in = yield msg_out

        if append_right:
            history.append(msg_in)
        else:
            history = [msg_in] + history
        msg_out = copy.copy(history)


@consumer
def my_gen_func_axarr(
    axis: str = "time",
) -> typing.Generator[AxisArray, AxisArray, None]:
    """
    Basic generator function used for testing. It returns the accumulated inputs
    when the inputs are AxisArrays and accumulation happens on the specified axis.
    """
    axis_arr_in = AxisArray(np.array([]), dims=[""])
    axis_arr_out = AxisArray(np.array([]), dims=[""])

    # state variables
    history: typing.Optional[AxisArray] = None

    while True:
        axis_arr_in = yield axis_arr_out

        if history is None:
            history = axis_arr_in
        else:
            history = AxisArray.concatenate(history, axis_arr_in, dim=axis)
        axis_arr_out = history  # copy?


def test_gen_funcs():
    # This does not test ezmsg. This simply verifies that our test generators work as expected.
    my_gen = my_gen_func()
    assert my_gen.send("a") == ["a"]
    assert my_gen.send("b") == ["a", "b"]

    my_gen = my_gen_func_axarr()
    in1 = AxisArray(np.array([0, 1]), dims=["time"])
    in2 = AxisArray(np.array([3, 4]), dims=["time"])

    out1 = my_gen.send(in1)
    assert out1 is in1
    out2 = my_gen.send(in2)
    assert np.array_equal(out2.data, np.hstack((in1.data, in2.data)))


class MessageAnyReceiver(ez.Unit):
    STATE = MessageReceiverState
    SETTINGS = MessageReceiverSettings

    INPUT = ez.InputStream(typing.List[typing.Any])

    @ez.subscriber(INPUT)
    async def on_message(self, msg: typing.List[typing.Any]) -> None:
        self.STATE.num_received += 1
        with open(self.SETTINGS.output_fn, "a") as output_file:
            payload = {self.STATE.num_received: [_.number for _ in msg]}
            output_file.write(json.dumps(payload) + "\n")
        if self.STATE.num_received == self.SETTINGS.num_msgs:
            raise ez.NormalTermination


def test_gen_to_unit_any():
    MySettings, MyUnit = gen_to_unit(my_gen_func)

    # Check MyUnit streams
    assert hasattr(MyUnit, "INPUT")
    assert isinstance(MyUnit.INPUT, ez.stream.InputStream)
    assert MyUnit.INPUT.msg_type is typing.Any
    assert hasattr(MyUnit, "OUTPUT")
    assert isinstance(MyUnit.OUTPUT, ez.stream.OutputStream)
    assert MyUnit.OUTPUT.msg_type is typing.List[typing.Any]

    num_msgs = 5
    test_filename = get_test_fn()
    comps = {
        "SIMPLE_PUB": MessageGenerator(num_msgs=num_msgs),
        "MYUNIT": MyUnit(),
        "SIMPLE_SUB": MessageAnyReceiver(num_msgs=num_msgs, output_fn=test_filename),
    }
    ez.run(
        components=comps,
        connections=(
            (comps["SIMPLE_PUB"].OUTPUT, comps["MYUNIT"].INPUT),
            (comps["MYUNIT"].OUTPUT, comps["SIMPLE_SUB"].INPUT),
        ),
    )
    results = []
    with open(test_filename, "r") as file:
        lines = file.readlines()
        for line in lines:
            results.append(json.loads(line))
    os.remove(test_filename)
    # We don't really care about the contents; functionality was confirmed in a separate test.
    # Keep this simple.
    assert len(results) == num_msgs


class AxarrGenerator(ez.Unit):
    SETTINGS = MessageGeneratorSettings
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    @ez.publisher(OUTPUT_SIGNAL)
    async def spawn(self) -> typing.AsyncGenerator:
        for i in range(self.SETTINGS.num_msgs):
            yield self.OUTPUT_SIGNAL, AxisArray(data=np.arange(i), dims=["time"])
        raise ez.Complete


class AxarrReceiver(ez.Unit):
    STATE = MessageReceiverState
    SETTINGS = MessageReceiverSettings
    INPUT_SIGNAL = ez.InputStream(AxisArray)

    @ez.subscriber(INPUT_SIGNAL)
    async def on_message(self, msg: AxisArray) -> None:
        self.STATE.num_received += 1
        with open(self.SETTINGS.output_fn, "a") as output_file:
            payload = {self.STATE.num_received: msg.data.tolist()}
            output_file.write(json.dumps(payload) + "\n")
        if self.STATE.num_received == self.SETTINGS.num_msgs:
            raise ez.NormalTermination


def test_gen_to_unit_axarr():
    MySettings, MyUnit = gen_to_unit(my_gen_func_axarr)

    assert hasattr(MyUnit, "INPUT_SIGNAL")
    assert isinstance(MyUnit.INPUT_SIGNAL, ez.stream.InputStream)
    assert MyUnit.INPUT_SIGNAL.msg_type is AxisArray
    assert hasattr(MyUnit, "OUTPUT_SIGNAL")
    assert isinstance(MyUnit.OUTPUT_SIGNAL, ez.stream.OutputStream)
    assert MyUnit.OUTPUT_SIGNAL.msg_type is AxisArray

    num_msgs = 5
    test_filename = get_test_fn()
    comps = {
        "SIMPLE_PUB": AxarrGenerator(num_msgs=num_msgs),
        "MYUNIT": MyUnit(),
        "SIMPLE_SUB": AxarrReceiver(num_msgs=num_msgs, output_fn=test_filename),
    }
    ez.run(
        components=comps,
        connections=(
            (comps["SIMPLE_PUB"].OUTPUT_SIGNAL, comps["MYUNIT"].INPUT_SIGNAL),
            (comps["MYUNIT"].OUTPUT_SIGNAL, comps["SIMPLE_SUB"].INPUT_SIGNAL),
        ),
    )
    results = []
    with open(test_filename, "r") as file:
        lines = file.readlines()
        for line in lines:
            results.append(json.loads(line))
    os.remove(test_filename)
    assert np.array_equal(
        results[-1][f"{num_msgs}"], np.hstack([np.arange(_) for _ in range(num_msgs)])
    )
    assert len(results) == num_msgs


# TODO: test compose, GenState, Gen
