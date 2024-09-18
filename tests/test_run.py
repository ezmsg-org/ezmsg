import json
import os
import pytest

import ezmsg.core as ez

from ez_test_utils import (
    get_test_fn,
    MessageGenerator,
    MessageGeneratorSettings,
    MessageReceiver,
    MessageReceiverSettings,
)


# Define and configure a system of modules to launch
class ToySystemSettings(ez.Settings):
    num_msgs: int
    output_fn: str


class ToySystem(ez.Collection):
    SETTINGS = ToySystemSettings

    # Publishers
    SIMPLE_PUB = MessageGenerator()

    # Subscribers
    SIMPLE_SUB = MessageReceiver()

    def configure(self) -> None:
        self.SIMPLE_PUB.apply_settings(
            MessageGeneratorSettings(num_msgs=self.SETTINGS.num_msgs)
        )

        self.SIMPLE_SUB.apply_settings(
            MessageReceiverSettings(
                num_msgs=self.SETTINGS.num_msgs, output_fn=self.SETTINGS.output_fn
            )
        )

    # Define Connections
    def network(self) -> ez.NetworkDefinition:
        return ((self.SIMPLE_PUB.OUTPUT, self.SIMPLE_SUB.INPUT),)


@pytest.fixture(
    params=[
        (ToySystem,),
        (
            ToySystem.SIMPLE_PUB,
            ToySystem.SIMPLE_SUB,
        ),
    ]
)
def toy_system_fixture(request):
    def func(self):
        return request.param

    ToySystem.process_components = func
    return ToySystem


@pytest.mark.parametrize("num_messages", [1, 5, 10])
def test_local_system(toy_system_fixture, num_messages):
    test_filename = get_test_fn()
    system = toy_system_fixture(
        ToySystemSettings(num_msgs=num_messages, output_fn=test_filename)
    )
    ez.run(SYSTEM=system)

    results = []
    with open(test_filename, "r") as file:
        lines = file.readlines()
        for line in lines:
            results.append(json.loads(line))
    os.remove(test_filename)
    assert len(results) == num_messages


@pytest.mark.parametrize("passthrough_settings", [False, True])
@pytest.mark.parametrize("num_messages", [1, 5, 10])
def test_run_comps_conns(passthrough_settings, num_messages):
    test_filename = get_test_fn()
    if passthrough_settings:
        comps = {
            "SIMPLE_PUB": MessageGenerator(num_msgs=num_messages),
            "SIMPLE_SUB": MessageReceiver(
                num_msgs=num_messages, output_fn=test_filename
            ),
        }
    else:
        comps = {
            "SIMPLE_PUB": MessageGenerator(
                MessageGeneratorSettings(num_msgs=num_messages)
            ),
            "SIMPLE_SUB": MessageReceiver(
                MessageReceiverSettings(num_msgs=num_messages, output_fn=test_filename)
            ),
        }
    conns = ((comps["SIMPLE_PUB"].OUTPUT, comps["SIMPLE_SUB"].INPUT),)

    ez.run(components=comps, connections=conns)

    results = []
    with open(test_filename, "r") as file:
        lines = file.readlines()
        for line in lines:
            results.append(json.loads(line))
    os.remove(test_filename)
    assert len(results) == num_messages


@pytest.mark.parametrize("passthrough_settings", [False, True])
@pytest.mark.parametrize("num_messages", [1, 5, 10])
def test_run_collection(passthrough_settings, num_messages):
    test_filename = get_test_fn()
    if passthrough_settings:
        collection = ToySystem(num_msgs=num_messages, output_fn=test_filename)
    else:
        collection = ToySystem(
            ToySystemSettings(num_msgs=num_messages, output_fn=test_filename)
        )
    ez.run(collection)
    results = []
    with open(test_filename, "r") as file:
        lines = file.readlines()
        for line in lines:
            results.append(json.loads(line))
    os.remove(test_filename)
    assert len(results) == num_messages
