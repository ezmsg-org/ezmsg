# An introduction to ezmsg

# If this is your first time using ezmsg, you're in the right place.
# This script will walk through the basics of creating a very simple
# ezmsg system. ezmsg is ideal for creating modular processing
# pipelines whose steps can be arranged as a directed acyclic graph.
# In this script, we will walk through a very simple graph which
# generates a count of numbers, adds 1 to each number, and prints to
# standard output.

# We will write an ezmsg Unit for each discrete step of our pipeline,
# and connect them together with a System.

import ezmsg.core as ez
from dataclasses import dataclass
from typing import AsyncGenerator, Tuple

# Create a message type to pass between the Units.
# Python dataclasses are great for arbitrary messages, but you can
# use any type

# If you've not worked with dataclasses before, here's a good primer.
# https://docs.python.org/3/library/dataclasses.html
# https://realpython.com/python-data-classes/


@dataclass
class CountMessage:
    value: int


# Also if you've not run into typehinting before, you'll see a lot of
# it in this code.  Dataclasses use typehinting for fields.  This
# type information can be very helpful when working in editors like
# vscode or pycharm, especially when linting is enabled.  These type
# hints can give you autocomplete info and let you know when the
# type assumptions are broken (and you code likely is too).  You can
# read more about typehinting here:
# https://docs.python.org/3/library/typing.html
# https://realpython.com/python-type-checking/

# We also need a way to tell the Unit how many numbers to generate.
# All classes that derive from ez.Settings are frozen dataclasses!


class CountSettings(ez.Settings):
    iterations: int


# Next, create a Unit that will generate the count. Every Unit should
# contain inputs and/or outputs and at least one function which
# subscribes to the inputs or publishes to the outputs.

# For Count, we create an OutputStream and a publishing function which
# will perform the number calculation and yield CountMessages to the
# OutputStream.


class Count(ez.Unit):
    # Only provide a settings type, do not instantiate
    # We do this because this unit may receive settings objects
    # from parent collections.  Also, SETTINGS is a special/reserved
    # class attribute for Components (ez.Unit, and ez.Collection)
    SETTINGS: CountSettings

    OUTPUT_COUNT = ez.OutputStream(CountMessage)

    @ez.publisher(OUTPUT_COUNT)
    async def count(self) -> AsyncGenerator:
        count = 0
        while count < self.SETTINGS.iterations:
            yield (self.OUTPUT_COUNT, CountMessage(value=count))
            count = count + 1


# The next `Unit` in the chain should accept a `CountMessage` from the
# first `Unit`, add 1 to its value, and yield a new CountMessage. To
# do this, we create a new `Unit` which contains a function which both
# subscribes and publishes. We will connect this `Unit` to `Count`
# later on, when we create a `System`.

# The subscribing function will be called anytime the `Unit` receives
# a message to the `InputStream` that the function subscribes to. In
# this case, `INPUT_COUNT`.


class AddOne(ez.Unit):
    INPUT_COUNT = ez.InputStream(CountMessage)
    OUTPUT_PLUS_ONE = ez.OutputStream(CountMessage)

    @ez.subscriber(INPUT_COUNT)
    @ez.publisher(OUTPUT_PLUS_ONE)
    async def on_message(self, message: CountMessage) -> AsyncGenerator:
        yield (self.OUTPUT_PLUS_ONE, CountMessage(value=message.value + 1))


# Finally, the last unit should print the value of any messages it
# receives.

# Since this unit is the last in the pipeline, it should also
# terminate the system when it receives the last message. We can use
# `ez.NormalTermination` to let the system know that it should
# gracefully shut down.

# We can use both Settings and State together to count messages and
# raise `ez.NormalTermination` when all have passed through.


class PrintSettings(ez.Settings):
    iterations: int


class PrintState(ez.State):
    current_iteration: int = 0


class PrintValue(ez.Unit):
    SETTINGS: PrintSettings

    # As with settings, only provide a state type, do not instantiate.
    # We do this because this unit may end up living in a different
    # process, and the state may contain attributes that cannot be
    # pickled/sent to subprocesses (e.g. open file-handles, asyncio
    # and threading primitives, etc.) STATE is a special/reserved
    # class attribute for Units
    STATE: PrintState

    INPUT = ez.InputStream(CountMessage)

    @ez.subscriber(INPUT)
    async def on_message(self, message: CountMessage) -> None:
        print(f"Current Count: {message.value}")

        self.STATE.current_iteration = self.STATE.current_iteration + 1
        if self.STATE.current_iteration == self.SETTINGS.iterations:
            raise ez.NormalTermination


# The last thing to do before we have a fully functioning ezmsg
# pipeline is to define any Settings that have been declared and to
# connect all of the units together. We do this using a Collection. The
# configure() and network() functions are special functions that
# propogate settings downward and specify message flow.


class CountSystemSettings(ez.Settings):
    iterations: int


class CountSystem(ez.Collection):
    SETTINGS: CountSystemSettings

    # Define member units
    COUNT = Count()
    ADD_ONE = AddOne()
    PRINT = PrintValue()

    # Use the configure function to apply settings to sub-Units
    def configure(self) -> None:
        self.COUNT.apply_settings(CountSettings(iterations=self.SETTINGS.iterations))

        self.PRINT.apply_settings(PrintSettings(iterations=self.SETTINGS.iterations))

    # Use the network function to connect inputs and outputs of Units
    def network(self) -> ez.NetworkDefinition:
        return (
            (self.COUNT.OUTPUT_COUNT, self.ADD_ONE.INPUT_COUNT),
            (self.ADD_ONE.OUTPUT_PLUS_ONE, self.PRINT.INPUT),
        )

    # Use process_components to define units that need to live in
    # their own process.  Any sub-units specified in the return
    # value from this function will be run in their own process
    # with true parallelism (goodbye Python GIL!)
    # Any remaining sub units not specified are all run in the
    # main process.
    def process_components(self) -> Tuple[ez.Component, ...]:
        return (self.COUNT, self.ADD_ONE, self.PRINT)


if __name__ == "__main__":
    # Finally, instantiate and run the system!
    # If you intend to launch subprocesses, this needs to be done within
    # the '__main__' include guard as shown here.
    settings = CountSystemSettings(iterations=20)
    system = CountSystem(settings)
    ez.run(system)
