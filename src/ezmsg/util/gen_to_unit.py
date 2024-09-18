"""
Please note that the `gen_to_unit` function in the current Python file is provided as
    an experimental feature and is not guaranteed to be actively maintained. It serves
    as an illustrative example of how generators can be utilized in the `ezmsg` library.
    However, please be aware that any changes made to the `ezmsg` core may potentially
    impact the functionality of `gen_to_unit`.
"""

import ezmsg.core as ez
import asyncio
import sys
import inspect
from dataclasses import asdict, fields
from typing import (
    Callable,
    get_args,
    Any,
    Generator,
    Dict,
    Tuple,
    cast,
    Type,
    TypeVar,
)
from ezmsg.util.messages.axisarray import AxisArray

if sys.version_info < (3, 12):
    from typing_extensions import dataclass_transform
else:
    from typing import dataclass_transform


T = TypeVar("T")


@dataclass_transform()
def gen_to_unit(
    func: Callable,
    sleep_time: float = 0.0,
    output_stream_kwargs: dict = {},
    msg_to_state: dict = {},
) -> Tuple[Any, Any]:
    sig = inspect.signature(func)

    # Parse the generator type hint in the return annotation
    generator_type_hint = sig.return_annotation
    if not hasattr(generator_type_hint, "__origin__") or not issubclass(
        generator_type_hint.__origin__, Generator
    ):
        raise ValueError(
            f"Function return type expected to be Generator, found: {generator_type_hint}"
        )
    gen_args = get_args(generator_type_hint)
    if len(gen_args) < 3:
        raise ValueError(
            "Must specify Generator yield, send, and return types for function annotation!"
        )
    publish_type, subscribe_type, _ = gen_args

    # Create Settings class from function signature parameters
    _defaults = {}
    _type_hints = {}
    for param in sig.parameters.values():
        if param.default is not inspect.Parameter.empty:
            _defaults[param.name] = param.default
        _type_hints[param.name] = (
            param.annotation
        )  # OK if empty, needed for untyped params
    settings = args_to_settings(func.__name__, _type_hints, _defaults)

    # Create State
    state = args_to_state(func.__name__, _type_hints, publish_type, subscribe_type)

    async def initialize(self):
        self.STATE.gen = func(**asdict(self.SETTINGS))
        for field in fields(self.SETTINGS):
            setattr(self.STATE, field.name, getattr(self.SETTINGS, field.name))

    async def shutdown(self):
        self.STATE.gen.close()

    def check_fields(self, msg):
        # We want to track incoming message fields and reinitialize the gen if changed.
        for msg_field_name, state_field_name in msg_to_state.items():
            if not hasattr(msg, msg_field_name):
                raise Exception(
                    f"Attribute {msg_field_name} not found in message of type {type(msg)}"
                )
            if not hasattr(self.STATE, state_field_name):
                raise Exception(
                    f"Attribute {state_field_name} not found in state of type {type(self.STATE)}"
                )
            if getattr(self.STATE, state_field_name) != getattr(msg, msg_field_name):
                setattr(self.STATE, state_field_name, getattr(msg, msg_field_name))
                self.STATE.gen = func(**asdict(self.STATE))

    stream_suffix = {
        AxisArray: "_SIGNAL"
    }  # Add more message types if accessible in core.
    streams = {}
    ez_task: Callable
    if subscribe_type is type(None) and publish_type is type(None):

        @ez.task
        async def task(self) -> None:
            while True:
                next(self.STATE.gen)
                await asyncio.sleep(sleep_time)

        ez_task = task

    elif subscribe_type is type(None) and publish_type is not type(None):
        _output = ez.OutputStream(publish_type, **output_stream_kwargs)
        streams["OUTPUT" + stream_suffix.get(publish_type, "")] = _output

        @ez.publisher(_output)
        async def publish(self):
            while True:
                OUTPUT = self.streams["OUTPUT" + stream_suffix.get(publish_type, "")]
                yield OUTPUT, next(self.STATE.gen)
                await asyncio.sleep(sleep_time)

        ez_task = publish

    elif subscribe_type is not type(None) and publish_type is type(None):
        _input = ez.InputStream(subscribe_type)
        streams["INPUT" + stream_suffix.get(subscribe_type, "")] = _input

        @ez.subscriber(_input)
        async def subscribe(self, msg) -> None:
            self.check_fields(msg)
            self.STATE.gen.send(msg)

        ez_task = subscribe

    else:
        _input = ez.InputStream(subscribe_type)
        _output = ez.OutputStream(publish_type, **output_stream_kwargs)
        streams["INPUT" + stream_suffix.get(subscribe_type, "")] = _input
        streams["OUTPUT" + stream_suffix.get(publish_type, "")] = _output

        @ez.subscriber(_input)
        @ez.publisher(_output)
        async def subscribe_publish(self, msg):
            self.check_fields(msg)
            res = self.STATE.gen.send(msg)
            OUTPUT = self.streams["OUTPUT" + stream_suffix.get(publish_type, "")]
            yield OUTPUT, res

        ez_task = subscribe_publish

    if "INPUT" in streams and sleep_time != 0.0:
        ez.logger.warning(
            f"sleep time of {sleep_time} requested, but ignored since generator is subscriber."
        )

    unit = cast(
        Type[ez.Unit],
        type(
            f"{func.__name__.capitalize()}Unit",
            (ez.Unit,),
            {
                "__annotations__": {"SETTINGS": settings, "STATE": state},
                **streams,
                "initialize": initialize,
                "shutdown": shutdown,
                "ez_task": ez_task,
                "check_fields": check_fields,
            },
        ),
    )

    return settings, unit


def args_to_settings(
    func_name: str, args: Dict[str, Any], defaults: dict
) -> Type[ez.Settings]:
    return cast(
        Type[ez.Settings],
        type(
            f"{func_name.capitalize()}Settings",
            (ez.Settings,),
            {"__annotations__": args, **defaults},
        ),
    )


def args_to_state(
    func_name: str, args: Dict[str, Any], yield_type: Any, send_type: Any
) -> Type[ez.State]:
    args["gen"] = Generator[yield_type, send_type, None]
    return cast(
        Type[ez.State],
        type(
            f"{func_name.capitalize()}State",
            (ez.State,),
            {"__annotations__": args},
        ),
    )
