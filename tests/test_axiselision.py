"""Wire elision of coordinate axes a receiver already holds."""

import asyncio
import pickle

import numpy as np
import pytest

import ezmsg.core as ez
from ezmsg.core.axiselision import AxisElision, AxisTable, MissingAxis, _AxisDef, _AxisRef, wire_token
from ezmsg.core.messagemarshal import MessageMarshal
from ezmsg.util.messages.axisarray import AxisArray, CoordinateAxis
from ezmsg.util.messages.util import replace

STRUCT = np.dtype([("label", "U8"), ("x", "f8"), ("y", "f8"), ("z", "f8")])


def ch_axis(n=16, prefix="e"):
    a = np.zeros(n, STRUCT)
    a["label"] = [f"{prefix}{i:03d}" for i in range(n)]
    a["x"] = np.arange(n)
    return CoordinateAxis(data=a, dims=["ch"])


def msg(ch, i=0, stream_dim="time", time_axis=None):
    return AxisArray(
        np.full((4, len(ch.data)), float(i), np.float32),
        dims=["time", "ch"],
        axes={"time": time_axis if time_axis is not None else AxisArray.TimeAxis(fs=100.0, offset=i), "ch": ch},
        key="k",
        **({"stream_dim": stream_dim} if stream_dim else {}),
    )


def roundtrip(obj, elision, table):
    """Serialize as a publisher would and load as a channel would."""
    with MessageMarshal.serialize(0, obj, elision) as (total, header, buffers):
        raw = bytearray(total)
        MessageMarshal._write(memoryview(raw), header, buffers)
    with MessageMarshal.obj_from_mem(memoryview(raw), table) as out:
        return out


class TestWire:
    def test_first_message_defines_then_references(self):
        el, ch = AxisElision(), ch_axis()
        first = el.wire(msg(ch, 0))
        second = el.wire(msg(ch, 1))
        assert type(first.axes["ch"]) is _AxisDef
        assert type(second.axes["ch"]) is _AxisRef
        assert first.axes["ch"].token == second.axes["ch"].token == wire_token(ch)

    def test_stream_axis_is_never_elided(self):
        el = AxisElision()
        events = CoordinateAxis(data=np.arange(4.0), dims=["time"], unit="s")
        wired = el.wire(msg(ch_axis(), time_axis=events))
        assert wired.axes["time"] is events

    def test_undeclared_stream_dim_falls_back_to_time(self):
        el = AxisElision()
        events = CoordinateAxis(data=np.arange(4.0), dims=["time"], unit="s")
        wired = el.wire(msg(ch_axis(), stream_dim=None, time_axis=events))
        assert wired.axes["time"] is events and type(wired.axes["ch"]) is _AxisDef

    def test_the_callers_message_is_untouched(self):
        el, ch = AxisElision(), ch_axis()
        m = msg(ch)
        el.wire(m)
        el.wire(m)
        assert m.axes["ch"] is ch

    def test_other_objects_pass_through(self):
        el = AxisElision()
        for obj in (b"bytes", {"a": 1}, np.zeros(3)):
            assert el.wire(obj) is obj

    def test_reset_defines_again(self):
        el, ch = AxisElision(), ch_axis()
        el.wire(msg(ch))
        el.reset()
        assert type(el.wire(msg(ch)).axes["ch"]) is _AxisDef

    def test_equal_axes_share_a_token(self):
        assert wire_token(ch_axis()) == wire_token(ch_axis())
        assert wire_token(ch_axis()) != wire_token(ch_axis(prefix="z"))

    def test_replace_drops_the_cached_token(self):
        ch = ch_axis()
        wire_token(ch)
        assert "_wire_token" not in replace(ch, data=ch_axis(prefix="z").data).__dict__


class TestResolve:
    def test_messages_share_one_owned_axis(self):
        el, table, ch = AxisElision(), AxisTable(), ch_axis()
        outs = [roundtrip(msg(ch, i), el, table) for i in range(3)]
        held = outs[0].axes["ch"]
        assert all(o.axes["ch"] is held for o in outs)
        assert held.data.flags.owndata
        assert np.array_equal(held.data, ch.data)
        assert [float(o.data[0, 0]) for o in outs] == [0.0, 1.0, 2.0]

    def test_a_relabel_reaches_the_receiver(self):
        el, table = AxisElision(), AxisTable()
        roundtrip(msg(ch_axis()), el, table)
        out = roundtrip(msg(ch_axis(prefix="z")), el, table)
        assert out.axes["ch"].data["label"][0] == "z000"

    def test_unknown_reference_raises(self):
        el, ch = AxisElision(), ch_axis()
        roundtrip(msg(ch), el, AxisTable())  # defined to a *different* table
        with pytest.raises(MissingAxis):
            roundtrip(msg(ch), el, AxisTable())

    def test_without_elision_nothing_changes(self):
        ch = ch_axis()
        out = roundtrip(msg(ch), None, AxisTable())
        assert np.array_equal(out.axes["ch"].data, ch.data)

    def test_wired_messages_survive_a_publisher_side_copy(self):
        """SHM grow re-serializes slots with copy_obj (no table): references
        must survive that round trip unresolved."""
        el, table, ch = AxisElision(), AxisTable(), ch_axis()
        roundtrip(msg(ch, 0), el, table)
        with MessageMarshal.serialize(1, msg(ch, 1), el) as (total, header, buffers):
            src = bytearray(total + 64)
            MessageMarshal._write(memoryview(src), header, buffers)
        dst = bytearray(total + 64)
        MessageMarshal.copy_obj(memoryview(src), memoryview(dst))
        with MessageMarshal.obj_from_mem(memoryview(dst), table) as out:
            assert out.axes["ch"] is roundtrip(msg(ch, 2), el, table).axes["ch"]


async def _pubsub(ctx, topic):
    pub = await ctx.publisher(topic, host="127.0.0.1", num_buffers=4, allow_local=False)
    sub = await ctx.subscriber(topic)
    for _ in range(100):  # the channel's ELIDE_OK arrives asynchronously
        if pub._elide:
            break
        await asyncio.sleep(0.01)
    return pub, sub


async def _recv(sub):
    async with sub.recv_zero_copy() as m:
        return m.axes["ch"], float(m.data[0, 0]), m.axes["ch"].data["label"][0]


@pytest.mark.asyncio
async def test_end_to_end_over_shm():
    async with ez.GraphContext(auto_start=True) as ctx:
        pub, sub = await _pubsub(ctx, "/ELIDE/E2E")
        assert pub._elide
        ch = ch_axis(64)
        got = []
        for i in range(5):
            await pub.broadcast(msg(ch, i))
            got.append(await _recv(sub))
        assert [g[1] for g in got] == [0.0, 1.0, 2.0, 3.0, 4.0]
        assert all(g[0] is got[0][0] for g in got)  # one shared axis on the far side
        await pub.broadcast(msg(ch_axis(64, prefix="z"), 5))
        assert (await _recv(sub))[2] == "z000"


@pytest.mark.asyncio
async def test_a_receiver_that_lost_its_axes_recovers():
    async with ez.GraphContext(auto_start=True) as ctx:
        pub, sub = await _pubsub(ctx, "/ELIDE/RECOVER")
        ch = ch_axis(32)
        await pub.broadcast(msg(ch, 0))
        await _recv(sub)
        channel = sub._channels[pub.id]
        channel._axis_table = AxisTable()  # as if its definition had been missed
        await pub.broadcast(msg(ch, 1))  # a reference it cannot resolve: dropped
        for _ in range(100):
            if not pub._elision.announced:
                break
            await asyncio.sleep(0.01)
        assert not pub._elision.announced  # the channel asked for axes again
        await pub.broadcast(msg(ch, 2))
        assert (await _recv(sub))[1] == 2.0


@pytest.mark.asyncio
async def test_disabled_while_a_channel_cannot_resolve(monkeypatch):
    """A channel that never says ELIDE_OK (an older ezmsg) keeps elision off."""
    from ezmsg.core import messagechannel

    monkeypatch.setattr(messagechannel, "ELISION_ENABLED", False)
    async with ez.GraphContext(auto_start=True) as ctx:
        pub, sub = await _pubsub(ctx, "/ELIDE/OLD")
        assert not pub._elide
        ch = ch_axis(8)
        await pub.broadcast(msg(ch, 0))
        assert (await _recv(sub))[1] == 0.0
