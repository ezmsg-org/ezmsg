#!/usr/bin/env python3
"""
Simple settings TUI for ezmsg GraphServer.

Features:
- Live settings view (push updates via GraphContext.subscribe_settings_events)
- Inspect component metadata and current settings snapshot
- Publish patched settings to components with dynamic INPUT_SETTINGS

Usage:
    .venv/bin/python examples/settings_tui.py --host 127.0.0.1 --port 25978

Commands:
    help
    refresh
    inspect <row|component_address>
    set <row|component_address> {"field": 123, "nested": {"gain": 0.5}}
    quit

Notes:
- Updates are sent over normal pub/sub to the component's INPUT_SETTINGS topic.
- For safe updates, the script expects pickled current settings to be available
  and unpickleable in this environment.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import pickle
from dataclasses import dataclass, is_dataclass, replace
from typing import Any

from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.graphmeta import (
    ComponentMetadataType,
    GraphMetadata,
    SettingsChangedEvent,
    SettingsSnapshotValue,
)
from ezmsg.core.netprotocol import DEFAULT_HOST, GRAPHSERVER_PORT_DEFAULT
from ezmsg.core.pubclient import Publisher


def _truncate(text: str, width: int) -> str:
    if width <= 3:
        return text[:width]
    if len(text) <= width:
        return text
    return text[: width - 3] + "..."


def _format_settings(value: SettingsSnapshotValue | None, width: int = 72) -> str:
    if value is None:
        return "-"
    return _truncate(repr(value.repr_value), width)


def _deep_merge_dict(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, patch_value in patch.items():
        base_value = merged.get(key)
        if isinstance(base_value, dict) and isinstance(patch_value, dict):
            merged[key] = _deep_merge_dict(base_value, patch_value)
        else:
            merged[key] = patch_value
    return merged


def _patch_dataclass(obj: Any, patch: dict[str, Any]) -> Any:
    updates: dict[str, Any] = {}
    for key, patch_value in patch.items():
        if not hasattr(obj, key):
            raise KeyError(f"Settings object has no field '{key}'")
        current = getattr(obj, key)
        if is_dataclass(current) and isinstance(patch_value, dict):
            updates[key] = _patch_dataclass(current, patch_value)
        elif isinstance(current, dict) and isinstance(patch_value, dict):
            updates[key] = _deep_merge_dict(current, patch_value)
        else:
            updates[key] = patch_value
    return replace(obj, **updates)


def _patch_value(value: Any, patch: dict[str, Any]) -> Any:
    if is_dataclass(value):
        return _patch_dataclass(value, patch)
    if isinstance(value, dict):
        return _deep_merge_dict(value, patch)
    raise TypeError(
        f"Cannot patch settings value of type {type(value).__name__}. "
        "Only dataclass/dict settings are supported by this script."
    )


def _components_from_metadata(
    metadata: GraphMetadata | None,
) -> dict[str, ComponentMetadataType]:
    if metadata is None:
        return {}
    return dict(metadata.components)


@dataclass
class ComponentRow:
    address: str
    name: str
    component_type: str
    settings_type: str
    dynamic_enabled: bool
    input_topic: str | None


class SettingsTUI:
    def __init__(self, ctx: GraphContext):
        self.ctx = ctx
        self.settings: dict[str, SettingsSnapshotValue] = {}
        self.components: dict[str, ComponentRow] = {}
        self.row_addresses: list[str] = []
        self.last_seq = 0
        self.publishers: dict[str, Publisher] = {}
        self._event_queue: asyncio.Queue[SettingsChangedEvent] = asyncio.Queue()
        self._watch_task: asyncio.Task[None] | None = None

    async def initialize(self) -> None:
        await self.refresh()
        events = await self.ctx.settings_events(after_seq=0)
        for event in events:
            self.settings[event.component_address] = event.value
            self.last_seq = max(self.last_seq, event.seq)
        self._watch_task = asyncio.create_task(self._watch_settings_events())

    async def close(self) -> None:
        if self._watch_task is not None:
            self._watch_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._watch_task

    async def _watch_settings_events(self) -> None:
        async for event in self.ctx.subscribe_settings_events(after_seq=self.last_seq):
            await self._event_queue.put(event)

    async def refresh(self) -> None:
        snapshot = await self.ctx.snapshot()
        settings = await self.ctx.settings_snapshot()

        components: dict[str, ComponentRow] = {}
        for session in snapshot.sessions.values():
            for address, comp in _components_from_metadata(session.metadata).items():
                components[address] = ComponentRow(
                    address=address,
                    name=comp.name,
                    component_type=comp.component_type,
                    settings_type=comp.settings_type,
                    dynamic_enabled=comp.dynamic_settings.enabled,
                    input_topic=comp.dynamic_settings.input_topic,
                )

        self.components = components
        self.settings = settings

    async def drain_events(self) -> int:
        count = 0
        while True:
            try:
                event = self._event_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self.settings[event.component_address] = event.value
            self.last_seq = max(self.last_seq, event.seq)
            count += 1
        return count

    def render(self, pending_updates: int = 0) -> None:
        print("\x1bc", end="")
        print("ezmsg settings tui")
        print(
            "Commands: help, refresh, inspect <row|address>, "
            "set <row|address> <json-patch>, quit"
        )
        if pending_updates > 0:
            print(f"Applied {pending_updates} new settings event(s).")

        all_addresses = sorted(set(self.settings) | set(self.components))
        self.row_addresses = all_addresses

        header = (
            f"{'Row':<4} {'Component':<36} {'Dyn':<4} "
            f"{'INPUT_SETTINGS Topic':<42} {'Current Settings':<72}"
        )
        print()
        print(header)
        print("-" * len(header))

        for idx, address in enumerate(all_addresses, start=1):
            comp = self.components.get(address)
            settings = self.settings.get(address)

            dynamic = "yes" if comp is not None and comp.dynamic_enabled else "no"
            input_topic = (
                comp.input_topic if comp is not None and comp.input_topic is not None else "-"
            )
            print(
                f"{idx:<4} "
                f"{_truncate(address, 36):<36} "
                f"{dynamic:<4} "
                f"{_truncate(input_topic, 42):<42} "
                f"{_format_settings(settings):<72}"
            )

    def resolve_target(self, token: str) -> str:
        if token.isdigit():
            idx = int(token) - 1
            if idx < 0 or idx >= len(self.row_addresses):
                raise ValueError(f"Row index out of range: {token}")
            return self.row_addresses[idx]
        return token

    async def inspect(self, token: str) -> None:
        address = self.resolve_target(token)
        comp = self.components.get(address)
        settings = self.settings.get(address)
        print("\n--- inspect ---")
        print(f"address: {address}")
        if comp is None:
            print("metadata: <none>")
        else:
            print(f"name: {comp.name}")
            print(f"component_type: {comp.component_type}")
            print(f"settings_type: {comp.settings_type}")
            print(f"dynamic_settings.enabled: {comp.dynamic_enabled}")
            print(f"dynamic_settings.input_topic: {comp.input_topic}")
        if settings is None:
            print("current_settings: <none>")
        else:
            print(f"repr: {settings.repr_value!r}")
            print(f"has_pickled_payload: {settings.serialized is not None}")
            if settings.serialized is not None:
                try:
                    obj = pickle.loads(settings.serialized)
                    print(f"unpickled_type: {type(obj).__module__}.{type(obj).__name__}")
                except Exception as exc:
                    print(f"unpickled_type: <failed: {exc}>")

    async def set_settings(self, token: str, patch: dict[str, Any]) -> str:
        address = self.resolve_target(token)
        comp = self.components.get(address)
        if comp is None:
            raise ValueError(f"No component metadata available for '{address}'")
        if not comp.dynamic_enabled or comp.input_topic is None:
            raise ValueError(
                f"Component '{address}' is not dynamic-settings enabled or has no INPUT_SETTINGS topic"
            )

        current = self.settings.get(address)
        if current is None:
            raise ValueError(f"No current settings snapshot for '{address}'")
        if current.serialized is None:
            raise ValueError(
                f"No serialized settings for '{address}'. Cannot safely build updated object."
            )

        try:
            current_obj = pickle.loads(current.serialized)
        except Exception as exc:
            raise ValueError(
                f"Could not unpickle current settings for '{address}': {exc}"
            ) from exc

        updated_obj = _patch_value(current_obj, patch)
        publisher = self.publishers.get(comp.input_topic)
        if publisher is None:
            publisher = await self.ctx.publisher(comp.input_topic)
            self.publishers[comp.input_topic] = publisher

        await publisher.broadcast(updated_obj)
        return f"Published settings update to {comp.input_topic}"


def _parse_patch(json_text: str) -> dict[str, Any]:
    try:
        patch = json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON patch: {exc}") from exc
    if not isinstance(patch, dict):
        raise ValueError("Patch must be a JSON object")
    return patch


def _parse_address(host: str, port: int) -> tuple[str, int]:
    return (host, port)


async def _run_tui(host: str, port: int, auto_start: bool) -> None:
    address = _parse_address(host, port)

    async with GraphContext(address, auto_start=auto_start) as ctx:
        tui = SettingsTUI(ctx)
        await tui.initialize()
        try:
            while True:
                pending = await tui.drain_events()
                tui.render(pending_updates=pending)
                cmdline = (await asyncio.to_thread(input, "\nsettings-tui> ")).strip()
                if not cmdline:
                    continue

                cmd, *rest = cmdline.split(" ", 1)
                if cmd in {"q", "quit", "exit"}:
                    break

                if cmd in {"h", "help"}:
                    print(
                        "\nhelp:\n"
                        "  refresh\n"
                        "  inspect <row|address>\n"
                        "  set <row|address> <json-patch>\n"
                        "  quit\n"
                    )
                    await asyncio.to_thread(input, "Press Enter to continue...")
                    continue

                if cmd == "refresh":
                    await tui.refresh()
                    continue

                if cmd == "inspect":
                    if not rest:
                        print("Usage: inspect <row|address>")
                    else:
                        await tui.inspect(rest[0].strip())
                    await asyncio.to_thread(input, "Press Enter to continue...")
                    continue

                if cmd == "set":
                    if not rest:
                        print("Usage: set <row|address> <json-patch>")
                        await asyncio.to_thread(input, "Press Enter to continue...")
                        continue

                    target_and_patch = rest[0].strip()
                    if " " not in target_and_patch:
                        print("Usage: set <row|address> <json-patch>")
                        await asyncio.to_thread(input, "Press Enter to continue...")
                        continue

                    target, patch_text = target_and_patch.split(" ", 1)
                    try:
                        patch = _parse_patch(patch_text.strip())
                        result = await tui.set_settings(target.strip(), patch)
                        print(result)
                    except Exception as exc:
                        print(f"set failed: {exc}")
                    await asyncio.to_thread(input, "Press Enter to continue...")
                    continue

                print(f"Unknown command: {cmd}")
                await asyncio.to_thread(input, "Press Enter to continue...")
        finally:
            await tui.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ezmsg settings TUI")
    parser.add_argument("--host", default=DEFAULT_HOST, help="GraphServer host")
    parser.add_argument(
        "--port",
        type=int,
        default=GRAPHSERVER_PORT_DEFAULT,
        help="GraphServer port",
    )
    parser.add_argument(
        "--auto-start",
        action="store_true",
        help="Allow GraphContext to auto-start GraphServer if unavailable",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run_tui(args.host, args.port, args.auto_start))


if __name__ == "__main__":
    main()
