"""Cross-process SHM grow / reattach coverage.

When a published message exceeds the publisher's shared-memory ``buf_size``,
``Publisher.broadcast`` grows the segment (allocate ``total_size*2``, copy
buffers, swap) and hands subscribers a new SHM name. Each subscriber must
detach from the old segment and reattach to the new one mid-stream. These tests
assert that the subscriber stays alive and every message is still delivered in
order across those grows.

The spawned publisher/subscriber units live in an importable package module so
multiprocessing ``spawn`` can unpickle them reliably under pytest.
"""

import json

import pytest

import ezmsg.core as ez

from ez_test_utils import get_test_fn
from ezmsg.core.shm_grow_test_support import GrowSystem, GrowSystemSettings


@pytest.mark.parametrize(
    "sizes",
    [
        (8, 8, 16384, 8, 8),
        (8, 16384, 8, 65536, 8),
        (16384, 8, 8),
    ],
)
def test_cross_process_grow_delivers_all(sizes):
    with get_test_fn() as test_filename:
        system = GrowSystem(
            GrowSystemSettings(
                sizes=sizes,
                buf_size=4096,
                num_buffers=4,
                output_fn=str(test_filename),
            )
        )
        ez.run(SYSTEM=system)

        results = []
        with open(test_filename, "r") as file:
            for line in file:
                results.append(json.loads(line))

        assert [r["seq"] for r in results] == list(range(len(sizes)))
        assert [r["len"] for r in results] == list(sizes)
