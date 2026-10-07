import asyncio
import time

import pytest

from replicated_log.nodes import MasterNode, SecondaryNode
from tests.harness import MockedTransport

TOLERANCE = 0.5

@pytest.fixture
def cluster():
    transport = MockedTransport()
    secondary1 = SecondaryNode("secondary1")
    secondary2 = SecondaryNode("secondary2")
    transport.register(secondary1)
    transport.register(secondary2)
    master = MasterNode([secondary1.node_id, secondary2.node_id], transport)
    return transport, master, secondary1, secondary2


async def timed_append(master: MasterNode, message: str) -> float:
    start = time.monotonic()
    await master.append(message)
    return time.monotonic() - start


async def test_append_blocks_until_delayed_secondary_acks(cluster):
    transport, master, secondary1, secondary2 = cluster
    transport.set_delay(secondary1, 5)

    elapsed = await timed_append(master, "m1")

    assert 5 <= elapsed < 5 + TOLERANCE

    assert master.list_msgs() == ["m1"]
    assert secondary1.list_msgs() == ["m1"]
    assert secondary2.list_msgs() == ["m1"]


async def test_replication_is_parallel(cluster):
    transport, master, secondary1, secondary2 = cluster
    transport.set_delay(secondary1, 5)
    transport.set_delay(secondary2, 6)

    elapsed = await timed_append(master, "m1")

    assert 6 <= elapsed < 6 + TOLERANCE

    assert secondary1.list_msgs() == ["m1"]
    assert secondary2.list_msgs() == ["m1"]


async def test_no_delay_no_blocking(cluster):
    transport, master, secondary1, secondary2 = cluster

    elapsed = await timed_append(master, "m1")

    assert elapsed < TOLERANCE
    assert master.list_msgs() == ["m1"]
    assert secondary1.list_msgs() == ["m1"]
    assert secondary2.list_msgs() == ["m1"]


async def test_master_is_blocked_during_replication(cluster):
    transport, master, secondary1, secondary2 = cluster
    transport.set_delay(secondary1, 3)

    task = asyncio.create_task(master.append("m1"))
    await asyncio.sleep(1)

    assert not task.done()
    assert secondary2.list_msgs() == ["m1"]
    assert secondary1.list_msgs() == []

    await task

    assert secondary1.list_msgs() == ["m1"]


async def test_harness_scenario(cluster):
    transport, master, secondary1, secondary2 = cluster

    transport.set_delay(secondary1, 5)
    elapsed = await timed_append(master, "m1")  # blocked for 5 s
    assert 5 <= elapsed < 5 + TOLERANCE
    assert master.list_msgs() == ["m1"]

    transport.set_delay(secondary2, 6)
    elapsed = await timed_append(master, "m2")  # blocked for 6 s, not 11 s
    assert 6 <= elapsed < 6 + TOLERANCE
    assert master.list_msgs() == ["m1", "m2"]

    transport.remove_delay(secondary1)
    transport.remove_delay(secondary2)
    elapsed = await timed_append(master, "m3")  # no blocking
    assert elapsed < TOLERANCE

    assert master.list_msgs() == ["m1", "m2", "m3"]
    assert secondary1.list_msgs() == ["m1", "m2", "m3"]
    assert secondary2.list_msgs() == ["m1", "m2", "m3"]