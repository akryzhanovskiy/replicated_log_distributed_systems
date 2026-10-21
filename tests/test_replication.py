import asyncio
import time

import pytest

from replicated_log.nodes import MasterNode, SecondaryNode
from tests.harness import MockedTransport

# Allowed overhead on top of the simulated delay when checking timings.
TOLERANCE = 0.5


async def wait_for_background_replication(master: MasterNode, timeout: float = 15) -> None:
    """Wait until replication tasks still running after append() have finished."""
    tasks = list(master._background_tasks)
    if not tasks:
        return
    _, pending = await asyncio.wait(tasks, timeout=timeout)
    for task in pending:
        task.cancel()


@pytest.fixture
async def cluster():
    """A fresh master with two secondaries connected through a MockedTransport."""
    transport = MockedTransport()
    secondary1 = SecondaryNode("secondary1")
    secondary2 = SecondaryNode("secondary2")
    transport.register(secondary1)
    transport.register(secondary2)
    master = MasterNode([secondary1.node_id, secondary2.node_id], transport)

    yield transport, master, secondary1, secondary2

    # With w < n + 1, replication continues after append() returns.
    # Let it finish so no task is left running when the test ends.
    await wait_for_background_replication(master)


async def timed_append(master: MasterNode, message: str, w: int | None = None) -> float:
    """Append a message through the master and return how long it took."""
    start = time.monotonic()
    await master.append(message, w)
    return time.monotonic() - start


# ---------------------------------------------------------------------------
# Iteration 1: blocking replication (default w = all nodes)
# ---------------------------------------------------------------------------


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

    # Parallel replication waits only for the slowest link (6 s).
    # Sequential replication would take 5 + 6 = 11 s.
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

    # Mid-replication: the fast secondary has the message, the slow one
    # does not yet, and the master is still waiting for its ACK.
    assert not task.done()
    assert secondary2.list_msgs() == ["m1"]
    assert secondary1.list_msgs() == []

    await task

    assert secondary1.list_msgs() == ["m1"]


async def test_harness_scenario(cluster):
    """The harness scenario from Iteration 1."""
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


# ---------------------------------------------------------------------------
# Iteration 2: write concern
# ---------------------------------------------------------------------------


async def test_write_concern_1_does_not_wait(cluster):
    transport, master, secondary1, secondary2 = cluster
    transport.set_delay(secondary1, 5)
    transport.set_delay(secondary2, 5)

    elapsed = await timed_append(master, "m1", w=1)

    # Only the master's own ACK is required.
    assert elapsed < TOLERANCE
    assert master.list_msgs() == ["m1"]
    assert secondary1.list_msgs() == []
    assert secondary2.list_msgs() == []

    # Replication still reaches every secondary in the background.
    await wait_for_background_replication(master)
    assert secondary1.list_msgs() == ["m1"]
    assert secondary2.list_msgs() == ["m1"]


async def test_write_concern_2_waits_for_fastest_secondary(cluster):
    transport, master, secondary1, secondary2 = cluster
    transport.set_delay(secondary1, 2)
    transport.set_delay(secondary2, 6)

    elapsed = await timed_append(master, "m1", w=2)

    # Master + one secondary: the fastest ACK (2 s) is enough.
    assert 2 <= elapsed < 2 + TOLERANCE
    assert secondary1.list_msgs() == ["m1"]
    assert secondary2.list_msgs() == []


async def test_write_concern_3_waits_for_all(cluster):
    transport, master, secondary1, secondary2 = cluster
    transport.set_delay(secondary1, 2)
    transport.set_delay(secondary2, 4)

    elapsed = await timed_append(master, "m1", w=3)

    # Master + both secondaries: waits for the slowest ACK (4 s).
    assert 4 <= elapsed < 4 + TOLERANCE
    assert secondary1.list_msgs() == ["m1"]
    assert secondary2.list_msgs() == ["m1"]


async def test_invalid_write_concern(cluster):
    transport, master, secondary1, secondary2 = cluster

    with pytest.raises(ValueError):
        await master.append("m1", w=0)
    with pytest.raises(ValueError):
        await master.append("m1", w=4)  # only 3 nodes exist

    # A rejected message is not stored anywhere.
    assert master.list_msgs() == []
    assert secondary1.list_msgs() == []
    assert secondary2.list_msgs() == []


async def test_eventual_consistency(cluster):
    transport, master, secondary1, secondary2 = cluster
    transport.set_delay(secondary2, 3)

    await master.append("m1", w=1)
    await master.append("m2", w=1)
    await asyncio.sleep(0.1)

    # Temporary inconsistency: the slow secondary lags behind.
    assert master.list_msgs() == ["m1", "m2"]
    assert secondary1.list_msgs() == ["m1", "m2"]
    assert secondary2.list_msgs() == []

    # Eventual consistency: after the delay, all nodes agree.
    await asyncio.sleep(3 + TOLERANCE)
    assert secondary2.list_msgs() == ["m1", "m2"]


async def test_acceptance_scenario(cluster):
    """The acceptance test from the task: S2 has a replication delay."""
    transport, master, secondary1, secondary2 = cluster
    transport.set_delay(secondary2, 6)

    # send (Msg1, W=1) - Ok
    assert await timed_append(master, "Msg1", w=1) < TOLERANCE

    # send (Msg2, W=2) - Ok (S1 has no delay)
    assert await timed_append(master, "Msg2", w=2) < TOLERANCE

    # send (Msg3, W=3) - waits because of S2 replication delay
    start = time.monotonic()
    msg3 = asyncio.create_task(master.append("Msg3", w=3))
    await asyncio.sleep(0.1)
    assert not msg3.done()

    # send (Msg4, W=1) - Ok, not blocked by Msg3
    assert await timed_append(master, "Msg4", w=1) < TOLERANCE
    assert not msg3.done()

    await msg3
    assert 6 <= time.monotonic() - start < 6 + TOLERANCE

    # Check messages on M and S1: all four
    assert master.list_msgs() == ["Msg1", "Msg2", "Msg3", "Msg4"]
    assert secondary1.list_msgs() == ["Msg1", "Msg2", "Msg3", "Msg4"]

    # Check messages on S2: Msg4 is still on its way
    assert secondary2.list_msgs() == ["Msg1", "Msg2", "Msg3"]

    # Eventually S2 catches up
    await wait_for_background_replication(master)
    assert secondary2.list_msgs() == ["Msg1", "Msg2", "Msg3", "Msg4"]


# ---------------------------------------------------------------------------
# Iteration 2: total ordering
# ---------------------------------------------------------------------------


async def test_reordered_messages_are_listed_in_order(cluster):
    transport, master, secondary1, secondary2 = cluster
    transport.reorder_messages(secondary1, messages_count=2)

    # w=1: with a higher w, m1 would wait for an ACK that only comes after m2 is sent.
    await master.append("m1", w=1)
    await asyncio.sleep(0.1)
    assert secondary1.list_msgs() == []  # m1 is held by the transport
    assert secondary2.list_msgs() == ["m1"]

    await master.append("m2", w=1)
    await asyncio.sleep(0.1)

    # The messages really arrived out of order...
    assert transport.delivered_seqs(secondary1) == [1, 0]
    # ...but are listed in the master's order.
    assert secondary1.list_msgs() == ["m1", "m2"]
    assert secondary2.list_msgs() == ["m1", "m2"]


async def test_secondary_hides_gaps():
    secondary = SecondaryNode("secondary")

    await secondary.replicate(0, "m1")
    await secondary.replicate(2, "m3")
    # seq 1 is missing, so m3 must not be visible yet
    assert secondary.list_msgs() == ["m1"]

    await secondary.replicate(1, "m2")
    assert secondary.list_msgs() == ["m1", "m2", "m3"]


async def test_duplicate_messages_are_ignored():
    secondary = SecondaryNode("secondary")

    ack1 = await secondary.replicate(0, "m1")
    ack2 = await secondary.replicate(0, "m1")

    # The duplicate is still ACKed, but stored only once.
    assert ack1 == "m1"
    assert ack2 == "m1"
    assert secondary.list_msgs() == ["m1"]