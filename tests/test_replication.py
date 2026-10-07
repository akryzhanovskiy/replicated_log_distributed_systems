import asyncio
import time

import pytest

from replicated_log.nodes import MasterNode, SecondaryNode
from tests.harness import MockedTransport


@pytest.fixture
def cluster():
    transport = MockedTransport()
    secondary1 = SecondaryNode("secondary1")
    secondary2 = SecondaryNode("secondary2")
    transport.register(secondary1)
    transport.register(secondary2)
    master = MasterNode([secondary1.node_id, secondary2.node_id], transport)
    return transport, master, secondary1, secondary2


async def test_append_blocks_until_delayed_secondary_acks(cluster):
    transport, master, secondary1, secondary2 = cluster
    transport.set_delay(secondary1, 5)

    start = time.monotonic()
    await master.append("m1")
    elapsed = time.monotonic() - start

    assert 5 <= elapsed < 5.5

    assert master.list_msgs() == ["m1"]
    assert secondary1.list_msgs() == ["m1"]
    assert secondary2.list_msgs() == ["m1"]