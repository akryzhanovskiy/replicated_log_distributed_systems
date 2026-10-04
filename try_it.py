import asyncio
import logging
import time

from replicated_log.nodes import MasterNode, SecondaryNode
from replicated_log.transport import InMemoryTransport

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")


async def main():
    transport = InMemoryTransport()
    s1 = SecondaryNode("secondary1", delay=2)
    s2 = SecondaryNode("secondary2", delay=3)
    transport.register(s1)
    transport.register(s2)

    master = MasterNode(["secondary1", "secondary2"], transport)

    start = time.time()
    await master.append("m1")
    print(f"append took {time.time() - start:.1f}s")

    print("master:", master.list_msgs())
    print("s1:", s1.list_msgs())
    print("s2:", s2.list_msgs())


asyncio.run(main())