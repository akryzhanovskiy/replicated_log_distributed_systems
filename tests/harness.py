import asyncio

from replicated_log.transport import InMemoryTransport
from replicated_log.nodes import SecondaryNode

class MockedTransport(InMemoryTransport):

    delays: dict[str, float]

    def __init__(self):
        super().__init__()
        self.delays = {}

    def set_delay(self, target: SecondaryNode, seconds: float) -> None:
        self.delays[target.node_id] = seconds

    def remove_delay(self, target: SecondaryNode) -> None:
        self.delays.pop(target.node_id, None)

    async def send(self, node_id: str, seq: int, message: str) -> str:
        delay = self.delays.get(node_id, 0)
        if delay:
            await asyncio.sleep(delay)
        return await super().send(node_id, seq, message)