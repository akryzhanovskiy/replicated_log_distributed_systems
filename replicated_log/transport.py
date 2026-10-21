import httpx
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from replicated_log.nodes import SecondaryNode

class Transport(ABC):
    @abstractmethod
    async def send(self, node_id: str, seq: int, message: str) -> str:
        "send message to secondary node and receive ACK"


class InMemoryTransport(Transport):

    secondary_nodes: dict[str, "SecondaryNode"]

    def __init__(self):
        self.secondary_nodes = {}

    def register(self, node: "SecondaryNode"):
        self.secondary_nodes[node.node_id] = node

    async def send(self, node_id: str, seq: int, message: str) -> str:
        node = self.secondary_nodes.get(node_id)
        if node is None:
            raise KeyError(f"unknown secondary: {node_id}")
        return await node.replicate(seq, message)

class HttpTransport(Transport):

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def send(self, node_id: str, seq: int, message: str) -> str:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f'{node_id}/replicate', 
                json={"seq": seq, "message": message},
            )
            response.raise_for_status()

            return response.json()["ack"]
