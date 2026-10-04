from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from replicated_log.nodes import SecondaryNode

class Transport(ABC):
    @abstractmethod
    async def send(self, node_id: str, message: str) -> str:
        "send message to secondary node and receive ACK"


class InMemoryTransport(Transport):

    secondary_nodes: dict[str, "SecondaryNode"]

    def __init__(self):
        self.secondary_nodes = {}

    def register(self, node: "SecondaryNode"):
        self.secondary_nodes[node.node_id] = node

    def send(self, node_id: str, message: str) -> str:
        node = self.secondary_nodes.get(node_id)
        if node:
            return node.replicate(message)
