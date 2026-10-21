import asyncio

from replicated_log.transport import InMemoryTransport


class MockedTransport(InMemoryTransport):

    def __init__(self):
        super().__init__()
        self.delays: dict[str, float] = {}
        self._reorder_counts: dict[str, int] = {}
        self._held: dict[str, list[tuple[int, str, asyncio.Future]]] = {}
        # (node_id, seq) in the order messages actually reached the secondaries
        self.delivery_log: list[tuple[str, int]] = []

    def set_delay(self, target, seconds: float) -> None:
        self.delays[target.node_id] = seconds

    def remove_delay(self, target) -> None:
        self.delays.pop(target.node_id, None)

    def reorder_messages(self, target, messages_count: int = 2) -> None:
        if messages_count < 2:
            raise ValueError("messages_count must be at least 2 to reorder anything")
        self._reorder_counts[target.node_id] = messages_count
        self._held[target.node_id] = []

    def delivered_seqs(self, target) -> list[int]:
        return [seq for node_id, seq in self.delivery_log if node_id == target.node_id]

    async def send(self, node_id: str, seq: int, message: str) -> str:
        delay = self.delays.get(node_id, 0)
        if delay:
            await asyncio.sleep(delay)

        if node_id in self._reorder_counts:
            return await self._hold_and_reorder(node_id, seq, message)
        return await self._deliver(node_id, seq, message)

    async def _deliver(self, node_id: str, seq: int, message: str) -> str:
        self.delivery_log.append((node_id, seq))
        return await super().send(node_id, seq, message)

    async def _hold_and_reorder(self, node_id: str, seq: int, message: str) -> str:
        # Each held message waits on its own future until the batch is delivered.
        future = asyncio.get_running_loop().create_future()
        held = self._held[node_id]
        held.append((seq, message, future))

        if len(held) == self._reorder_counts[node_id]:
            del self._reorder_counts[node_id]
            batch = self._held.pop(node_id)
            for held_seq, held_message, held_future in reversed(batch):
                try:
                    ack = await self._deliver(node_id, held_seq, held_message)
                    held_future.set_result(ack)
                except Exception as e:
                    held_future.set_exception(e)

        return await future