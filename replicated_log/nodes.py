import asyncio
import logging
from asyncio import Task
from replicated_log.transport import Transport

logger = logging.getLogger(__name__)

class SecondaryNode:
    node_id: str
    delay: float
    messages: dict[int, str]

    def __init__(self, node_id: str, delay: float = 0):
        self.node_id = node_id
        self.delay = delay
        self.messages = {}

    async def replicate(self, seq: int, message: str) -> str:
        if self.delay:
            await asyncio.sleep(self.delay)

        if seq in self.messages:
            logger.info('secondary node %s got duplicate message #%d %s', self.node_id, seq, message)
        else:
            self.messages[seq] = message
            logger.info('secondary node %s stored message #%d %s', self.node_id, seq, message)
        return message
            
    
    def list_msgs(self) -> list[str]:
        node_messages: list[str] = []
        seq = 0
        while seq in self.messages:
            node_messages.append(self.messages[seq])
            seq += 1

        logger.info('secondary node %s returned messages %s', self.node_id, node_messages)
        return node_messages


class MasterNode:
    secondary_node_ids: list[str]
    transport: Transport
    messages: list[str]
    _background_tasks: set

    def __init__(self, secondary_nodes_ids: list[str], transport: Transport):
        self.secondary_node_ids = secondary_nodes_ids
        self.transport = transport
        self.messages = []
        self._background_tasks = set()

    async def append(self, message: str, w: int | None = None) -> None:
        if w is None:
            w = len(self.secondary_node_ids) + 1
        if 1 <= w <= len(self.secondary_node_ids) + 1:
            seq = len(self.messages)
            self.messages.append(message)
            tasks = []
            for node_id in self.secondary_node_ids:
                task = asyncio.create_task(
                    self._replicate_to(node_id, seq, message)
                )
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
                tasks.append(task)

            if w == 1:
                logger.info(f"message #{seq} {message} ACKed successfully with {w} node")
                return

            got_ack_count = 0
            for next_done in asyncio.as_completed(tasks):
                try:
                    await next_done
                    got_ack_count += 1
                    if got_ack_count == w - 1:
                        logger.info(f"message #{seq} {message} ACKed successfully with {got_ack_count} nodes")
                        return
                except Exception as e:
                    logger.warning("Replication of %r failed: %s", message, e)

            
            raise RuntimeError(f"only {got_ack_count} of {w - 1} required ACKs received")
                
        else:
            raise ValueError(f"w must be between 1 and {len(self.secondary_node_ids)}, got {w}")
            


    def list_msgs(self) -> list[str]:
        return list(self.messages)

    async def _replicate_to(self, node_id: str, seq: int, message: str) -> None:
        result = await self.transport.send(node_id, seq, message)
        if result != message:
            raise RuntimeError(f"unexpected response: {result!r} for node {node_id}")
        logger.info("message %s ACKed by %s", message, node_id)
            
