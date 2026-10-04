import asyncio
import logging
from asyncio import Task
from replicated_log.transport import Transport

logger = logging.getLogger(__name__)

class SecondaryNode:
    node_id: str
    delay: float
    messages: list[str]

    def __init__(self, node_id: str, delay: float = 0):
        self.node_id = node_id
        self.delay = delay
        self.messages = []

    async def replicate(self, message: str) -> str:
        if self.delay:
            await asyncio.sleep(self.delay)

        self.messages.append(message)
        logger.info('secondary node %s stored message %s', self.node_id, message)
        return message
    
    def list_msgs(self) -> list[str]:
        logger.info('secondary node %s returned messages %s', self.node_id, self.messages)
        return list(self.messages)



    # async def delay(func, func_args, func_kwargs, delay):
    #     await asyncio.sleep(delay)
    #     return await func(*func_args, **func_kwargs)

class MasterNode:
    secondary_node_ids: list[str]
    transport: Transport
    messages: list[str]

    def __init__(self, secondary_nodes_ids: list[str], transport: Transport):
        self.secondary_node_ids = secondary_nodes_ids
        self.transport = transport
        self.messages = []

    async def append(self, message: str) -> None:
        self.messages.append(message)

        results = await asyncio.gather(
            *(self.transport.send(node_id, message) for node_id in self.secondary_node_ids), 
            return_exceptions=True
            )

        failures = {}
        for node_id, result in zip(self.secondary_node_ids, results):
            if isinstance(result, BaseException):
                failures[node_id] = repr(result)
            elif result != message:
                failures[node_id] = f"unexpected response: {result!r}"

        if failures:
            for node_id, reason in failures.items():
                logger.warning("Replication of %r to %s failed: %s", message, node_id, reason)
            raise RuntimeError(f"Replication failed for: {', '.join(failures)}")

        logger.info("Message %r replicated to all %d secondaries", message, len(results))

    def list_msgs(self) -> list[str]:
        return list(self.messages)
            
