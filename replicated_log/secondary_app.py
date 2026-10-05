import logging
import os

from fastapi import FastAPI
from pydantic import BaseModel

from replicated_log.nodes import SecondaryNode

logging.basicConfig (
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

node_id = os.getenv('NODE_ID', 'secondary')
delay = os.getenv('DELAY', '0')

secondary_node = SecondaryNode(node_id, float(delay))

app = FastAPI()

class ReplicateRequest(BaseModel):
    message: str


@app.post('/replicate')
async def replicate(request: ReplicateRequest):
    return {'ack': await secondary_node.replicate(request.message)}

@app.get('/messages')
def list_messages():
    return {'messages': secondary_node.list_msgs()}

