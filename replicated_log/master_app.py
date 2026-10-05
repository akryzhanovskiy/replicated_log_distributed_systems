import logging
import os

from fastapi import FastAPI
from fastapi import HTTPException
from pydantic import BaseModel

from replicated_log.nodes import MasterNode
from replicated_log.transport import HttpTransport

logging.basicConfig (
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

secondaries = os.getenv('SECONDARIES', '')

secondary_ids = [x.strip() for x in secondaries.split(',')]
master_node = MasterNode(secondary_ids, HttpTransport())

logger.info("Master started with secondaries: %s", secondary_ids)

app = FastAPI()

class MessageRequest(BaseModel):
    message: str

@app.post('/messages')
async def append(request: MessageRequest):
    try:    
        await master_node.append(request.message)
        return {"status": "ok"}
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))

@app.get('/messages')
def list_messages():
    return {'messages': master_node.list_msgs()}