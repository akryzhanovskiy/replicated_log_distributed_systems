import logging 
from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger('echo')

app = FastAPI()

class Message(BaseModel):
    text: str

@app.post("/echo")
async def echo(msg: Message):
    logger.info('Received message %s', msg.text)

    return msg