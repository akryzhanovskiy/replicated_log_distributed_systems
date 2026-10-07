# Replicated Log — Iteration 1

A simple replicated log with one **Master** and any number of **Secondaries**, built with Python, FastAPI and asyncio.

- The Master accepts new messages over HTTP, stores them in memory and replicates each one to every Secondary.
- Replication is **blocking**: a `POST` to the Master completes only after **all** Secondaries have acknowledged (ACK) the message.
- Replication runs **in parallel**: the Master waits only as long as the slowest Secondary, not the sum of all delays.
- Every node runs in its own Docker container.

## Project structure

```
replicated-log/
├── replicated_log/
│   ├── nodes.py           # MasterNode and SecondaryNode (core logic, no HTTP)
│   ├── transport.py       # Transport interface, InMemoryTransport, HttpTransport
│   ├── master_app.py      # FastAPI app for the Master
│   └── secondary_app.py   # FastAPI app for a Secondary
├── tests/
│   ├── harness.py         # MockedTransport with programmable network delays
│   └── test_replication.py
├── Dockerfile
├── docker-compose.yml
├── pytest.ini
└── requirements.txt
```

## Design

The node logic is separated from the network through a `Transport` interface:

```
MasterNode ──> Transport ──> SecondaryNode
```

- `MasterNode` only calls `transport.send(node_id, message)` and does not know how the message is delivered.
- `HttpTransport` is used in the real deployment and sends `POST /replicate` requests to Secondaries over HTTP.
- `MockedTransport` (in `tests/harness.py`) is used in tests. It delivers messages in memory and can simulate network delays per link.

Because of this, the same `MasterNode` code runs in Docker and in the test harness.

**Replication flow** (`MasterNode.append`):

1. The message is appended to the Master's in-memory list.
2. The message is sent to all Secondaries concurrently with `asyncio.gather`.
3. The Master waits until every Secondary responds. A Secondary acknowledges by returning the message it stored.
4. If any Secondary fails or returns an unexpected ACK, the failure is logged and an error is raised, so the client's `POST` fails instead of reporting success.

**Two kinds of delay:**

- `DELAY` on a Secondary (environment variable) simulates a **slow node** in the Docker deployment. It is used to demonstrate that replication is blocking.
- `MockedTransport.set_delay(...)` simulates a **slow network link** in the tests.

**Known limitation:** the Master stores the message locally before replicating it. While replication is in progress (or if it fails), the Master may already show a message that the Secondaries do not have yet. In this iteration the communication channel is assumed to be a perfect link, so replication failures are not expected.

## API

### Master (`http://localhost:8000`)

| Method | Path        | Body                    | Response                      |
|--------|-------------|-------------------------|-------------------------------|
| POST   | `/messages` | `{"message": "text"}`   | `{"status": "ok"}` after all ACKs |
| GET    | `/messages` | –                       | `{"messages": ["m1", ...]}`   |

If replication to any Secondary fails, `POST /messages` returns an error response (HTTP 502) with the list of failed Secondaries.

### Secondary (`http://localhost:8001`, `http://localhost:8002`)

| Method | Path         | Body                  | Response                    |
|--------|--------------|-----------------------|-----------------------------|
| GET    | `/messages`  | –                     | `{"messages": ["m1", ...]}` |
| POST   | `/replicate` | `{"message": "text"}` | `{"ack": "text"}` (internal, called by the Master) |

Each service also provides interactive API docs at `/docs` (for example `http://localhost:8000/docs`), which can be used from a browser on any operating system.

### Configuration (environment variables)

| Variable      | Used by   | Example                                        | Description                                 |
|---------------|-----------|------------------------------------------------|---------------------------------------------|
| `SECONDARIES` | Master    | `http://secondary1:8000,http://secondary2:8000` | Comma-separated base URLs of Secondaries    |
| `NODE_ID`     | Secondary | `secondary1`                                   | Node name used in logs                      |
| `DELAY`       | Secondary | `2`                                            | Seconds to sleep before storing a message   |

## Running with Docker

Requirements: Docker Desktop (or Docker Engine with the Compose plugin).

```bash
docker compose up --build
```

This starts three containers:

| Service      | Host port | Delay |
|--------------|-----------|-------|
| `master`     | 8000      | –     |
| `secondary1` | 8001      | 2 s   |
| `secondary2` | 8002      | 3 s   |

Logs of all nodes are shown in the same terminal. To follow logs from a separate terminal, run `docker compose logs -f`.

To stop everything, press `Ctrl+C` and then run:

```bash
docker compose down
```

To add more Secondaries, add another service to `docker-compose.yml` and include its URL in the Master's `SECONDARIES` variable.

## Trying it out manually

### macOS / Linux

```bash
# Append a message and measure how long the request blocks
time curl -X POST localhost:8000/messages -H "Content-Type: application/json" -d '{"message":"m1"}'

# List messages on every node
curl localhost:8000/messages
curl localhost:8001/messages
curl localhost:8002/messages
```

### Windows (PowerShell)

```powershell
# Append a message and measure how long the request blocks
Measure-Command {
  Invoke-RestMethod -Method Post -Uri http://localhost:8000/messages `
    -ContentType "application/json" -Body '{"message":"m1"}'
}

# List messages on every node
Invoke-RestMethod http://localhost:8000/messages
Invoke-RestMethod http://localhost:8001/messages
Invoke-RestMethod http://localhost:8002/messages
```

### Expected result

- The `POST` takes about **3 seconds**: the Master waits for the slowest Secondary (3 s). Sequential replication would take 2 + 3 = 5 seconds.
- All three nodes return `{"messages": ["m1"]}`.
- The logs show both Secondaries storing the message about one second apart, followed by the Master's "replicated to all secondaries" line.

## Running the tests

The tests run locally, without Docker. Python 3.12+ is required (developed on Python 3.14).

macOS / Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -v
```

Windows (PowerShell):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest -v
```

The full suite takes about 30 seconds because it waits for simulated network delays.

### What the tests verify

The tests use `MockedTransport`, which allows setting a delay on the link between the Master and a specific Secondary:

```python
transport.set_delay(secondary1, 5)   # messages to secondary1 are delayed by 5 s
transport.remove_delay(secondary1)
```

| Test | What it proves |
|------|----------------|
| `test_append_blocks_until_delayed_secondary_acks` | `append` blocks until the delayed Secondary ACKs, and all nodes have the message when it returns |
| `test_replication_is_parallel` | With delays of 5 s and 6 s, `append` takes ~6 s, not 11 s |
| `test_no_delay_no_blocking` | Without delays, `append` returns almost immediately |
| `test_master_is_blocked_during_replication` | Mid-replication, the Master is still waiting, the fast Secondary has the message and the slow one does not yet |
| `test_harness_scenario` | The full harness scenario from the task (m1, m2, m3) |

## Running without Docker (optional)

Each node can also be started directly with uvicorn, one per terminal (macOS / Linux syntax):

```bash
NODE_ID=secondary1 DELAY=2 uvicorn replicated_log.secondary_app:app --port 8001
NODE_ID=secondary2 DELAY=3 uvicorn replicated_log.secondary_app:app --port 8002
SECONDARIES=http://localhost:8001,http://localhost:8002 uvicorn replicated_log.master_app:app --port 8000
```

## Logging

All nodes use Python's `logging` module with timestamps. The logs show:

- the Master's configured Secondaries on startup,
- each Secondary storing a message,
- the Master confirming that a message was replicated to all Secondaries, or warning which Secondaries failed.