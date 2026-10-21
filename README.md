# Replicated Log — Iteration 2

A replicated log with one **Master** and any number of **Secondaries**, built with Python, FastAPI and asyncio.

- The Master accepts new messages over HTTP, stores them in memory and replicates each one to every Secondary.
- Replication has **tunable semi-synchronicity**: with each message the client sends a **write concern** `w`, which defines how many acknowledgements (ACKs) the Master waits for before responding.
- Messages have a **total order**: every Secondary lists messages in exactly the same order as the Master, even if they arrive out of order.
- Every node runs in its own Docker container.

The Iteration 1 version (fully blocking replication) is available under the git tag `iteration-1`.

## Project structure

```
replicated-log/
├── replicated_log/
│   ├── nodes.py           # MasterNode and SecondaryNode (core logic, no HTTP)
│   ├── transport.py       # Transport interface, InMemoryTransport, HttpTransport
│   ├── master_app.py      # FastAPI app for the Master
│   └── secondary_app.py   # FastAPI app for a Secondary
├── tests/
│   ├── harness.py         # MockedTransport: programmable delays and message reordering
│   └── test_replication.py
├── Dockerfile
├── docker-compose.yml
├── pytest.ini
└── requirements.txt
```

## Design

### Separation of logic and network

The node logic is separated from the network through a `Transport` interface:

```
MasterNode ──> Transport ──> SecondaryNode
```

- `MasterNode` only calls `transport.send(node_id, seq, message)` and does not know how the message is delivered.
- `HttpTransport` is used in the real deployment and sends `POST /replicate` requests to Secondaries.
- `MockedTransport` (in `tests/harness.py`) is used in tests. It delivers messages in memory and can delay or reorder messages per link.

The same `MasterNode` and `SecondaryNode` code runs in Docker and in the test harness.

### Write concern

The Master itself counts as one acknowledgement. With `n` Secondaries, `w` can be between `1` and `n + 1`:

| `w` | ACKs required from Secondaries | Client gets a response… |
|-----|-------------------------------|-------------------------|
| 1   | 0 | as soon as the Master has stored the message |
| 2   | 1 | after the fastest Secondary has ACKed |
| 3   | 2 | after two Secondaries have ACKed (with two Secondaries: all of them) |

If `w` is omitted, the Master waits for all nodes, like in Iteration 1.

`w` only controls **how long the client waits**. The message is always sent to **every** Secondary:

1. The Master assigns the message a sequence number and stores it locally.
2. It starts one background task per Secondary (`asyncio.create_task`).
3. It waits, using `asyncio.as_completed`, until `w - 1` of those tasks have succeeded, then responds to the client.
4. The remaining tasks keep running in the background, so slower Secondaries still receive the message later.

Because of this, the system is **eventually consistent**: after a request with a low `w`, the Master may already list a message that a slow Secondary does not have yet. Once replication finishes, all nodes return the same list.

Requests are handled concurrently. A request waiting for ACKs (for example with `w=3` and a slow Secondary) does not block other requests, such as a later one with `w=1`.

### Total ordering

The Master defines the order of messages, and Secondaries follow it:

- **Sequence numbers.** Each message gets a sequence number equal to its position in the Master's log (0, 1, 2, …). The number is sent to the Secondaries together with the message.
- **Atomic assignment.** Reading the next number and appending the message happen without an `await` in between. asyncio runs on a single thread and switches between tasks only at `await`, so two concurrent requests can never get the same number.
- **Storage by sequence number.** A Secondary stores messages in a dict keyed by sequence number, not in arrival order.
- **Gap-free listing.** A Secondary lists messages from sequence number 0 up to the first missing number. If it has received messages 0, 1 and 3, it lists only 0 and 1; message 3 becomes visible once 2 arrives. A client therefore never sees messages out of order or with a gap.
- **Deduplication.** If a Secondary receives a sequence number it already has, it keeps the first copy and still returns an ACK.

A Secondary ACKs a message as soon as it has stored it, even if the message is not yet visible because an earlier one is missing.

### Two kinds of delay

- `DELAY` on a Secondary (environment variable) simulates a **slow node** in the Docker deployment.
- `MockedTransport.set_delay(...)` simulates a **slow network link** in the tests.

### Known limitations

- The Master stores a message before replicating it, so the Master can list a message that no Secondary has yet. This is expected with `w=1`.
- If replication to a Secondary fails, it is logged but not retried, and the client gets an error only if fewer than `w - 1` ACKs were received. In this iteration the communication channel is still assumed to be a perfect link.

## API

### Master (`http://localhost:8000`)

| Method | Path        | Body | Response |
|--------|-------------|------|----------|
| POST   | `/messages` | `{"message": "text", "w": 2}` | `{"status": "ok", "w": 2}` once the required ACKs are received |
| GET    | `/messages` | – | `{"messages": ["m1", ...]}` |

- `w` is optional. Without it, the Master waits for all nodes (the response then shows `"w": null`).
- An invalid `w` (less than 1 or greater than the number of nodes) returns **HTTP 400**.
- If fewer than `w - 1` Secondaries ACK the message, the response is **HTTP 502**.

### Secondary (`http://localhost:8001`, `http://localhost:8002`)

| Method | Path         | Body | Response |
|--------|--------------|------|----------|
| GET    | `/messages`  | – | `{"messages": ["m1", ...]}` (gap-free, in total order) |
| POST   | `/replicate` | `{"seq": 0, "message": "text"}` | `{"ack": "text"}` (internal, called by the Master) |

Each service also provides interactive API docs at `/docs` (for example `http://localhost:8000/docs`), which can be used from a browser on any operating system.

### Configuration (environment variables)

| Variable      | Used by   | Example | Description |
|---------------|-----------|---------|-------------|
| `SECONDARIES` | Master    | `http://secondary1:8000,http://secondary2:8000` | Comma-separated base URLs of the Secondaries |
| `NODE_ID`     | Secondary | `secondary1` | Node name used in logs |
| `DELAY`       | Secondary | `8` | Seconds to sleep before storing each message |

## Running with Docker

Requirements: Docker Desktop (or Docker Engine with the Compose plugin).

```bash
docker compose up --build
```

This starts three containers:

| Service      | Host port | Delay |
|--------------|-----------|-------|
| `master`     | 8000      | –     |
| `secondary1` | 8001      | none  |
| `secondary2` | 8002      | 8 s   |

Logs of all nodes are shown in the same terminal. To follow logs from a separate terminal, run `docker compose logs -f`.

To stop everything, press `Ctrl+C` and then run:

```bash
docker compose down
```

To add more Secondaries, add another service to `docker-compose.yml` and include its URL in the Master's `SECONDARIES` variable.

## Trying it out manually

### Write concern (macOS / Linux)

```bash
time curl -X POST localhost:8000/messages -H "Content-Type: application/json" -d '{"message":"m1","w":1}'
time curl -X POST localhost:8000/messages -H "Content-Type: application/json" -d '{"message":"m2","w":2}'
time curl -X POST localhost:8000/messages -H "Content-Type: application/json" -d '{"message":"m3","w":3}'
```

Expected: `w=1` and `w=2` return almost immediately, `w=3` takes about 8 seconds because it waits for `secondary2`.

An invalid write concern is rejected immediately with HTTP 400:

```bash
curl -X POST localhost:8000/messages -H "Content-Type: application/json" -d '{"message":"bad","w":5}'
```

### Temporary inconsistency and eventual consistency

Send a message with `w=1` and list `secondary2` right away, then again after the delay:

```bash
curl -X POST localhost:8000/messages -H "Content-Type: application/json" -d '{"message":"x","w":1}'
curl localhost:8000/messages   # already contains "x"
curl localhost:8002/messages   # does not contain "x" yet
sleep 9
curl localhost:8002/messages   # now contains "x"
```

### Acceptance scenario (macOS / Linux)

Restart the containers first (`docker compose down && docker compose up`) so the log starts empty. Then run:

```bash
curl -X POST localhost:8000/messages -H "Content-Type: application/json" -d '{"message":"Msg1","w":1}'
curl -X POST localhost:8000/messages -H "Content-Type: application/json" -d '{"message":"Msg2","w":2}'

# Msg3 waits for secondary2; run it in the background with '&'
time curl -X POST localhost:8000/messages -H "Content-Type: application/json" -d '{"message":"Msg3","w":3}' &

# Msg4 is not blocked by Msg3 and returns immediately
curl -X POST localhost:8000/messages -H "Content-Type: application/json" -d '{"message":"Msg4","w":1}'

# wait until Msg3 has returned, then compare the nodes
wait
curl localhost:8000/messages   # Msg1, Msg2, Msg3, Msg4
curl localhost:8001/messages   # Msg1, Msg2, Msg3, Msg4
curl localhost:8002/messages   # Msg1, Msg2, Msg3 (Msg4 is still on its way)
```

A few seconds later, `secondary2` also lists all four messages.

### Windows (PowerShell)

```powershell
# Append with a write concern and measure the time
Measure-Command {
  Invoke-RestMethod -Method Post -Uri http://localhost:8000/messages `
    -ContentType "application/json" -Body '{"message":"m1","w":1}'
}

# List messages on every node
Invoke-RestMethod http://localhost:8000/messages
Invoke-RestMethod http://localhost:8001/messages
Invoke-RestMethod http://localhost:8002/messages
```

For the acceptance scenario on Windows, send `Msg3` from one PowerShell window and `Msg4` from a second window while `Msg3` is still waiting.

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

The suite takes about a minute because it waits for simulated network delays.

### Test harness

`MockedTransport` controls the links between the Master and each Secondary:

```python
transport.set_delay(secondary1, 5)                        # delay messages to secondary1 by 5 s
transport.remove_delay(secondary1)
transport.reorder_messages(secondary1, messages_count=2)  # hold 2 messages, deliver them in reverse order
```

### What the tests verify

| Test | What it proves |
|------|----------------|
| `test_append_blocks_until_delayed_secondary_acks` | Without `w`, `append` blocks until all Secondaries ACK (Iteration 1 behavior) |
| `test_replication_is_parallel` | Replication to Secondaries runs concurrently (5 s and 6 s delays take ~6 s, not 11 s) |
| `test_no_delay_no_blocking` | Without delays, `append` returns almost immediately |
| `test_master_is_blocked_during_replication` | Mid-replication, the Master is still waiting, the fast Secondary has the message and the slow one does not |
| `test_harness_scenario` | The Iteration 1 harness scenario (m1, m2, m3) |
| `test_write_concern_1_does_not_wait` | With `w=1`, `append` returns immediately even if all Secondaries are slow |
| `test_write_concern_2_waits_for_fastest_secondary` | With `w=2`, `append` waits only for the fastest Secondary |
| `test_write_concern_3_waits_for_all` | With `w=3`, `append` waits for both Secondaries |
| `test_invalid_write_concern` | `w=0` and `w > n + 1` are rejected |
| `test_eventual_consistency` | A slow Secondary temporarily lists fewer messages than the Master, then catches up |
| `test_acceptance_scenario` | The acceptance scenario from the task (Msg1–Msg4, `w=1,2,3,1`) |
| `test_reordered_messages_are_listed_in_order` | Messages delivered out of order are still listed in the correct order |
| `test_secondary_hides_gaps` | A Secondary does not list messages after a missing sequence number |
| `test_duplicate_messages_are_ignored` | A duplicate sequence number is ACKed but not stored twice |

## Running without Docker (optional)

Each node can also be started directly with uvicorn, one per terminal (macOS / Linux syntax):

```bash
NODE_ID=secondary1 DELAY=0 uvicorn replicated_log.secondary_app:app --port 8001
NODE_ID=secondary2 DELAY=8 uvicorn replicated_log.secondary_app:app --port 8002
SECONDARIES=http://localhost:8001,http://localhost:8002 uvicorn replicated_log.master_app:app --port 8000
```

## Logging

All nodes use Python's `logging` module with timestamps. The logs show:

- the Master's configured Secondaries on startup,
- each Secondary storing a message (with its sequence number) or detecting a duplicate,
- each ACK received by the Master,
- the Master responding to the client once the write concern is satisfied, or warning which Secondaries failed.