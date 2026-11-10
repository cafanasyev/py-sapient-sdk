# sapient-quickstart

A copy-paste starting point for a SAPIENT ASM node built on
[`sapient-sdk`](https://github.com/cafanasyev/py-sapient-sdk). Copy
this whole directory out to start a new project — it's a standalone uv
project, not part of the SDK's own package.

## Use it

```bash
cp -r examples/quickstart my-sapient-node
cd my-sapient-node
cp .env.example .env
# edit .env: FUSION_NODE_HOST, FUSION_NODE_PORT, FUSION_NODE_DESTINATION_ID
uv sync
uv run python main.py
```

## Layout

- **`main.py`** — wiring and the run loop only: build the client and
  dispatcher config, register the node, wait, shut down gracefully on
  Ctrl+C/SIGTERM. Read this first to see the whole flow at a glance. It also
  registers `_log_connection_state_change()` via
  `client.add_state_change_listener(...)`, logging every `CONNECTING` /
  `CONNECTED` / `DISCONNECTED` / `CLOSED` transition — a sample for hooking
  in real alerting/metrics on connection health.
- **`node.py`** — `QuickstartNode`, the smallest `Node` implementation the
  ICD will accept. Replace `get_registration()`/`get_status_report()` with
  your sensor's real capabilities, mode, and status — everything else
  (registration, keep-alive, jitter, reconnect, GOOD BYE) is handled for you
  by `NodeDispatcher`; see the
  [Automated Node Lifecycle Behavior](../../README.md#automated-node-lifecycle-behavior)
  table in the main SDK README for the full list. It also holds its own
  `dispatcher` reference and runs `send_detection_reports_periodically()` —
  Detection Reports are the one thing `NodeDispatcher` does *not* send on a
  schedule for you, so the node drives that loop itself (every
  `DETECTION_INTERVAL`, 10s by default); `main.py` starts it as a background
  task after registering and cancels it on shutdown. Replace the placeholder
  detection in that method with whatever your sensor actually detected.
- **`config.py`** — everything read from `.env`: connection settings, the
  plain-TCP-vs-TLS choice (`_build_socket_provider()` picks automatically
  based on whether `FUSION_NODE_TLS_CA_CERT` is set), and the `SocketClient`
  timeouts described below.

## Timeouts, explained

| `.env` setting | Default | What it controls |
|---|---|---|
| `SOCKET_HEALTH_CHECK_TYPE` | `netcat` | Which liveness check runs: `netcat`, `icmp`, `echo` or `pingpong`. See the SDK README, section "Health checks". |
| `SOCKET_HEALTH_CHECK_INTERVAL_SECONDS` | 10s | How often a check starts. |
| `SOCKET_HEALTH_CHECK_TIMEOUT_SECONDS` | 2s | How long one check may take. Never above the interval. |
| `SOCKET_HEALTH_CHECK_FAILURE_THRESHOLD` | 3 | Failed checks in a row before the connection is dropped. |
| `SOCKET_INITIAL_RECONNECT_DELAY_SECONDS` | 1s | Base delay for reconnect backoff (attempt *N* waits `min(N, 10) × initial_reconnect_delay`). |
| *(computed, not a setting)* `connection_loss_detection_delay` | `failure_threshold × interval + timeout` | Worst-case time between an actual network loss and the client noticing it. The client reports it and `NodeDispatcher` reads it from there. |
| *(not in `.env` here)* `reconnect_grace_period` | 2 minutes (SDK default, not overridden here) | How long the fusion node is assumed to retain a registration after a disconnect, per BSI Flex 335 v2.0 §4.9. Only change this if your fusion node's actual retention window differs from the spec. |

The first five defaults match `SocketClient`'s own built-in defaults — this
example just makes them overridable per-deployment via `.env` instead of
requiring a code change, the same way Java's test-harness exposes them via
`application.properties`.

## Dependency note

`sapient-sdk` isn't published to PyPI yet, so `pyproject.toml`
here pulls it from git (see `[tool.uv.sources]`). Once it's published, delete
that table and pin a normal version instead.