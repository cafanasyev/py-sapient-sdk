# py-sapient-sdk

[![CI](https://github.com/cafanasyev/py-sapient-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/cafanasyev/py-sapient-sdk/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/cafanasyev/py-sapient-sdk/graph/badge.svg)](https://codecov.io/gh/cafanasyev/py-sapient-sdk)

Python SDK for [BSI Flex 335 v2.0](https://www.bsigroup.com/en-US/insights-and-media/insights/brochures/bsi-flex-335-interface-of-the-sapient-sensor-management-specification/) SAPIENT — a protocol standard for autonomous sensor and effector interoperability. The SDK provides TCP client connectivity and node dispatching for communicating with SAPIENT fusion nodes using Pydantic-typed SAPIENT messages.

Java counterpart:
[java-sapient-sdk](https://github.com/cafanasyev/java-sapient-sdk).

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Build

```bash
uv sync
```

## Test

```bash
uv run pytest
```

Tests run in parallel by default (`pytest -n auto`, via `pytest-xdist`).

## Structure

| Package | Description |
|---|---|
| `sapient_sdk.transport` | TCP transport layer — publish/subscribe over typed SAPIENT messages, automatic reconnection, mTLS support |
| `sapient_sdk.transmission` | Transmission module — node registration, status reporting, ack handling |

## Code Quality

The project uses two static analysis tools that run automatically during `make check`:

| Tool | What it does | Runs during |
|---|---|---|
| [Ruff](https://docs.astral.sh/ruff/) (lint) | Style/lint checks (`E`, `F`, `W`, `I`, `UP`, `B` rule sets) | `make lint` |
| [Ruff](https://docs.astral.sh/ruff/) (format) | Code formatting | `make format` |
| [Mypy](https://mypy-lang.org/) | Static type checking, strict mode | `make typecheck` |

### Standalone commands

```bash
# Check lint
make lint

# Auto-fix lint issues
make lint-fix

# Format code
make format

# Type-check
make typecheck

# Run everything (lint + typecheck + test)
make check
```

### IDE setup

Install the [Ruff extension](https://docs.astral.sh/ruff/editors/) for your editor (VS Code: `charliermarsh.ruff`; PyCharm/IntelliJ: the Ruff plugin) so on-save formatting matches `ruff format` and lint warnings match `ruff check`.

### How to use SDK

The SDK is built around four abstract classes. To familiarize yourself with the internal logic without a deep dive into the implementation, read these four — they are documented and cover the whole flow:

- [`Node`](src/sapient_sdk/transmission/node.py) — a SAPIENT edge/fusion node you implement (identity, registration, and server-to-node callbacks).
- [`NodeDispatcher`](src/sapient_sdk/transmission/dispatcher.py) — manages node registration, the keep-alive/status-report lifecycle, and message routing. `DefaultNodeDispatcher` is the built-in implementation.
- [`Client`](src/sapient_sdk/transport/client.py) — the transport: publish/subscribe typed SAPIENT messages plus connection-state monitoring. `SocketClient` is the built-in TCP implementation.
- [`SocketProvider`](src/sapient_sdk/transport/socket_provider.py) — supplies the socket (host/port and TLS vs. plain) to the client. `PlainSocketProvider` covers plain TCP, `TlsSocketProvider` covers TLS/mTLS.

Add the SDK to your project. Note: `cafanasyev-sapient-sdk` itself is not yet published to PyPI (its dependencies are).

```bash
uv add cafanasyev-sapient-sdk
```

```bash
pip install cafanasyev-sapient-sdk
```

1. Implement the [`Node`](src/sapient_sdk/transmission/node.py) abstract class for each node you want to connect. All methods are documented with their purpose.
2. Create an instance of [`DefaultNodeDispatcher`](src/sapient_sdk/transmission/dispatcher.py). In order to do so:
    * Implement or reuse a [`SocketProvider`](src/sapient_sdk/transport/socket_provider.py) — `PlainSocketProvider` for a non-TLS connection, or `TlsSocketProvider` for TLS/mTLS. For TLS, build the `ssl.SSLContext` with `build_client_ssl_context(ca_cert, client_cert, client_key)`. Both PEM and DER encodings are accepted (keys: PKCS#8, PKCS#1, SEC1/EC), and each argument is read as:
        * `bytes` — the certificate/key itself;
        * a `str` starting with `-----BEGIN` — inline PEM text;
        * any other `str`, or a `Path` — a filesystem path.

      Using this factory is optional: implement `SocketProvider` yourself to supply any `ssl.SSLContext` (or socket) you like.
    * Instantiate [`NodeDispatcherConfig`](src/sapient_sdk/transmission/dispatcher_config.py).
    * Instantiate [`SocketClient`](src/sapient_sdk/transport/socket_client.py).
    * Instantiate `DefaultNodeDispatcher(client=..., config=...)`.

   [`examples/quickstart`](examples/quickstart) wires all of the above together into a copy-paste starting point that serves as a reference implementation using this SDK — see its README for what to configure and run against a real fusion node.
3. Pass your `Node` implementations to `dispatcher.register(node)`. This is what makes the dispatcher automate node lifecycle management for you — see [Automated Node Lifecycle Behavior](#automated-node-lifecycle-behavior) below for the full list of what that gets you for free.
4. Stop managing a node with `await dispatcher.unregister(node)` when you no longer want the dispatcher to report for it.
5. Close the dispatcher with `await dispatcher.close()` to shut down the connection and its background tasks when you are done.
6. OPTIONALLY:
   * Use the dispatcher to send Detection Reports/Alerts/Task Acks via the typed `dispatcher.publish(msg, node_id, timeout)` method;
   * You can invoke sending of Registrations/Status Reports outside of the dispatcher's automatic lifecycle — for example if you want to notify the server about some changes immediately without waiting for the next interval — using the same `publish(...)` method;
   * Call [`client.add_state_change_listener(...)`](src/sapient_sdk/transport/client.py) to subscribe to connection state changes (and `remove_state_change_listener(...)` to unsubscribe). You may want to log or run additional logic when, for example, the connection is lost for a prolonged period. You can also poll the connection directly with `client.state`, `client.is_connected`, and `await client.probe_reachable(timeout)`.

### Automated Node Lifecycle Behavior

Everything `dispatcher.register(node)` automates for you — you don't need to implement any of this yourself — compared across both SDKs:

| Behavior | Details | Java | Python |
|---|---|---|---|
| Regularly poll `Node.is_online()`/`isOnline()` to detect online/offline transitions | Drives every other behavior below. | ✅ | ✅ |
| Auto-send Registration when a node comes online | Obtains the Registration message from the node implementation and sends it to the fusion node (server). | ✅ | ✅ |
| Auto Status Report keep-alive | Sends automatic Status Reports on the interval stated in the node's Registration. | ✅ | ✅ |
| Jitter — one-time phase offset before first status report | A one-time random phase offset in `[0, statusInterval)` before the first status report of each (re-)registered loop, so nodes that share a `statusInterval` don't start in sync. | ✅ | ✅ |
| Jitter — ±10% per-cycle on subsequent status reports | A fresh per-cycle jitter of `statusInterval ± 10%` on every subsequent Status Report, so nodes drift apart instead of re-synchronising — the mean send rate stays exactly `statusInterval`, and the ±10% stays well inside the protocol's 3-missed-report budget. | ✅ | ✅ |
| Jitter — random delay before registration/re-registration | A random delay in `[0, registrationJitterWindow)` (default 2 seconds) before every registration/re-registration, to spread the registration storm when many clients reconnect at once (e.g. after a fusion-server restart). Tunable; set to `0`/`Duration.ZERO` to disable, e.g. in tests. | ✅ | ✅ |
| Auto GOOD BYE Status Report on going offline | Sends a GOOD BYE Status Report to de-register the node when it becomes offline. | ✅ | ✅ |
| Route server messages to the node's callbacks | Routes server messages (RegistrationAck, AlertAck, Error, Task) to the required node's callback methods. | ✅ | ✅ |
| Open connection when a node comes online | Keeps the connection open as long as at least one online node is registered. | ✅ | ✅ |
| Close connection when no nodes are online | Closes the connection once no online nodes are left. | ✅ | ✅ |
| Re-open connection when a node comes online again | Re-opens the connection if at least one online node reappears. | ✅ | ✅ |
| Auto `StatusReport.Info = INFO_UNCHANGED` when unchanged | Set automatically if the last message has no changes, so unchanged reports aren't treated as new events. | ✅ | ✅ |
| Auto-populate `StatusReport.ReportId` if blank | Fills in a fresh ULID before sending, so callers don't need to mint one themselves. | ✅ | ✅ |
| Auto-populate `DetectionReport.ReportId` if blank | Fills in a fresh ULID before sending, so callers don't need to mint one themselves. | ✅ | ✅ |

### Logging

The SDK logs through Python's standard [`logging`](https://docs.python.org/3/library/logging.html) module, under loggers named after each module (e.g. `sapient_sdk.transmission.dispatcher`). Configure a handler and level in your application to see output — without one, Python's logging defaults apply (`WARNING` and above, to stderr). Log level is configured through whichever handler/config you set up (there is nothing SDK-specific to set).

What each level shows:

| Level | What is logged |
|---|---|
| `INFO` | One-line summaries: node registration/unregistration, GOOD BYE on going offline, and one line per message with its type and destination node (`sending <type> …`, `received <type> for node: …`). |
| `DEBUG` | Everything from `INFO`, **plus the full JSON body of every message** sent and received. Use this to inspect the exact contents of Registrations, Status Reports, Acks, Tasks, Errors, etc. |
| `WARNING` / `ERROR` | Recoverable and error conditions (e.g. no node registered for a destination, publish timeouts, failed connection close). |

`DEBUG` is verbose — it prints the entire body of every message on the wire — so it is best reserved for diagnosing protocol issues rather than for normal operation.


## License

This project is released into the public domain under the [Unlicense](https://unlicense.org).