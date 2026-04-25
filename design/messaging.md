# Rendering Daemon Messaging and Control Design

## Purpose

This document defines the messaging design for the Rendering Daemon. It focuses on how the daemon
communicates with the Client, how commands and responses flow through the system, and how overall
daemon state is managed.

## High-Level Architecture

The daemon consists of:

* one **Orchestrator** running in the main process
* sub processes for:

    * **Reader**
    * **Worker**
    * **Writer**
* two helper threads in the main process for client communication:

    * **Command_rcv**
    * **Command_rsp**

The Orchestrator is the central coordinator. It is the sole owner of overall daemon state and is the only component that
makes decisions about request acceptance, request lifecycle, batch execution, and subsystem coordination.

The worker processes perform rendering work only. They have no knowledge of the Client, socket protocol, or JSON message
format.

The two command threads act only as proxies between the socket and the Orchestrator.

## Core Design Principles

### Orchestrator owns all overall state

The Orchestrator is the only component that owns and mutates overall daemon state.

This includes:

* whether the daemon is idle or busy
* the currently active request
* render request lifecycle
* subsystem configuration lifecycle
* request acceptance, rejection, or cancellation decisions
* generation of Client-facing responses

No thread or worker process other than the Orchestrator may modify overall state.

### Socket threads are proxies only

`Command_rcv` and `Command_rsp` are adapters between the socket and the Orchestrator. 

Their responsibilities are intentionally narrow:

* `Command_rcv` receives socket messages, performs basic syntax validation, and forwards  commands
  to the Orchestrator
* `Command_rsp` receives Orchestrator-generated responses and writes them to the socket


### Workers are protocol-blind

Reader, Worker, and Writer tasks operate only on internal envelopes and work packets. They do not know that an
Client exists, and they do not know anything about socket format, JSON structure, or request/response protocol.

## Components

## Orchestrator

The Orchestrator runs in the main process and acts as the central event loop and control authority.

### Responsibilities

The Orchestrator shall:

* start and manage worker processes
* start and manage `Command_rcv` and `Command_rsp`
* own all overall daemon state
* receive inbound events from `status_queue`
* interpret commands received from the Client
* validate request semantics beyond syntax
* decide whether requests are accepted, rejected, deferred, or cancelled
* dispatch work and control messages to subsystem queues
* generate all Client-facing response messages
* send those responses to `command_rsp_queue`

## Command_rcv

`Command_rcv` is a helper thread in the main process.

It is the only component allowed to: 1) read from the Client socket,2) replace or 3) close that socket.

### Responsibilities

`Command_rcv` shall:

* block on socket receive
* parse incoming messages
* perform straightforward syntax validation
* normalize valid messages into internal command envelopes
* forward those envelopes into `status_queue`
* detect socket disconnects and socket-read failures
* forward such failures into `status_queue`
* must use os.unlink(path) before binding

## Command_rsp

`Command_rsp` is a helper thread in the main process.

It is write-only with respect to the Client socket.

### Responsibilities

`Command_rsp` shall:

* block on `command_rsp_queue`
* serialize Orchestrator-generated response messages
* write those messages to the Client socket
* report socket write failures to the Orchestrator through `status_queue`

## Reader, Worker, and Writer Workers

The subsystem workers are independent execution units used for rendering work.

Each subsystem has its own multiprocessing queue.

### Responsibilities

Workers shall:

* receive internal commands from their subsystem queue
* process internal envelopes and work packets
* perform their assigned rendering tasks
* report status, completion, and error events to the Orchestrator through `status_queue`


## Queue Design

## Inbound event bus: `status_queue`

`status_queue` is the Orchestrator’s inbound event bus.

All inbound events that require Orchestrator attention shall arrive through this queue.

Typical producers include:

* `Command_rcv`
* Reader
* Worker
* Writer
* `Command_rsp`, but only for failures such as socket write errors

The Orchestrator is the sole consumer of `status_queue`.

## Outbound subsystem queues

Each subsystem has its own queue:

* `read_queue`
* `render_queue`
* `write_queue`

These queues are used by the Orchestrator to send work and control messages to the corresponding subsystem workers.

## Outbound Client response queue

`command_rsp_queue` is the Orchestrator’s outbound response queue for the Client.

The Orchestrator is the producer.
`Command_rsp` is the consumer.

This queue is used only for outgoing Client responses.

## Message Flow

## Incoming command flow

1. The Client sends a socket message.
2. `Command_rcv` receives the message.
3. `Command_rcv` performs basic syntax validation.
4. `Command_rcv` wraps the message as an internal command envelope.
5. `Command_rcv` places that envelope on `status_queue`.
6. The Orchestrator receives the envelope from `status_queue`.
7. The Orchestrator decides how to handle the request.
8. The Orchestrator dispatches work to subsystem queues as needed.

## Outgoing response flow

1. The Orchestrator determines that an Client response should be sent.
2. The Orchestrator generates the response envelope.
3. The Orchestrator places the response on `command_rsp_queue`.
4. `Command_rsp` reads the response from `command_rsp_queue`.
5. `Command_rsp` serializes and writes the response to the socket.

If socket writing fails, `Command_rsp` reports that failure to the Orchestrator via `status_queue`.

## Example control flow

### Start render request

1. Client sends `start_render`
2. `Command_rcv` receives and syntax-checks the message
3. `Command_rcv` forwards normalized request to `status_queue`
4. Orchestrator accepts or rejects the request
5. If accepted, Orchestrator configures subsystems and begins dispatching work
6. Worker status and completion messages flow back through `status_queue`
7. Orchestrator determines request completion
8. Orchestrator generates `complete` or `error`
9. Orchestrator sends response to `command_rsp_queue`
10. `Command_rsp` writes response to the Client

## State Ownership

Overall daemon state shall be owned exclusively by the Orchestrator.

This includes:

* current daemon mode
* whether a render request is active
* the active job identifier
* current request parameters
* subsystem readiness
* completion, failure, and recovery state

Neither `Command_rcv`, `Command_rsp`, nor any worker process may mutate overall daemon state.

They may only emit events to the Orchestrator or perform directed work assigned by the Orchestrator.

## Socket Ownership Rules

Socket ownership is intentionally asymmetric.

### Command_rcv

`Command_rcv` is the sole owner of socket read-side lifecycle.

It alone may:

* read from the socket
* replace the socket
* close the socket

### Command_rsp

`Command_rsp` may only:

* write to the socket

It may not read, replace, or close the socket.

This split prevents ambiguous socket ownership and reduces race conditions during disconnect or reconnect handling.

## Validation Rules

## Syntax validation

The proxy threads only do minimal validation.

For inbound commands, `Command_rcv` may validate:

* JSON parseability
* required top-level keys
* presence of command name
* presence of job id
* presence of params object when required

For outbound responses, `Command_rsp` may validate only enough to serialize safely.

## Semantic validation

Semantic validation is owned by the Orchestrator.

Examples include:

* whether the daemon is currently able to accept a new request
* whether a command is valid in the current state
* whether a request conflicts with an active request
* whether parameters are acceptable for scheduling and execution

## Failure Handling

## Socket read failures

If `Command_rcv` encounters a read failure, disconnect, or invalid inbound message, it shall forward a normalized error
or disconnect event to `status_queue`.

The Orchestrator shall decide how to react.

## Socket write failures

If `Command_rsp` fails to write a response, it shall report the failure to `status_queue`.

The Orchestrator shall decide how to react.

## Worker failures

If Reader, Worker, or Writer fail, they shall report those failures to `status_queue`.

The Orchestrator may then generate an Client-facing `error` response through `command_rsp_queue`.

## Isolation Guarantees

The design intentionally isolates the major concerns:

### Orchestrator

Owns policy, state, and orchestration.

### Command_rcv and Command_rsp

Own only socket adaptation and basic syntax handling.

### Workers

Own only rendering-related execution.

This separation reduces coupling and makes the daemon easier to evolve.

## Benefits of This Design

This design provides several advantages:

* the Orchestrator remains queue-driven and does not need socket multiplexing logic
* socket handling is isolated into small, comprehensible proxy threads
* all overall state has a single owner
* subsystem workers remain independent of Client protocol concerns
* message flow is easier to debug because inbound and outbound paths are explicit
* queue boundaries make the system easier to extend with additional commands later

## Daemon Internal Message Categories

    JOB_REQUEST   # Client -> Orch: New Job
    JOB_DONE      # Orch -> Client: Job Done
    JOB_CANCEL    # Orch -> Writer: Cancel
    LOAD_BLOCK    # Orch -> Reader: Load Block
    BLOCK_LOADED  # Reader -> Orch: Block loaded
    RENDER_TILE   # Orch -> render: Render Tile
    WRITE_TILE    # Orch -> Writer: Write Tile
    TILE_WRITTEN  # Writer -> Orch: Tile Written
    TILES_FINALIZED  # Writer -> Orch:  Output Finalized
    WRITER_ABORTED   # Writer -> Cancel is complete
    TELEMETRY     # Statistics
    ERROR         # Any -> Orch: Error occurred
    SHUTDOWN      # Client -> Orch: Shutdown

## Client / Daemon JSON Socket Messages

_See client_messaging.md_
