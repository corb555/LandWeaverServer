# Client–Daemon Messaging Requirements

## Purpose

This document defines the messaging requirements between the Client and the Raster Builder Daemon.

## Goal

The messaging system shall provide a simple, low-latency, local mechanism for the Client to request raster renders from
the
Daemon and receive status, completion, and error responses.

The messaging design shall support: 1) an interactive workflow in which the user edits settings, saves them, requests a
render, and sees the resulting raster displayed with minimal delay. 2) alternatively a CLI can also request a render.

## Scope

This interface covers:

* connection establishment between the Client and the Daemon
* render request messages from the Client
* status and completion messages from the Daemon
* error reporting
* message framing and format
* request correlation
* connection and protocol behavior during normal and failure conditions

## Transport

Messaging shall use a local interprocess communication channel based on `QLocalSocket` and `QLocalServer` on
the Client and socket.AF_UNIX on the Daemon.

The Daemon shall act as the server.
The Client shall act as the client.

The transport shall be local-machine only.

The Daemon shall open the socket: as specified

## Message Model

All messages exchanged between the Client and the Daemon shall be small control messages. Raster image data shall not be
sent over the messaging channel.

The messaging channel shall be used for:

Client messages:
* render request
* cancel job

Server messages:
* progress or state update
* completion notification
* error notification

## Encoding and Framing

Messages shall be encoded as UTF-8 JSON.

Each message shall be framed as NDJSON (a single newline-terminated JSON object).

Each side shall treat one newline-terminated JSON object as one complete message.

The protocol shall not rely on partial-message parsing outside this framing rule.

All messages will include a message type (msg)

## Connection Model

The Client shall establish a client connection to the Daemon when it needs to issue or receive
render-related messages.

The Client must close the connection after each request.

The Daemon shall be able to accept a connection from the Client and remain available for repeated
request/response cycles.

If the connection is lost, the Client shall detect the disconnect and report that the Daemon is unavailable.

## Request–Response Pattern

The Client shall send a render request message when the user requests a render.

The Daemon shall respond asynchronously. 

### _Terminal Response_

For each render request, the Daemon must eventually send exactly one _Terminal Response_:

* `complete`
* `error` with Severity other than SEV_WARNING  (2)
* `cancelled`

### Progress / Status

Optional intermediate progress or state messages may be sent before the _Terminal Response_.

## Message Types

> ALL PATHS ARE RELATIVE TO THE SERVER WORKING DIR. ABSOLUTE PATHS ARE REJECTED
> The server will reject any client message other than: `render_request`  

### Render Request

The Client shall send a `render_request` command to request a render.

The request shall include a `params` object containing the operational settings needed for that render.

Required params entries:

* `output_suffix`
* `prefix`
* `percent`
* `row`
* `col`


Example:

```json
{
  "msg": "render_request",
  "job_id": 12,
  "params": {
    "percent": 0.2,
    "row": 0.1,
    "col": 0.9,
    "prefix": "Sedona",
    "output_suffix": "_biome"
  }
}
```

### Progress Message

The Daemon _may_ periodically send progress status messages while work is in progress.

A progress message _may_ include human-readable status text.
It _must_ include the `progress` field with 0 - 100.00 progress,  including decimal precision.
The client does not need to process progress messages.

Example:

```json
{
  "msg": "progress",
  "job_id": "1",
  "progress": 85.64,
  "message": "Rendering sample window"
}
```

### Completion Message

When rendering succeeds, the Daemon shall send a `complete` message.

The completion message shall include the relative output raster path.

Example:

```json
{
    "msg": "complete", 
    "job_id":"34",
    "path": "xxx/zzz.tif"
}
```

### Error Message

The Daemon shall send an `error` message for errors during the render.

The error message shall include a human-readable description and a severity.

Severity: 
SEV_FATAL = 0  # Fatal Daemon error.  Daemon shutting down. (This may not make it to client!)
SEV_CANCEL = 1  # Job Cancellation. Error text is sent to client.
SEV_WARNING = 2  # Job continues. Warning text is sent to the client

Example:

```json
{
    "msg": "error", 
    "job_id": "41", 
    "severity": 0,
    "message": "render pipeline Crash: XYZ"
}
```

#### Server Queue
When a request is sent the Daemon _may_ respond with a Warning indicating the request is being queued rather being
immediately started.

Example:

```json
{
    "msg": "error", "job_id": "41", "severity": 2,
    "message": "Request queued"
}
```

## Client Requirements

The Client shall:

* connect to the Daemon over `QLocalSocket`
* send NDJSON requests
* parse NDJSON responses
* include a `job_id` in every render request
* treat `complete` and `error` as terminal states for the active request
* ignore messages whose `job_id` does not match the active request
* display an error if the Daemon cannot be reached
* display an error if a malformed or unknown message is received
* use the returned `path` from a `complete` message to load and display the TIFF

The Client shall not assume that a response will arrive immediately.
The Client shall remain responsive while waiting for Daemon messages.

## Daemon Requirements

The Daemon shall:

* listen for Client connections using `socket.AF_UNIX` on the above specified socket name.
* accept NDJSON requests
* validate incoming messages before acting on them (see Validation)
* reject malformed  requests with an `error` response where specified
* include the originating `job_id` in every response associated with that request
* send exactly one terminal response for each accepted `start_render` request
* send the output  path in the completion response
* send an error response if the render cannot be started or cannot complete
* remain running across multiple requests unless explicitly shut down

## Message Validation 

The Daemon validates each incoming Client message immediately upon receipt.

A message is accepted only if all of the following are true:

the message length is no greater than MAX_MSG_BYTES (1000 bytes)
the message parses as valid JSON using json.loads(line)
the parsed JSON value is a dict
the msg field is exactly "render_request"
job_id is a digit-only string with a maximum length of 11 characters
the message passes Cerberus schema validation:
all required fields are present
no unexpected fields are present
all values are of the expected type and within allowed ranges
region matches the allowed pattern: alphanumeric plus '_'

If validation fails, the message is dropped.

No error response is sent for malformed or invalid protocol messages.
An error response is sent only when the message is structurally valid but one or more user parameters are invalid, such as region, percent, row, or col.

```json
{
  "msg": "render_request",
  "job_id": "12",
  "params": {
    "percent": 0.2,
    "row": 0.1,
    "col": 0.9,
    "region": "Sedona"
  }
}
```

## Ordering Requirements

Responses for a single request shall be logically ordered:

* zero or more progress messages
* then one terminal message

## Failure Handling

If the Client cannot connect to the Daemon, it shall report the connection failure to the user.

If the Daemon disconnects unexpectedly while a request is active, the Client shall treat the request as failed.

If the Daemon receives malformed JSON, it may close the connection or send an error response. 

If either side receives an unknown message type, it shall treat that as a protocol error.

## Timeout Behavior

The Client should apply a reasonable timeout policy for detecting a lost or stalled Daemon connection.

The timeout shall be long enough to allow valid renders to complete but short enough to detect a dead Daemon or broken
connection.

A timeout shall be treated as a failed request unless a progress policy explicitly resets the timeout window.

## Concurrency Policy

If the Client sends a new `start_render` request while another is still active, the Daemon will queue the request and
start it when the current request finishes.  The Daemon may respond with a Warning message that the request is queued.


## Security and Trust Boundary

The messaging channel is intended for local communication between trusted processes on the same machine.

No authentication or encryption is required for the initial implementation.

The Daemon shall still validate message structure and required fields to avoid unsafe behavior caused by malformed
input.
