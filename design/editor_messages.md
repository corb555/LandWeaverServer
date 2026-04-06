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

The Daemon shall open the socket: /tmp/thematic_render.sock

## Message Model

All messages exchanged between the Client and the Daemon shall be small control messages. Raster image data shall not be
sent over the messaging channel.

The messaging channel shall be used for:

* render requests
* progress or state updates
* completion notifications
* error notifications
* optional cancel or shutdown commands

## Encoding and Framing

Messages shall be encoded as UTF-8 JSON.

Each message shall be framed as a single newline-terminated JSON object.

Each side shall treat one newline-terminated JSON object as one complete message.

The protocol shall not rely on partial-message parsing outside this framing rule.

All messages will include a message type (msg)

## Connection Model

The Client shall establish a client connection to the Daemon when the View tab needs to issue or receive
render-related messages.

The Client may keep the connection open across multiple requests.

The Daemon shall be able to accept a connection from the Client and remain available for repeated
request/response cycles.

If the connection is lost, the Client shall detect the disconnect and report that the Daemon is unavailable.

## Request–Response Pattern

The Client shall send a render request message when the user clicks **Build**.

The Daemon shall respond asynchronously. It is not required to complete the request before acknowledging receipt of
the message at the transport level.

## Request Identity

Each render request shall include a `job_id`.

The Daemon shall include the same `job_id` in every response associated with that request.

This requirement applies to:

* progress messages
* completion messages
* error messages
* any future cancellation acknowledgments

The `job_id` allows the Client to match responses to the correct request and safely ignore stale messages.

### Terminal Response

For each render request, the Daemon must eventually send exactly one terminal response:

* `complete`
* `error` with Severity other than SEV_WARNING  (2)
* `cancelled`, if cancellation is later supported

Any message for that `job_id` received after a Terminal Response should be ignored.

### Progress / Status

Optional intermediate progress or state messages may be sent before the terminal response.

## Message Types

### Render Request

The Client shall send a `start_render` command to request a render.

The request shall include a `params` object containing the operational settings needed for that render.

Required params entries:

* `percent`
* `row`
* `col`
* `config_path`
* `prefix`
* `build_dir`
* `output_file`

Example:

```json
{
  "msg": "job_request",
  "job_id": "21",
  "params": {
     "percent": 0.1,
     "row": 0.5,
     "col": 0.5,
     "config_path": "biome.yml",
     "prefix": "Yosemite",
     "build_dir": "xyz",
     "output_file": "yosemite_relief.tif"
  }
}
```

### Progress Message

The Daemon may periodically send progress status messages while work is in progress.

A progress message may include human-readable status text.
It must include a progress field with 0 - 100.00 progress,  including decimal precision.
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

The completion message shall include the output raster path.

Example:

```json
{
    "msg": "complete", "job_id":"34",
    "path": "xxx/zzz.tif"
}
```

### Error Message

If rendering fails, the Daemon shall send an `error` message.

The error message shall include a human-readable description and a severity.

Severity: 
SEV_FATAL = 0  # Fatal Daemon error.  Daemon shutting down. (This may not make it to client!)
SEV_CANCEL = 1  # Job Cancellation. Error text is sent to client.
SEV_WARNING = 2  # Job continues. Warning text is sent to the client

Example:

```json
{
    "msg": "error", "job_id": "41", "severity": 0,
    "message": "render pipeline Crash: XYZ"
}
```

## Client Requirements

The Client shall:

* connect to the Daemon over `QLocalSocket`
* send newline-delimited JSON requests
* parse newline-delimited JSON responses
* include a `job_id` in every render request
* disable or otherwise guard the Build action while a request is active, unless overlapping requests are intentionally
  supported
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
* accept newline-delimited JSON requests
* validate incoming messages before acting on them
* reject malformed or incomplete requests with an `error` response when possible
* include the originating `job_id` in every response associated with that request
* send exactly one terminal response for each accepted `start_render` request
* send the output TIFF path in the completion response
* send an error response if the render cannot be started or cannot complete
* remain running across multiple requests unless explicitly shut down

## Validation 

The Daemon performs some validation on the incoming `start_render` messages before starting work.

At minimum, it  validates:

* that `msg` is recognized
* that `job_id` is present
* that `params` is present
* that required parameter fields are present
* that parameter values are of usable type and range

If validation fails, the Daemon  returns an `error` response and does not start the render.

## Ordering Requirements

Responses for a single request shall be logically ordered:

* zero or more progress messages
* then one terminal message

## Failure Handling

If the Client cannot connect to the Daemon, it shall report the connection failure to the user.

If the Daemon disconnects unexpectedly while a request is active, the Client shall treat the request as failed.

If the Daemon receives malformed JSON, it may close the connection or send an error response. The preferred behavior is
to send an error response when the message boundary is intact and the request can still be identified.

If either side receives an unknown message type, it shall treat that as a protocol error.

## Timeout Behavior

The Client should apply a reasonable timeout policy for detecting a lost or stalled Daemon connection.

The timeout shall be long enough to allow valid renders to complete but short enough to detect a dead Daemon or broken
connection.

A timeout shall be treated as a failed request unless a progress policy explicitly resets the timeout window.

## Concurrency Policy

If the Client sends a new `start_render` request while another is still active, the Daemon will queue the request and
start it when the current request finishes.


## Security and Trust Boundary

The messaging channel is intended for local communication between trusted processes on the same machine.

No authentication or encryption is required for the initial implementation.

The Daemon shall still validate message structure and required fields to avoid unsafe behavior caused by malformed
input.
