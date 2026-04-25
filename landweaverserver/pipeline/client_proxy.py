import json
import multiprocessing as mp
import os
from queue import Empty
import socket
import threading
from typing import Optional

from landweaverserver.common.ipc_packets import Envelope, Op

MAX_TOKEN_LEN = 1024


class ClientProxy:
    """
    Handles NDJSON messaging between the Daemon and Client
    Has a receive thread and a send thread
    All messaaging to Daemon goes via mp queue
    Ensures client messages are valid NDJSON, reasonable length and pass the specified schema
    """

    ACCEPT_TIMEOUT_S = 1.0
    RESPONSE_TIMEOUT_S = 1.0

    def __init__(
            self, socket_path: str, status_q: "mp.Queue", response_q: "mp.Queue", request_schema
    ) -> None:
        self.socket_path = socket_path
        self.status_q = status_q
        self.response_q = response_q
        self.request_schema = request_schema

        self.running = False
        self._threads: list[threading.Thread] = []
        self._server_socket: Optional[socket.socket] = None
        self._active_connection: Optional[socket.socket] = None
        self._conn_lock = threading.Lock()

        if response_q is None:
            raise ValueError("none response_queue")

    def start(self) -> None:
        """Initialize the Unix socket and start communication threads."""
        print(f"➡️ [ClientProxy] Opening socket at {self.socket_path}")

        try:
            if os.path.exists(self.socket_path):
                os.remove(self.socket_path)
        except OSError as exc:
            raise RuntimeError(
                f"Failed to remove existing socket file: {self.socket_path}"
            ) from exc

        self.running = True

        rcv_thread = threading.Thread(
            target=self._rcv_loop, name="Socket_Rcv", daemon=True, )
        rsp_thread = threading.Thread(
            target=self._rsp_loop, name="Socket_Rsp", daemon=True, )

        rcv_thread.start()
        rsp_thread.start()
        self._threads = [rcv_thread, rsp_thread]

    def _set_active_connection(self, conn: Optional[socket.socket]) -> None:
        """Replace the current active connection safely."""
        with self._conn_lock:
            old_conn = self._active_connection
            self._active_connection = conn

        if old_conn is not None and old_conn is not conn:
            try:
                old_conn.close()
            except OSError:
                pass

    def _get_active_connection(self) -> Optional[socket.socket]:
        """Return the current active connection safely."""
        with self._conn_lock:
            return self._active_connection

    def _clear_active_connection(self) -> None:
        """Clear and close the current active connection safely."""
        self._set_active_connection(None)

    def _rcv_loop(self) -> None:
        """Listen for NDJSON commands from clients
        """
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server_socket = server

        ACCEPT_TIMEOUT = 5.0
        MAX_MSG_LEN = 1024

        try:
            server.bind(self.socket_path)
            server.listen(1)
            server.settimeout(ACCEPT_TIMEOUT)

            while self.running:
                try:
                    conn, _ = server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break

                self._set_active_connection(conn)

                try:
                    with conn, conn.makefile("r", encoding="utf-8", buffering=1) as stream:
                        while self.running:
                            line = stream.readline(MAX_MSG_LEN)
                            if not line:
                                break

                            if not line.endswith("\n"):
                                print(
                                    f"❌ [ClientProxy] Rejected oversized message "
                                    f"(>{MAX_MSG_LEN} chars)."
                                )
                                break
                            try:
                                data = json.loads(line)
                                self._handle_incoming_line(data)
                            except Exception as exc:
                                print(
                                    f"❌ [ClientProxy] Malformed JSON. Disconnecting client. {exc}"
                                    )
                                break

                except (OSError, UnicodeDecodeError) as exc:
                    print(f"⚠️ [CommandProxy] Connection error: {exc}")
                finally:
                    self._clear_active_connection()

        finally:
            try:
                server.close()
            except OSError:
                pass

    def _handle_incoming_line(self, data: dict) -> None:
        """Parse and validate one inbound NDJSON line."""
        # ---  LOGICAL VALIDATION (Cerberus) ---
        from cerberus import Validator
        job_id = data.get("job_id", "")
        v = Validator(self.request_schema)

        if not v.validate(data):
            # Message is structurally valid but parameters are wrong.
            # We respond with an error so the Client can tell the user why.
            error_details = v.errors
            print(f"⚠️ [CommandProxy] Parameter validation failed for '{job_id}' : {error_details}")

            self.response_q.put(
                {
                    "msg": "error", "job_id": job_id, "severity": 1,  # SEV_CANCEL
                    "message": f"Invalid render parameters: {error_details}"
                }
            )
            return

        # --- SUCCESS: DISPATCH TO ORCHESTRATOR ---
        print(
            f" [CommandProxy] Accepted Render Request: Job '{job_id}' ({data['params']['prefix']})"
        )
        self.status_q.put(Envelope(op=Op.JOB_REQUEST, payload=data))

    def _rsp_loop(self) -> None:
        """Send NDJSON responses with write timeouts."""
        while self.running:
            try:
                #  Wait for data from the Orchestrator
                payload = self.response_q.get(timeout=1.0)
                if payload is None: break  # Poison pill
            except Empty:
                continue

            conn = self._get_active_connection()
            if conn is None: continue

            try:
                # ensure the underlying socket won't block forever
                conn.settimeout(2.0)

                msg = json.dumps(payload) + "\n"
                conn.sendall(msg.encode("utf-8"))

            except (socket.timeout, BrokenPipeError, OSError) as exc:
                # If the client is gone or their buffer is jammed, drop connection
                self._clear_active_connection()
                print(f"❌ [CommandProxy] Send failed (Timeout or Reset): {exc}")

    def _queue_protocol_error(self, message: str) -> None:
        """Queue a protocol-level error response for the client."""
        self.response_q.put({"msg": "error", "message": message})

    def stop(self) -> None:
        """Stop threads and clean up socket resources."""
        self.running = False
        self._clear_active_connection()

        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError:
                pass

        for thread in self._threads:
            thread.join(timeout=2.0)

        if os.path.exists(self.socket_path):
            try:
                os.remove(self.socket_path)
            except OSError:
                pass
