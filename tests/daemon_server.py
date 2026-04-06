from __future__ import annotations

import json
from pathlib import Path
import signal
import socket
import sys
from typing import Any

SOCKET_PATH = "thematic_render.sock"
ENCODING = "utf-8"
BACKLOG = 5
BUFFER_SIZE = 4096


class DaemonEmulator:
    """Emulate the Raster Builder Daemon over a Unix domain socket."""

    def __init__(self, socket_path: str) -> None:
        """Initialize the emulator.

        Args:
            socket_path: Filesystem path for the Unix domain socket.
        """
        self.socket_path = Path(socket_path)
        self.server_socket: socket.socket | None = None
        self._running = True
        self._request_count = 0

    def run(self) -> None:
        """Start the emulator event loop."""
        self._cleanup_stale_socket()
        self.server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server_socket.bind(str(self.socket_path))
        self.server_socket.listen(BACKLOG)

        print(f"✅ Daemon emulator listening on: {self.socket_path}")

        try:
            while self._running:
                conn, _addr = self.server_socket.accept()
                print("➡️ Client connected")
                with conn:
                    self._handle_connection(conn)
                print("✅ Client disconnected")
        except KeyboardInterrupt:
            print("\n⚠️ Interrupted by user")
        finally:
            self.close()

    def close(self) -> None:
        """Close server resources and remove the socket file."""
        self._running = False

        if self.server_socket is not None:
            try:
                self.server_socket.close()
            except OSError as exc:
                print(f"⚠️ Error closing server socket: {exc}")
            self.server_socket = None

        self._cleanup_stale_socket()
        print("✅ Daemon emulator stopped")

    def _cleanup_stale_socket(self) -> None:
        """Remove any stale socket file."""
        try:
            if self.socket_path.exists():
                self.socket_path.unlink()
        except OSError as exc:
            print(f"❌ Failed to remove stale socket {self.socket_path}: {exc}")
            raise

    def _handle_connection(self, conn: socket.socket) -> None:
        """Read newline-delimited JSON messages from one client.

        Args:
            conn: Connected client socket.
        """
        buffer = ""

        while self._running:
            try:
                data = conn.recv(BUFFER_SIZE)
            except OSError as exc:
                print(f"⚠️ Receive error: {exc}")
                break

            if not data:
                break

            buffer += data.decode(ENCODING)

            while "\n" in buffer:
                raw_line, buffer = buffer.split("\n", 1)
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                self._handle_message(raw_line, conn)

    def _handle_message(self, raw_line: str, conn: socket.socket) -> None:
        """Parse, print, and respond to one message.

        Args:
            raw_line: One newline-delimited JSON message.
            conn: Connected client socket.
        """
        print(f"🟣 Received raw: {raw_line}")

        try:
            msg = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            print(f"❌ Invalid JSON: {exc}")
            self._send_json(
                conn, {
                    "msg": "error", "job_id": "unknown", "message": f"Invalid JSON: {exc}",
                }, )
            return

        print("🔵 Parsed message:")
        print(json.dumps(msg, indent=2))

        msg_type = msg.get("msg")
        job_id = str(msg.get("job_id", "unknown"))

        if msg_type != "start_render":
            self._send_json(
                conn, {
                    "msg": "error", "job_id": job_id,
                    "message": f"Unsupported msg type: {msg_type}",
                }, )
            return

        self._request_count += 1
        response = (self._build_complete_response(
            job_id
        ) if self._request_count % 2 == 1 else self._build_error_response(
            job_id
        ))
        self._send_json(conn, response)

    @staticmethod
    def _build_complete_response(job_id: str) -> dict[str, Any]:
        """Build a successful completion response.

        Args:
            job_id: Request identifier from the client.

        Returns:
            Completion message dictionary.
        """
        return {
            "msg": "complete", "job_id": job_id, "path": "build/Rainier/Rainier_biome.tif",
        }

    @staticmethod
    def _build_error_response(job_id: str) -> dict[str, Any]:
        """Build an error response.

        Args:
            job_id: Request identifier from the client.

        Returns:
            Error message dictionary.
        """
        return {
            "msg": "error", "job_id": job_id, "message": "render failed",
        }

    @staticmethod
    def _send_json(conn: socket.socket, payload: dict[str, Any]) -> None:
        """Send one newline-delimited JSON response.

        Args:
            conn: Connected client socket.
            payload: Response payload.
        """
        message = json.dumps(payload) + "\n"
        print("➡️ Sending response:")
        print(json.dumps(payload, indent=2))
        conn.sendall(message.encode(ENCODING))


def main() -> int:
    """Run the daemon emulator.

    Returns:
        Process exit code.
    """
    emulator = DaemonEmulator(SOCKET_PATH)

    def _handle_signal(_signum: int, _frame: Any) -> None:
        """Handle termination signals."""
        print("\n⚠️ Shutdown signal received")
        emulator.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        emulator.run()
        return 0
    except Exception as exc:
        print(f"❌ Fatal error: {exc}")
        emulator.close()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
