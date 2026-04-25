from __future__ import annotations

import hashlib
import hmac
import os
import secrets

MAX_TOKEN_LEN = 256
NONCE_BYTES = 16


def get_shared_secret() -> str:
    """Return the pre-shared secret from the environment.

    Returns:
        Shared secret string.

    Raises:
        RuntimeError: If the environment variable is missing.
    """
    secret = os.environ.get("WEAVER_KEY")
    if not secret:
        raise RuntimeError("FATAL: 'WEAVER_KEY' environment variable not set.")
    return secret


def generate_challenge_nonce() -> str:
    """Generate a cryptographically strong per-connection nonce.

    Returns:
        Hex-encoded nonce string.
    """
    return secrets.token_hex(NONCE_BYTES)


def compute_auth_response(challenge: str) -> str:
    """Compute the expected HMAC-SHA256 response for a challenge.

    The client proves possession of the shared secret by returning the
    HMAC of the server-provided challenge.

    Args:
        challenge: One-time server nonce.

    Returns:
        Hex-encoded HMAC digest.
    """
    secret = get_shared_secret().encode("utf-8")
    challenge_bytes = challenge.encode("utf-8")
    return hmac.new(secret, challenge_bytes, hashlib.sha256).hexdigest()


def verify_challenge_response(challenge: str, client_response: str) -> bool:
    """Verify a client response against the expected HMAC.

    Args:
        challenge: One-time server nonce.
        client_response: Client-provided hex digest.

    Returns:
        True if valid, otherwise False.
    """
    if not challenge or not client_response:
        return False

    if not isinstance(client_response, str) or len(client_response) > MAX_TOKEN_LEN:
        return False

    expected = compute_auth_response(challenge)
    return hmac.compare_digest(expected, client_response)
