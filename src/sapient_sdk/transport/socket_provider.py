from __future__ import annotations

import abc
import asyncio
import contextlib
import os
import ssl
import tempfile
from collections.abc import Iterator
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from pydantic import BaseModel, ConfigDict

_PEM_MARKER = b"-----BEGIN"


class SocketProvider(BaseModel, abc.ABC):
    host: str
    port: int

    @abc.abstractmethod
    async def open(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]: ...


class PlainSocketProvider(SocketProvider):
    async def open(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_connection(self.host, self.port)


@contextlib.contextmanager
def _temp_pem_file(pem: bytes) -> Iterator[str]:
    # ssl.SSLContext.load_cert_chain() has no in-memory equivalent of
    # load_verify_locations(cadata=...) — it is file-path-only in CPython's
    # stdlib. This bridges that gap without ever asking the caller for a path:
    # write the content to an owner-only-permission temp file for the instant
    # OpenSSL needs to read it, then delete it immediately.
    fd, path = tempfile.mkstemp(suffix=".pem")
    try:
        try:
            os.chmod(path, 0o600)
        except BaseException:
            os.close(fd)
            raise
        with os.fdopen(fd, "wb") as f:
            f.write(pem)
        yield path
    finally:
        os.unlink(path)


def _is_pem(data: bytes) -> bool:
    return data.lstrip().startswith(_PEM_MARKER)


def resolve_pem_source(value: str | bytes | os.PathLike[str]) -> bytes:
    """Resolve certificate/key material given as either a path or content.

    ``bytes`` always mean the material itself. DER is binary and carries no
    ``-----BEGIN`` header, so treating unheadered bytes as a path would make
    in-memory DER impossible to pass at all.

    A ``str`` starting with ``-----BEGIN`` is inline PEM text; any other
    ``str``, and every ``os.PathLike``, is read from disk. Falling back to
    "path" (rather than "content") means a mistyped path fails as a plain
    ``FileNotFoundError`` instead of a confusing certificate parse error.
    """
    if isinstance(value, bytes):
        return value
    if isinstance(value, str) and value.lstrip().startswith(
        _PEM_MARKER.decode("ascii")
    ):
        return value.encode("utf-8")
    return Path(value).read_bytes()


def _ensure_pem_cert(data: bytes, label: str) -> bytes:
    if _is_pem(data):
        return data
    try:
        cert = x509.load_der_x509_certificate(data)
    except Exception as exc:
        raise ValueError(f"{label}: not valid PEM or DER certificate data") from exc
    return cert.public_bytes(serialization.Encoding.PEM)


def _ensure_pem_key(data: bytes, label: str) -> bytes:
    if _is_pem(data):
        return data
    try:
        key = serialization.load_der_private_key(data, password=None)
    except Exception as exc:
        raise ValueError(f"{label}: not valid PEM or DER private key data") from exc
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def build_client_ssl_context(
    ca_cert: str | bytes | os.PathLike[str],
    client_cert: str | bytes | os.PathLike[str] | None = None,
    client_key: str | bytes | os.PathLike[str] | None = None,
) -> ssl.SSLContext:
    """Build a client SSLContext trusting ``ca_cert``, optionally with mTLS.

    Each argument accepts a filesystem path, inline PEM, or DER bytes — see
    :func:`resolve_pem_source` for how the three are told apart.
    """
    if client_key is not None and client_cert is None:
        raise ValueError("client_key was given without client_cert")

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED

    ca_data = resolve_pem_source(ca_cert)
    # cadata takes PEM as str and DER as bytes, so the CA needs no conversion.
    context.load_verify_locations(
        cadata=ca_data.decode("utf-8") if _is_pem(ca_data) else ca_data
    )

    if client_cert is not None:
        cert_pem = _ensure_pem_cert(resolve_pem_source(client_cert), "client_cert")
        key_pem = (
            _ensure_pem_key(resolve_pem_source(client_key), "client_key")
            if client_key is not None
            else None
        )
        with contextlib.ExitStack() as stack:
            certfile = stack.enter_context(_temp_pem_file(cert_pem))
            # keyfile=None tells OpenSSL to look for the private key inside
            # certfile itself — valid when the caller's PEM already combines
            # cert and key, so an empty placeholder file must never be used.
            keyfile = (
                stack.enter_context(_temp_pem_file(key_pem))
                if key_pem is not None
                else None
            )
            context.load_cert_chain(certfile, keyfile)
    return context


class TlsSocketProvider(SocketProvider):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    ssl_context: ssl.SSLContext

    async def open(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_connection(self.host, self.port, ssl=self.ssl_context)
