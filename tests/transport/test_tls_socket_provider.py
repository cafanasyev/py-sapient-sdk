from __future__ import annotations

import asyncio
import ssl
from pathlib import Path

import pytest
import trustme
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from sapient_sdk.transport.socket_provider import (
    TlsSocketProvider,
    build_client_ssl_context,
)


def _cert_to_der(pem: bytes) -> bytes:
    return x509.load_pem_x509_certificate(pem).public_bytes(serialization.Encoding.DER)


def _key_to_der(pem: bytes) -> bytes:
    key = serialization.load_pem_private_key(pem, password=None)
    return key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def test_build_client_ssl_context_trusts_the_given_ca() -> None:
    ca = trustme.CA()
    context = build_client_ssl_context(ca_cert=ca.cert_pem.bytes())
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True


def test_build_client_ssl_context_loads_a_client_cert_from_memory() -> None:
    ca = trustme.CA()
    client_cert = ca.issue_cert("client.example.org")
    context = build_client_ssl_context(
        ca_cert=ca.cert_pem.bytes(),
        client_cert=client_cert.cert_chain_pems[0].bytes(),
        client_key=client_cert.private_key_pem.bytes(),
    )
    # no exception means the chain loaded
    assert context.verify_mode == ssl.CERT_REQUIRED


def test_build_client_ssl_context_rejects_key_without_cert() -> None:
    ca = trustme.CA()
    client_cert = ca.issue_cert("client.example.org")
    with pytest.raises(ValueError, match="client_key"):
        build_client_ssl_context(
            ca_cert=ca.cert_pem.bytes(),
            client_key=client_cert.private_key_pem.bytes(),
        )


def test_build_client_ssl_context_accepts_file_paths(tmp_path: Path) -> None:
    ca = trustme.CA()
    client_cert = ca.issue_cert("client.example.org")

    ca_file = tmp_path / "ca.pem"
    ca_file.write_bytes(ca.cert_pem.bytes())
    cert_file = tmp_path / "client.pem"
    cert_file.write_bytes(client_cert.cert_chain_pems[0].bytes())
    key_file = tmp_path / "client.key"
    key_file.write_bytes(client_cert.private_key_pem.bytes())

    context = build_client_ssl_context(
        ca_cert=str(ca_file),
        client_cert=str(cert_file),
        client_key=str(key_file),
    )
    assert context.verify_mode == ssl.CERT_REQUIRED


def test_build_client_ssl_context_accepts_der_ca_in_memory() -> None:
    ca = trustme.CA()
    context = build_client_ssl_context(ca_cert=_cert_to_der(ca.cert_pem.bytes()))
    assert context.verify_mode == ssl.CERT_REQUIRED


def test_build_client_ssl_context_accepts_der_ca_from_file(tmp_path: Path) -> None:
    ca = trustme.CA()
    ca_file = tmp_path / "ca.der"
    ca_file.write_bytes(_cert_to_der(ca.cert_pem.bytes()))

    context = build_client_ssl_context(ca_cert=str(ca_file))
    assert context.verify_mode == ssl.CERT_REQUIRED


def test_build_client_ssl_context_accepts_der_client_cert_and_key() -> None:
    ca = trustme.CA()
    client_cert = ca.issue_cert("client.example.org")

    context = build_client_ssl_context(
        ca_cert=_cert_to_der(ca.cert_pem.bytes()),
        client_cert=_cert_to_der(client_cert.cert_chain_pems[0].bytes()),
        client_key=_key_to_der(client_cert.private_key_pem.bytes()),
    )
    assert context.verify_mode == ssl.CERT_REQUIRED


async def test_tls_provider_connects_to_a_tls_server() -> None:
    ca = trustme.CA()
    server_cert = ca.issue_cert("127.0.0.1")

    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_cert.configure_cert(server_ctx)

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        writer.write(b"secure")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=server_ctx)
    host, port = server.sockets[0].getsockname()[:2]

    client_ctx = build_client_ssl_context(ca_cert=ca.cert_pem.bytes())
    provider = TlsSocketProvider(host=host, port=port, ssl_context=client_ctx)
    reader, writer = await provider.open()
    data = await reader.read(6)
    assert data == b"secure"

    writer.close()
    server.close()
    await server.wait_closed()


async def test_der_client_credentials_are_presented_in_a_real_mtls_handshake() -> None:
    # The strongest check that DER->PEM conversion yields genuinely usable
    # credentials: assert server-side that the client actually presented the
    # expected certificate. Merely connecting proves nothing here -- under
    # TLS 1.3 the client's connect succeeds before the server validates its
    # cert, so a client with no cert at all would still read the greeting.
    ca = trustme.CA()
    server_cert = ca.issue_cert("127.0.0.1")
    client_cert = ca.issue_cert("client.example.org")

    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_cert.configure_cert(server_ctx)
    ca.configure_trust(server_ctx)
    server_ctx.verify_mode = ssl.CERT_REQUIRED

    peer_certs: list[object] = []

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer_certs.append(writer.get_extra_info("ssl_object").getpeercert())
        writer.write(b"mtls")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=server_ctx)
    host, port = server.sockets[0].getsockname()[:2]

    client_ctx = build_client_ssl_context(
        ca_cert=_cert_to_der(ca.cert_pem.bytes()),
        client_cert=_cert_to_der(client_cert.cert_chain_pems[0].bytes()),
        client_key=_key_to_der(client_cert.private_key_pem.bytes()),
    )
    provider = TlsSocketProvider(host=host, port=port, ssl_context=client_ctx)
    reader, writer = await provider.open()
    assert await reader.read(4) == b"mtls"
    await asyncio.sleep(0.05)  # let the server finish its handler

    assert peer_certs, "server never completed the handshake"
    peer = peer_certs[0]
    assert isinstance(peer, dict), "client presented no certificate"
    assert ("DNS", "client.example.org") in peer["subjectAltName"]

    writer.close()
    server.close()
    await server.wait_closed()
