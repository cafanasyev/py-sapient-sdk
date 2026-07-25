from __future__ import annotations

from pathlib import Path

import pytest
import trustme

from sapient_sdk.transport.socket_provider import resolve_pem_source


def test_inline_pem_str_is_returned_as_bytes() -> None:
    pem = trustme.CA().cert_pem.bytes().decode("ascii")
    assert resolve_pem_source(pem) == pem.encode("ascii")


def test_inline_pem_bytes_are_returned_unchanged() -> None:
    pem = trustme.CA().cert_pem.bytes()
    assert resolve_pem_source(pem) == pem


def test_str_path_is_read_from_disk(tmp_path: Path) -> None:
    pem = trustme.CA().cert_pem.bytes()
    target = tmp_path / "ca.pem"
    target.write_bytes(pem)
    assert resolve_pem_source(str(target)) == pem


def test_path_object_is_read_from_disk(tmp_path: Path) -> None:
    pem = trustme.CA().cert_pem.bytes()
    target = tmp_path / "ca.pem"
    target.write_bytes(pem)
    assert resolve_pem_source(target) == pem


def test_der_bytes_are_treated_as_content_not_a_path() -> None:
    # DER is binary and has no -----BEGIN header. Bytes must always mean
    # "this is the certificate itself", never "this is a path" -- otherwise
    # passing DER in memory tries to open a file named after binary garbage.
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization

    pem = trustme.CA().cert_pem.bytes()
    der = x509.load_pem_x509_certificate(pem).public_bytes(serialization.Encoding.DER)

    assert resolve_pem_source(der) == der


def test_der_file_path_is_read_from_disk(tmp_path: Path) -> None:
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization

    pem = trustme.CA().cert_pem.bytes()
    der = x509.load_pem_x509_certificate(pem).public_bytes(serialization.Encoding.DER)
    target = tmp_path / "ca.der"
    target.write_bytes(der)

    assert resolve_pem_source(str(target)) == der


def test_missing_path_raises_file_not_found(tmp_path: Path) -> None:
    # A typo'd path must fail as a plain missing-file error rather than being
    # silently reinterpreted as certificate content.
    with pytest.raises(FileNotFoundError):
        resolve_pem_source(str(tmp_path / "nope.pem"))
