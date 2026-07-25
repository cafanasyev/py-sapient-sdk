from __future__ import annotations

from tests.fixtures import make_registration, make_sapient_message, make_status_report


def test_make_registration_serializes() -> None:
    reg = make_registration()
    pb2 = reg.to_pb2()
    assert pb2.icd_version == "BSI Flex 335 v2.0"


def test_make_status_report_serializes() -> None:
    sr = make_status_report()
    pb2 = sr.to_pb2()
    assert pb2.report_id != ""


def test_make_sapient_message_registration_round_trips() -> None:
    msg = make_sapient_message(registration=make_registration())
    pb2 = msg.to_pb2()
    data = pb2.SerializeToString()
    assert len(data) > 0
