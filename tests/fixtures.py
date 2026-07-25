from __future__ import annotations

import uuid

from sapient_msg.bsi_flex_335_v2_0 import (
    location_pb2,
    registration_pb2,
    status_report_pb2,
)
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import (
    Capability,
    ConfigurationData,
    Duration,
    LocationType,
    ModeDefinition,
    NodeDefinition,
    RegionDefinition,
    Registration,
    StatusDefinition,
    TaskDefinition,
)
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage
from sapient_msg_pydantic.bsi_flex_335_v2_0.status_report import StatusReport

_R = registration_pb2.Registration


def random_uuid() -> str:
    return str(uuid.uuid4())


def random_node_id() -> uuid.UUID:
    return uuid.uuid4()


def make_registration(status_interval_seconds: float = 5.0) -> Registration:
    return Registration(
        node_definition=[NodeDefinition(node_type=_R.NodeType.NODE_TYPE_CAMERA)],
        icd_version="BSI Flex 335 v2.0",
        capabilities=[Capability(category="camera", type="detection")],
        status_definition=StatusDefinition(
            status_interval=Duration(
                units=_R.TimeUnits.TIME_UNITS_SECONDS,
                value=status_interval_seconds,
            ),
        ),
        mode_definition=[
            ModeDefinition(
                mode_name="search",
                mode_type=_R.ModeType.MODE_TYPE_PERMANENT,
                settle_time=Duration(units=_R.TimeUnits.TIME_UNITS_SECONDS, value=1.0),
                task=TaskDefinition(
                    region_definition=RegionDefinition(
                        region_type=[_R.RegionType.REGION_TYPE_AREA_OF_INTEREST],
                        region_area=[
                            LocationType(
                                location_units=(
                                    location_pb2.LocationCoordinateSystem.LOCATION_COORDINATE_SYSTEM_LAT_LNG_DEG_M
                                ),
                                location_datum=(
                                    location_pb2.LocationDatum.LOCATION_DATUM_WGS84_E
                                ),
                            )
                        ],
                    ),
                ),
            )
        ],
        config_data=[ConfigurationData(manufacturer="Acme", model="Widget-1")],
    )


def make_status_report(
    system: status_report_pb2.StatusReport.System = (
        status_report_pb2.StatusReport.System.SYSTEM_OK
    ),
    info: status_report_pb2.StatusReport.Info = (
        status_report_pb2.StatusReport.Info.INFO_NEW
    ),
    mode: str = "idle",
) -> StatusReport:
    return StatusReport(system=system, info=info, mode=mode)


def make_sapient_message(
    node_id: str | None = None,
    destination_id: str | None = None,
    registration: Registration | None = None,
    status_report: StatusReport | None = None,
) -> SapientMessage:
    from datetime import UTC, datetime

    return SapientMessage(
        timestamp=datetime.now(UTC),
        node_id=node_id or random_uuid(),
        destination_id=destination_id or random_uuid(),
        registration=registration,
        status_report=status_report,
    )
