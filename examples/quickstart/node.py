"""The Node implementation this quickstart registers with NodeDispatcher."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import timedelta

from pydantic import Field
from sapient_msg.bsi_flex_335_v2_0 import (
    location_pb2,
    range_bearing_pb2,
    registration_pb2,
    status_report_pb2,
)
from sapient_msg_pydantic.bsi_flex_335_v2_0.alert_ack import AlertAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.detection_report import DetectionReport
from sapient_msg_pydantic.bsi_flex_335_v2_0.error import Error
from sapient_msg_pydantic.bsi_flex_335_v2_0.location import Location
from sapient_msg_pydantic.bsi_flex_335_v2_0.range_bearing import (
    LocationOrRangeBearing,
    RangeBearing,
    RangeBearingCone,
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
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.status_report import StatusReport
from sapient_msg_pydantic.bsi_flex_335_v2_0.task import Task
from ulid import ULID

from sapient_sdk.transmission.dispatcher import NodeDispatcher
from sapient_sdk.transmission.node import Node

logger = logging.getLogger("sapient_quickstart")

_R = registration_pb2.Registration

# How often this node sends a Status Report to stay registered. The SDK
# jitters around this automatically -- you don't need to implement that.
STATUS_INTERVAL = timedelta(seconds=10)

# How often send_detection_reports_periodically() publishes a DetectionReport.
# Unlike Status Reports, Detection Reports are never sent automatically by
# NodeDispatcher -- an application decides when it has something to report.
DETECTION_INTERVAL = timedelta(seconds=10)


class QuickstartNode(Node):
    """A minimal-but-real Node: reports itself online immediately, sends a
    static Registration/StatusReport, and logs everything the server sends
    back. get_registration()/get_status_report() below return the smallest
    payload the ICD will accept -- replace their contents with your sensor's
    real capabilities and state, not the wiring around them.

    Holds its own `dispatcher` reference so it can publish Detection Reports
    on its own schedule (see send_detection_reports_periodically()) -- unlike
    Registration/Status Reports, NodeDispatcher never sends those for you.
    """

    node_id_: uuid.UUID = Field(default_factory=uuid.uuid4)
    dispatcher: NodeDispatcher

    @property
    def node_id(self) -> uuid.UUID:
        return self.node_id_

    async def is_online(self) -> bool:
        return True

    async def get_registration(self) -> Registration:
        return Registration(
            node_definition=[NodeDefinition(node_type=_R.NodeType.NODE_TYPE_CAMERA)],
            icd_version="BSI Flex 335 v2.0",
            capabilities=[Capability(category="camera", type="detection")],
            status_definition=StatusDefinition(
                status_interval=Duration(
                    units=_R.TimeUnits.TIME_UNITS_SECONDS,
                    value=STATUS_INTERVAL.total_seconds(),
                ),
            ),
            mode_definition=[
                ModeDefinition(
                    mode_name="Default",
                    mode_type=_R.ModeType.MODE_TYPE_DEFAULT,
                    settle_time=Duration(
                        units=_R.TimeUnits.TIME_UNITS_SECONDS, value=1.0
                    ),
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
            config_data=[
                ConfigurationData(manufacturer="Your Company", model="Your Sensor")
            ],
        )

    async def get_status_report(self) -> StatusReport:
        return StatusReport(
            system=status_report_pb2.StatusReport.System.SYSTEM_OK,
            mode="Default",
            # Placeholder coordinates -- replace with the sensor's real position.
            node_location=Location(
                x=32.765288,
                y=49.243465,
                coordinate_system=(
                    location_pb2.LocationCoordinateSystem.LOCATION_COORDINATE_SYSTEM_LAT_LNG_DEG_M
                ),
                datum=location_pb2.LocationDatum.LOCATION_DATUM_WGS84_E,
            ),
            # A full 360-degree horizontal field of view -- narrow this to
            # the sensor's real coverage (and set `range` to its real
            # detection range instead of this placeholder).
            field_of_view=LocationOrRangeBearing(
                range_bearing=RangeBearingCone(
                    azimuth=0.0,
                    range=1000.0,
                    horizontal_extent=360.0,
                    coordinate_system=(
                        range_bearing_pb2.RangeBearingCoordinateSystem.RANGE_BEARING_COORDINATE_SYSTEM_DEGREES_M
                    ),
                    datum=range_bearing_pb2.RangeBearingDatum.RANGE_BEARING_DATUM_TRUE,
                )
            ),
        )

    async def send_detection_reports_periodically(self) -> None:
        """Runs forever, publishing a DetectionReport every DETECTION_INTERVAL
        while online. Start this as its own task after registering the node,
        and cancel that task on shutdown -- NodeDispatcher does not manage
        this loop for you the way it does registration/status reports."""
        while True:
            await asyncio.sleep(DETECTION_INTERVAL.total_seconds())
            if not await self.is_online():
                continue

            # A single placeholder detection at a fixed bearing -- replace
            # with whatever your sensor actually detected.
            detection = DetectionReport(
                object_id=str(ULID()),
                range_bearing=RangeBearing(
                    azimuth=0.0,
                    range=50.0,
                    coordinate_system=(
                        range_bearing_pb2.RangeBearingCoordinateSystem.RANGE_BEARING_COORDINATE_SYSTEM_DEGREES_M
                    ),
                    datum=range_bearing_pb2.RangeBearingDatum.RANGE_BEARING_DATUM_TRUE,
                ),
            )
            try:
                await self.dispatcher.publish(
                    detection, self.node_id, timedelta(seconds=5)
                )
            except TimeoutError:
                logger.warning("detection report publish timeout")

    async def on_registration_ack(self, ack: RegistrationAck) -> None:
        logger.info("registration acknowledged: accepted=%s", ack.acceptance)

    async def on_alert_ack(self, ack: AlertAck) -> None:
        logger.info("alert acknowledged")

    async def on_task(self, task: Task) -> None:
        logger.info("received task: %s", task.task_id)

    async def on_error(self, error: Error) -> None:
        logger.warning("received error from fusion node: %s", error.error_message)
