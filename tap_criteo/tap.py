"""Criteo tap class."""
from __future__ import annotations

from typing import TYPE_CHECKING

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_criteo.streams import v202601

if TYPE_CHECKING:
    from tap_criteo.client import CriteoStream

OBJECT_STREAMS: dict[str, list[type[CriteoStream]]] = {
    "current": [
        v202601.AudiencesStream,
        v202601.AdvertisersStream,
        v202601.CampaignsStream,
        v202601.AdSetsStream,
        v202601.AdsStream,
        v202601.CreativesStream,
    ],
}

REPORTS_BASE = v202601.StatsReportStream


class TapCriteo(Tap):
    """Criteo tap class."""

    name = "tap-criteo"

    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("advertiser_ids", th.ArrayType(th.StringType), required=True),
        th.Property("start_date", th.DateTimeType, required=True),
        th.Property(
            "reports",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True),
                    th.Property(
                        "dimensions",
                        th.ArrayType(th.StringType),
                        required=True,
                    ),
                    th.Property("metrics", th.ArrayType(th.StringType), required=True),
                    th.Property("currency", th.StringType, default="USD"),
                ),
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams.

        Returns:
            List of stream instances.
        """
        objects = [
            stream_class(tap=self)
            for api in ("current",)
            for stream_class in OBJECT_STREAMS[api]
        ]

        reports = [
            REPORTS_BASE(tap=self, report=report) for report in self.config["reports"]
        ]

        return objects + reports
