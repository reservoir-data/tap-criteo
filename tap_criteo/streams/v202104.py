"""Stream type classes for Criteo version 2021.04."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dateutil.parser import parse

from tap_criteo.client import CriteoSearchStream, CriteoStream
from tap_criteo.streams.reports import analytics_type_mappings, value_func_mapping

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from singer_sdk.tap_base import Tap

SCHEMAS_DIR = Path(__file__).parent.parent / "./schemas"
UTC = timezone.utc


class AudiencesStream(CriteoStream):
    """Audiences stream."""

    name = "audiences"
    path = "/2021-04/audiences"
    schema_filepath = SCHEMAS_DIR / "audience.json"


class AdvertisersStream(CriteoStream):
    """Advertisers stream."""

    name = "advertisers"
    path = "/2021-04/advertisers/me"
    schema_filepath = SCHEMAS_DIR / "advertiser.json"


class AdSetsStream(CriteoSearchStream):
    """Ad sets stream."""

    name = "ad_sets"
    path = "/2021-04/marketing-solutions/ad-sets/search"
    schema_filepath = SCHEMAS_DIR / "ad_set.json"


DIMENSIONS = [
    "AdSetId",
    "CampaignId",
    "AdvertiserId",
    "OS",
    "Device",
    "Hour",
]

METRICS = [
    "Clicks",
    "Displays",
    "Visits",
]

CURRENCY = "USD"


class StatsReportStream(CriteoStream):
    """Statistics reports stream."""

    name = "statistics"
    path = "/2021-04/statistics/report"
    records_jsonpath = "$.Rows[*]"
    http_method = "post"

    @override
    def __init__(
        self,
        tap: Tap,
        report: dict,
    ) -> None:
        """Initialize a stats report stream.

        Args:
            tap: The tap instance.
            report: The report dictionary.
        """
        name = report["name"]
        schema = {"properties": {"Currency": {"type": "string"}}}
        schema["properties"].update(
            {k: analytics_type_mappings[k] for k in report["metrics"]},
        )
        schema["properties"].update(
            {k: analytics_type_mappings[k] for k in report["dimensions"]},
        )

        super().__init__(tap, name=name, schema=schema)

        self.dimensions = report["dimensions"]
        self.metrics = report["metrics"]
        self.currency = report["currency"]
        self.primary_keys = self.dimensions

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: Any,
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream context.
            next_page_token: The next page value.

        Returns:
            Dictionary for the JSON body of the request.
        """
        start_date = parse(self.config["start_date"])
        end_date = datetime.now(UTC)

        return {
            "dimensions": self.dimensions,
            "metrics": self.metrics,
            "currency": self.currency,
            "format": "json",
            "timezone": "UTC",
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        }

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Process the record before emitting it.

        Args:
            row: Record dictionary.
            context: Stream context.

        Returns:
            Mutated record dictionary.
        """
        for key, value in row.items():
            func = value_func_mapping.get(key)
            if func:
                row[key] = func(value)
        return row
