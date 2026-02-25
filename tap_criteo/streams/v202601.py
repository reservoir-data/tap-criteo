"""Stream type classes for Criteo version 2026.01."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from importlib.resources import files
from typing import TYPE_CHECKING, Any

from dateutil.parser import parse
from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.pagination import BaseOffsetPaginator

from tap_criteo import schemas
from tap_criteo.client import CriteoSearchStream, CriteoStream
from tap_criteo.streams.reports import analytics_type_mappings, value_func_mapping

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record
    from singer_sdk.tap_base import Tap

PAGE_SIZE = 50
SCHEMAS_DIR = SchemaDirectory(files(schemas) / "v2026.01")
UTC = timezone.utc


class AudiencesStream(CriteoSearchStream):
    """Audiences stream."""

    name = "audiences"
    path = "/2026-01/marketing-solutions/audiences/search"
    schema = StreamSchema(SCHEMAS_DIR, key="audience")

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict:
        """Prepare request payload for audiences search."""
        advertiser_ids = self.config.get("advertiser_ids", [])

        return {
            "data": {
                "type": "AudienceSearchEntity",
                "attributes": {"advertiserIds": advertiser_ids},
            },
        }


class AdvertisersStream(CriteoStream):
    """Advertisers stream."""

    name = "advertisers"
    path = "/2026-01/advertisers/me"
    schema = StreamSchema(SCHEMAS_DIR, key="advertiser")

    @override
    def get_child_context(
        self,
        record: Record,
        context: Context | None,
    ) -> dict:
        """Return a context dictionary for child streams."""
        return {"advertiserId": record["id"]}

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Scope to provided advertisers."""
        if "attributes" in row and isinstance(row["attributes"], dict):
            attributes = row.pop("attributes")
            row.update(attributes)

        advertiser_ids = self.config.get("advertiser_ids", [])
        if advertiser_ids and str(row.get("id")) not in advertiser_ids:
            return None

        return row


class CampaignsStream(CriteoSearchStream):
    """Campaigns stream."""

    name = "campaigns"
    path = "/2026-01/marketing-solutions/campaigns/search"
    schema = StreamSchema(SCHEMAS_DIR, key="campaign")


class AdSetsStream(CriteoSearchStream):
    """Ad sets stream."""

    name = "ad_sets"
    path = "/2026-01/marketing-solutions/ad-sets/search"
    schema = StreamSchema(SCHEMAS_DIR, key="ad_set")


class StatsReportStream(CriteoStream):
    """Statistics reports stream."""

    name = "statistics"
    path = "/2026-01/statistics/report"
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

        advertiser_ids = ",".join(self.config.get("advertiser_ids", []))

        payload = {
            "dimensions": self.dimensions,
            "metrics": self.metrics,
            "currency": self.currency,
            "format": "json",
            "timezone": "UTC",
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        }

        if advertiser_ids:
            payload["advertiserIds"] = advertiser_ids

        return payload

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


class AdsStream(CriteoStream):
    """Ads stream."""

    name = "ads"
    path = "/2026-01/marketing-solutions/advertisers/{advertiserId}/ads"
    schema = StreamSchema(SCHEMAS_DIR, key="ad")

    parent_stream_type = AdvertisersStream
    ignore_parent_replication_key = True

    @override
    def get_new_paginator(self) -> BaseOffsetPaginator:
        """Return a new paginator for this API endpoint."""
        return BaseOffsetPaginator(start_value=0, page_size=PAGE_SIZE)

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}

        params["limit"] = PAGE_SIZE
        params["offset"] = next_page_token or 0

        return params


class CreativesStream(CriteoStream):
    """Creatives stream."""

    name = "creatives"
    path = "/2026-01/marketing-solutions/advertisers/{advertiserId}/creatives"
    schema = StreamSchema(SCHEMAS_DIR, key="creative")

    parent_stream_type = AdvertisersStream
    ignore_parent_replication_key = True

    @override
    def get_new_paginator(self) -> BaseOffsetPaginator:
        """Return a new paginator for this API endpoint."""
        return BaseOffsetPaginator(start_value=0, page_size=PAGE_SIZE)

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}

        params["limit"] = PAGE_SIZE
        params["offset"] = next_page_token or 0

        return params
