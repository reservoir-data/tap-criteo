"""Stream type classes for Criteo version 2026.01."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dateutil.parser import parse

from tap_criteo.client import CriteoSearchStream, CriteoStream
from tap_criteo.streams.reports import analytics_type_mappings, value_func_mapping

if TYPE_CHECKING:
    from singer_sdk.plugin_base import PluginBase as TapBaseClass

SCHEMAS_DIR = Path(__file__).parent.parent / "./schemas/v2026.01"
UTC = timezone.utc


class AudiencesStream(CriteoSearchStream):
    """Audiences stream."""

    name = "audiences"
    path = "/2026-01/marketing-solutions/audiences/search"
    schema_filepath = SCHEMAS_DIR / "audience.json"

    # override to add body
    def prepare_request_payload(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ARG002
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
    schema_filepath = SCHEMAS_DIR / "advertiser.json"

    def get_child_context(
        self,
        record: dict,
        context: dict | None,  # noqa: ARG002
    ) -> dict:
        """Return a context dictionary for child streams."""
        return {"id": record["id"]}

    def post_process(
        self,
        row: dict,
        context: dict | None,  # noqa: ARG002
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
    schema_filepath = SCHEMAS_DIR / "campaign.json"


class AdSetsStream(CriteoSearchStream):
    """Ad sets stream."""

    name = "ad_sets"
    path = "/2026-01/marketing-solutions/ad-sets/search"
    schema_filepath = SCHEMAS_DIR / "ad_set.json"


class StatsReportStream(CriteoStream):
    """Statistics reports stream."""

    name = "statistics"
    path = "/2026-01/statistics/report"
    records_jsonpath = "$.Rows[*]"
    rest_method = "post"

    def __init__(
        self,
        tap: TapBaseClass,
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

    def prepare_request_payload(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any,  # noqa: ARG002, ANN401
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

    def post_process(
        self,
        row: dict,
        context: dict | None,  # noqa: ARG002
    ) -> dict:
        """Process the record before emitting it.

        Args:
            row: Record dictionary.
            context: Stream context.

        Returns:
            Mutated record dictionary.
        """
        for key in row:
            func = value_func_mapping.get(key)
            if func:
                row[key] = func(row[key])
        return row


class AdsStream(CriteoStream):
    """Ads stream."""

    name = "ads"
    path = "/2026-01/marketing-solutions/advertisers/{id}/ads"
    schema_filepath = SCHEMAS_DIR / "ad.json"

    parent_stream_type = AdvertisersStream
    ignore_parent_replication_key = True

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}

        params["limit"] = 50
        params["offset"] = next_page_token if next_page_token else 0

        return params


class CreativesStream(CriteoStream):
    """Creatives stream."""

    name = "creatives"
    path = "/2026-01/marketing-solutions/advertisers/{id}/creatives"
    schema_filepath = SCHEMAS_DIR / "creative.json"

    parent_stream_type = AdvertisersStream
    ignore_parent_replication_key = True

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}

        params["limit"] = 50
        params["offset"] = next_page_token if next_page_token else 0

        return params
