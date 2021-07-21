"""Stream type classes for Criteo version 2021.04."""

from datetime import datetime, timedelta
from pathlib import Path

from dateutil.parser import parse

from singer_sdk import typing as th
from singer_sdk.plugin_base import PluginBase as TapBaseClass

from tap_criteo.client import CriteoSearchStream, CriteoStream
from tap_criteo.streams.reports import dimensions_mapping, metrics_mapping

SCHEMAS_DIR = Path(__file__).parent.parent / "./schemas"


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
    rest_method = "post"

    def __init__(
        self,
        tap: TapBaseClass,
        report: dict,
    ):
        name = report["name"]
        schema = th.PropertiesList(
            *[dimensions_mapping[d] for d in report["dimensions"]],
            *[metrics_mapping[m] for m in report["metrics"]],
            th.Property("Currency", th.StringType),
        ).to_dict()

        super().__init__(tap, name=name, schema=schema)

        self.dimensions = report["dimensions"]
        self.metrics = report["metrics"]
        self.currency = report["currency"]
        self.primary_keys = self.dimensions

    def prepare_request_payload(self, context, next_page_token) -> dict:
        start_date = parse(self.config["start_date"])
        end_date = start_date + timedelta(days=10)

        return {
            "dimensions": self.dimensions,
            "metrics": self.metrics,
            "currency": self.currency,
            "format": "json",
            "timezone": "UTC",
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        }

    def post_process(self, row: dict, context) -> dict:
        if "Hour" in row:
            hour = row.pop("Hour")
            row["Hour"] = datetime.strptime(hour, "%m/%d/%Y %H:%M:%S")

        for field in filter(lambda s: s.startswith("Clicks"), self.metrics):
            value = row.pop(field)
            row[field] = int(value)

        for field in filter(lambda s: s.startswith("Displays"), self.metrics):
            displays = row.pop(field)
            row[field] = int(displays)

        return row
