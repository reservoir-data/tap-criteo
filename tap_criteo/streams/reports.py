from typing import Dict

from singer_sdk import typing as th
from singer_sdk.helpers._classproperty import classproperty


class DateType(th.JSONTypeHelper):
    """Date type."""

    @classproperty
    def type_dict(cls) -> dict:
        """Return dict describing the type."""
        return {
            "type": ["string"],
            "format": "date",
        }

dimensions_mapping: Dict[str, th.Property] = {
    "Adset": th.Property("Adset", th.StringType),
    "AdsetId": th.Property("AdsetId", th.StringType),
    "Campaign": th.Property("Campaign", th.StringType),
    "CampaignId": th.Property("CampaignId", th.StringType),
    "Advertiser": th.Property("Advertiser", th.StringType),
    "AdvertiserId": th.Property("AdvertiserId", th.StringType),
    "Category": th.Property("Category", th.StringType),
    "CategoryId": th.Property("CategoryId", th.StringType),
    "OS": th.Property("Os", th.StringType),
    "Device": th.Property("Device", th.StringType),
    "Currency": th.Property("Currency", th.StringType),
    "Year": th.Property("Year", DateType),
    "Month": th.Property("Month", DateType),
    "Week": th.Property("Week", DateType),
    "Day": th.Property("Day", DateType),
    "Hour": th.Property("Hour", th.DateTimeType),
}

metrics_mapping: Dict[str, th.Property] = {
    "Clicks": th.Property("Clicks", th.IntegerType),
    "Displays": th.Property("Displays", th.IntegerType),
    "Visits": th.Property("Visits", th.IntegerType),
}
