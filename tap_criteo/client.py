"""REST client handling, including CriteoStream base class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from singer_sdk.streams import RESTStream

from tap_criteo.auth import CriteoAuthenticator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record


class CriteoStream(RESTStream):
    """Criteo stream class."""

    url_base = "https://api.criteo.com"

    records_jsonpath = "$.data[*]"

    primary_keys = ("id",)

    @override
    @property
    def authenticator(self) -> CriteoAuthenticator:
        """Return a new authenticator object."""
        return CriteoAuthenticator(
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
            auth_endpoint="https://api.criteo.com/oauth2/token",
        )

    # flatten attributes field
    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        """Flatten the 'attributes' dictionary into top-level."""
        if "attributes" in row and isinstance(row["attributes"], dict):
            attributes = row.pop("attributes")

            row.update(attributes)

        return row


class CriteoSearchStream(CriteoStream):
    """Search stream."""

    http_method = "post"

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: Any,
    ) -> dict:
        """Prepare request payload.

        Args:
            context: Stream context.
            next_page_token: Next page value.

        Returns:
            Dictionary for the JSON request body.
        """
        return {}
