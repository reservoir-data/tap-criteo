"""REST client handling, including CriteoStream base class."""

from pathlib import Path

from singer_sdk.streams import RESTStream

from tap_criteo.auth import CriteoAuthenticator

SCHEMAS_DIR = Path(__file__).parent / "./schemas"


class CriteoStream(RESTStream):
    """Criteo stream class."""

    url_base = "https://api.criteo.com"

    records_jsonpath = "$.data[*]"

    primary_keys = ["id"]

    @property
    def authenticator(self) -> CriteoAuthenticator:
        """Return a new authenticator object."""
        return CriteoAuthenticator.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers


class CriteoSearchStream(CriteoStream):
    rest_method = "post"

    def prepare_request_payload(self, context, next_page_token) -> dict:
        return {}
