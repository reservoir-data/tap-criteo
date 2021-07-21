"""Criteo Authentication."""

from singer_sdk.authenticators import OAuthAuthenticator


class CriteoAuthenticator(OAuthAuthenticator):
    """Authenticator class for Criteo."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the Criteo API."""

        return {
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "grant_type": "client_credentials",
        }

    @classmethod
    def create_for_stream(cls, stream) -> "CriteoAuthenticator":
        return cls(stream=stream, auth_endpoint="https://api.criteo.com/oauth2/token")
