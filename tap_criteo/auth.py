"""Criteo Authentication."""

from __future__ import annotations

from singer_sdk.authenticators import OAuthAuthenticator


class CriteoAuthenticator(OAuthAuthenticator):
    """Authenticator class for Criteo."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the Criteo API.

        Returns:
            A dictionary with the request body for authentication.
        """
        return {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
