import json
import logging
from importlib import import_module
from deriva.core import get_credential, urlsplit, urlunsplit, format_exception, stob
from deriva.core.utils.webauthn_utils import get_wallet_entries
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.base_processor import *
from fair_identifiers_client.config import config
from fair_identifiers_client.identifiers_api import identifiers_client, IdentifierClient, AccessTokenAuthorizer, \
    IdentifierClientError


class FAIRIdentifierPostProcessor(BaseProcessor):
    """
    Post processor that mints identifiers for download results using FAIR-Research Identifier Client.
    """

    IDENTIFIER_SERVICE = "https://identifiers.fair-research.org/"
    TEST_IDENTIFIER_NAMESPACE = "minid-test"
    IDENTIFIER_NAMESPACE = "minid"

    def __init__(self, envars=None, **kwargs):
        super(FAIRIdentifierPostProcessor, self).__init__(envars, **kwargs)

    def load_identifier_client(self):
        if not self.identity:
            logging.warning("Unauthenticated (anonymous) identity being used with identifier client")
        if self.wallet:
            entries = get_wallet_entries(self.wallet, "oauth2",
                                         credential_source="https://auth.globus.org",
                                         scopes=["https://auth.globus.org/scopes/identifiers.fair-research.org/writer"])
            token = entries[0].get("access_token") if entries else None
            ac = AccessTokenAuthorizer(token) if token else None
            return IdentifierClient(base_url=self.IDENTIFIER_SERVICE, app_name="DERIVA Export", authorizer=ac)
        else:
            return identifiers_client(config)

    def process(self):
        ic = self.load_identifier_client()
        test = stob(self.parameters.get("test", "False"))
        namespace = (self.TEST_IDENTIFIER_NAMESPACE if test else self.IDENTIFIER_NAMESPACE)
        for k, v in self.outputs.items():
            file_path = v[LOCAL_PATH_KEY]
            self.make_file_output_values(file_path, v)
            checksum = v[SHA256_KEY][0]
            title = self.parameters.get("title", "DERIVA Export: %s" % k)
            metadata = {"title": title}
            author = self.parameters.get("author")
            if not author:
                author = self.identity.get('full_name') if self.identity else None
            if author and stob(self.parameters.get("include_author", "True")):
                metadata.update({"author": author})
            visible_to = self.parameters.get("visible_to", ["public"])
            locations = v.get(REMOTE_PATHS_KEY)
            if not locations:
                raise DerivaDownloadConfigurationError(
                    "Invalid URLs: One or more location URLs must be specified when registering an identifier.")

            kwargs = {
                "namespace": namespace,
                "visible_to": visible_to,
                "location": locations,
                "checksums": [{
                    "function": "sha256",
                    "value": checksum
                }],
                "metadata": metadata
            }
            try:
                logging.info("Attempting to create identifier for file [%s] with locations: %s" %
                             (file_path, locations))
                minid = ic.create_identifier(**kwargs)
                identifier = minid["identifier"]
                v[IDENTIFIER_KEY] = identifier
                v[IDENTIFIER_LANDING_PAGE] = self.IDENTIFIER_SERVICE + identifier
            except IdentifierClientError as e:
                raise DerivaDownloadError("Unable to create identifier: %s" % format_exception(e))

        return self.outputs
