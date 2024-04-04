import json
import logging
from importlib import import_module
from deriva.core import get_credential, urlsplit, urlunsplit, format_exception, stob
from deriva.core.utils.globus_auth_utils import GlobusNativeLogin
from deriva.core.utils.webauthn_utils import get_wallet_entries
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.base_processor import *
from fair_research_login.client import NoSavedTokens, TokensExpired
from fair_identifiers_client.identifiers_api import IdentifierClient, AccessTokenAuthorizer, IdentifierClientError


class FAIRIdentifierPostProcessor(BaseProcessor):
    """
    Post processor that mints identifiers for download results using FAIR-Research Identifier Client.
    """

    IDENTIFIER_SERVICE = "https://identifiers.fair-research.org/"
    IDENTIFIER_SERVICE_TEST = "https://identifiers-test.fair-research.org/"
    IDENTIFIER_SERVICE_WRITER_SCOPE = "https://auth.globus.org/scopes/identifiers.fair-research.org/writer"
    TEST_IDENTIFIER_NAMESPACE = "minid-test"
    IDENTIFIER_NAMESPACE = "minid"

    def __init__(self, envars=None, **kwargs):
        super(FAIRIdentifierPostProcessor, self).__init__(envars, **kwargs)

    def load_identifier_client(self, identifiers_service_url):
        if not self.identity:
            logging.warning("Unauthenticated (anonymous) identity being used with identifier client")
        if self.wallet:
            entries = get_wallet_entries(self.wallet, "oauth2",
                                         credential_source="https://auth.globus.org",
                                         scopes=[self.IDENTIFIER_SERVICE_WRITER_SCOPE])
            token = entries[0].get("access_token") if entries else None
        else:
            tokens = None
            host = self.envars["hostname"]
            gnl = GlobusNativeLogin(host=host)
            try:
                tokens = gnl.client.load_tokens()
            except NoSavedTokens:
                pass
            except TokensExpired as e:
                raise RuntimeError(
                    "Unable to obtain token set due to refresh token expiry. Please logout of the expired scopes. %s" %
                    format_exception(e))
            if not tokens:
                raise RuntimeError("Login required. No saved tokens.")
            token = gnl.find_access_token_for_scope(self.IDENTIFIER_SERVICE_WRITER_SCOPE, tokens)
        ac = AccessTokenAuthorizer(token) if token else None
        return IdentifierClient(base_url=identifiers_service_url, app_name="DERIVA Export", authorizer=ac)

    def process(self):
        test = stob(self.parameters.get("test", "False"))
        test_service = stob(self.parameters.get("test_service", "False"))
        identifiers_service_url = self.IDENTIFIER_SERVICE_TEST if test_service else self.IDENTIFIER_SERVICE
        ic = self.load_identifier_client(identifiers_service_url)
        namespace = (self.TEST_IDENTIFIER_NAMESPACE if test else self.IDENTIFIER_NAMESPACE)
        for k, v in self.outputs.items():
            file_path = v[LOCAL_PATH_KEY]
            self.make_file_output_values(file_path, v)
            checksum = v[SHA256_KEY][0]
            title = self.parameters.get("title", "%s" % k)
            metadata = {"title": title}
            length = v[FILE_SIZE_KEY]
            if length is not None:
                metadata.update({"length": length})
            author = self.parameters.get("author")
            if author and stob(self.parameters.get("include_author", "True")):
                metadata.update({"author": author})
            created_by = self.identity.get('full_name') if self.identity else None
            if created_by:
                metadata.update({"created_by": created_by})
            visible_to = self.parameters.get("visible_to", ["public"])
            locations = v.get(REMOTE_PATHS_KEY) or self.parameters.get("locations")
            if not locations:
                raise DerivaDownloadConfigurationError(
                    "Invalid URLs: One or more location URLs must be specified when registering an identifier.")
            env_column_map = self.parameters.get("env_column_map")
            if env_column_map:
                env_metadata = {key: val.format(**self.envars) for key, val in env_column_map.items()}
                metadata.update(env_metadata)
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
                v[IDENTIFIER_LANDING_PAGE] = \
                    self.parameters.get("redirect_base", "") + identifiers_service_url + identifier
            except IdentifierClientError as e:
                raise DerivaDownloadError("Unable to create identifier: %s" % format_exception(e))

        return self.outputs
