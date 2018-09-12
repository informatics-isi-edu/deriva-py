import json
import logging
from importlib import import_module
from deriva.core import get_credential, urlsplit, urlunsplit, format_exception, strtobool
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.base_processor import *


class IdentifierPostProcessor(BaseProcessor):
    """
    Post processor that mints identifiers for download results
    """
    def __init__(self, envars=None, **kwargs):
        super(IdentifierPostProcessor, self).__init__(envars, **kwargs)

    def process(self):
        return self.outputs


class MinidIdentifierPostProcessor(IdentifierPostProcessor):
    """
    Post processor that mints identifiers for download results using legacy "minid_client_api".
    """
    MINID = None

    def __init__(self, envars=None, **kwargs):
        super(MinidIdentifierPostProcessor, self).__init__(envars, **kwargs)
        self.import_minid_api()

    def import_minid_api(self):
        # locate library
        if self.MINID is None:
            try:
                self.MINID = import_module("minid_client")
            except ImportError as e:
                raise DerivaDownloadConfigurationError(
                    "Unable to find required module. Ensure that the Python package \"minid\" is installed.", e)

    def process(self):
        config_file = self.parameters.get("minid_client_config", self.MINID.minid_client_api.DEFAULT_CONFIG_FILE)
        config = self.MINID.minid_client_api.parse_config(config_file)
        server = config.get("minid_server", "http://minid.bd2k.org/minid")
        email = config.get("email", self.identity.get("email"))
        code = config.get("code")
        for k, v in self.outputs.items():
            file_path = v[LOCAL_PATH_KEY]
            self.make_file_output_values(file_path, v)
            checksum = v[SHA256_KEY][0]
            locations = v.get(REMOTE_PATHS_KEY)
            if not locations:
                raise DerivaDownloadConfigurationError(
                    "Invalid URLs: One or more location URLs must be specified when registering an identifier.")
            result = self.MINID.minid_client_api.register_entity(
                server, checksum, email, code,
                url=locations, title=self.parameters.get("title", ''),
                test=strtobool(self.parameters.get("test", "False")),
                globus_auth_token=None, checksum_function=None)
            v[IDENTIFIER_KEY] = result

        return self.outputs


class GlobusIdentifierPostProcessor(IdentifierPostProcessor):
    """
    Post processor that mints identifiers for download results using Globus SDK and Globus Identifier Client.
    """

    GLOBUS_SDK = None
    GLOBUS_IDENTIFIER_CLIENT = None
    TEST_IDENTIFIER_NAMESPACE = 'HHxPIZaVDh9u'
    IDENTIFIER_NAMESPACE = 'kHAAfCby2zdn'

    def __init__(self, envars=None, **kwargs):
        super(GlobusIdentifierPostProcessor, self).__init__(envars, **kwargs)
        self.import_globus_sdk()

    def import_globus_sdk(self):
        # locate libraries
        if self.GLOBUS_SDK is None and self.GLOBUS_IDENTIFIER_CLIENT is None:
            try:
                self.GLOBUS_SDK = import_module("globus_sdk")
            except ImportError as e:
                raise DerivaDownloadConfigurationError(
                    "Unable to find required module. Ensure that the Python package \"globus_sdk\" is installed.", e)
            try:
                self.GLOBUS_IDENTIFIER_CLIENT = import_module("identifier_client")
            except ImportError as e:
                raise DerivaDownloadConfigurationError(
                    "Unable to find required module. "
                    "Ensure that the Python package \"identifier_client\" is installed.", e)

    def load_identifier_client(self):
        token = self.get_access_token_from_wallet("identifiers.globus.org", self.wallet)
        ac = self.GLOBUS_SDK.AccessTokenAuthorizer(token) if token else None
        return self.GLOBUS_IDENTIFIER_CLIENT.identifier_api.IdentifierClient(
            'Identifier', base_url='https://identifiers.globus.org/', app_name='DERIVA Export', authorizer=ac)

    def process(self):
        ic = self.load_identifier_client()
        test = strtobool(self.parameters.get("test", "False"))
        namespace = (self.TEST_IDENTIFIER_NAMESPACE if test else self.IDENTIFIER_NAMESPACE)
        for k, v in self.outputs.items():
            file_path = v[LOCAL_PATH_KEY]
            self.make_file_output_values(file_path, v)
            checksum = v[SHA256_KEY][0]
            title = self.parameters.get("title", "DERIVA Export: %s" % k)
            metadata = {"title": title}
            locations = v.get(REMOTE_PATHS_KEY)
            if not locations:
                raise DerivaDownloadConfigurationError(
                    "Invalid URLs: One or more location URLs must be specified when registering an identifier.")

            visible_to = ('public',)
            kwargs = {
                'namespace': namespace,
                'visible_to': json.dumps(visible_to),
                'location': json.dumps(locations),
                'checksums': json.dumps([{
                    'function': 'sha256',
                    'value': checksum
                }]),
                'metadata': json.dumps(metadata)
            }
            try:
                minid = ic.create_identifier(**kwargs)
                v[IDENTIFIER_KEY] = minid['identifier']
            except self.GLOBUS_IDENTIFIER_CLIENT.identifier_api.IdentifierClientError as e:
                raise DerivaDownloadError("Unable to create identifier: %s" % e.message)

        return self.outputs
