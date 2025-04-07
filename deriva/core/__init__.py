__version__ = "1.7.8"

from deriva.core.utils.core_utils import *
from deriva.core.base_cli import BaseCLI, KeyValuePairArgs
from deriva.core.deriva_binding import DerivaBinding, DerivaPathError, DerivaClientContext
from deriva.core.deriva_server import DerivaServer
from deriva.core.ermrest_catalog import ErmrestCatalog, ErmrestSnapshot, ErmrestCatalogMutationError, ErmrestAlias
from deriva.core.polling_ermrest_catalog import PollingErmrestCatalog
from deriva.core.hatrac_store import HatracStore, HatracHashMismatch, HatracJobPaused, HatracJobAborted, \
    HatracJobTimeout
from deriva.core.utils.globus_auth_utils import GlobusNativeLogin


def get_credential(host,
                   credential_file=DEFAULT_CREDENTIAL_FILE,
                   globus_credential_file=DEFAULT_GLOBUS_CREDENTIAL_FILE,
                   config_file=DEFAULT_CONFIG_FILE,
                   requested_scope=None,
                   force_scope_lookup=False,
                   match_scope_tag="deriva-all",
                   update_bdbag_keychain=True):
    """
    This function is used to get authorization credentials (in dict form) for use with various deriva-py API calls
    which take it as a parameter. A user must have already authenticated to the target host using either `deriva-auth`
    or `deriva-globus-auth-utils login` prior to calling this function, or the credential set for the host will not be
    found.

    :param host: The hostname to retrieve the credential set for.
    :param credential_file: Optional path to non-default location of the webauthn cookie credential file.
    :param globus_credential_file: Optional path to non-default location of the GlobusAuth bearer token store.
    :param config_file: Optional path to the non-default location of the deriva-py config file.
    :param requested_scope: Optional, specific scope request string for the given host. If not specified, the webauthn
        service on the host will be queried to determine the host-to-scope mapping that should be used.
    :param force_scope_lookup: Optional parameter to force the webauthn scope query and update the cached value in the
        deriva-py config file. A scope lookup will always be performed the first time a host-to-scope mapping is needed
        and is not already present in the configuration file for a given host.
    :param match_scope_tag: In the case that a host-to-scope mapping request returns multiple scopes, this is the key
        value ("tag") to match against in the result dict. By convention, the default is set to "deriva-all", which is
        the expected response from webauthn.
    :param update_bdbag_keychain: Updates the bdbag keychain file with the bearer token mapped to `host`. This is
        done to ensure that the bdbag keychain is updated when a refreshable bearer-token gets refreshed during the
        login check. Defaults to True.
    :return: A dict containing credential authorization values mapped by authorization type
    """
    # load deriva credential set first
    credentials = read_credential(credential_file or DEFAULT_CREDENTIAL_FILE, create_default=True)
    creds = credentials.get(host, credentials.get(host.lower(), dict()))

    # load globus credentials and merge, if present
    if globus_credential_file is not None and \
            os.path.isfile(globus_credential_file) and \
            os.path.getsize(globus_credential_file) > 10:  # Don't load empty json
        try:
            globus_client = GlobusNativeLogin(hosts=[host], config_file=config_file)
            scope_map = globus_client.hosts_to_scope_map(hosts=[host], match_scope_tag=match_scope_tag,
                                                         force_refresh=force_scope_lookup,
                                                         warn_on_discovery_failure=True if not creds else False)
            tokens = globus_client.is_logged_in(exclude_defaults=True)
            if tokens:
                # 1. look for the explicitly requested scope in the token store, if specified
                token = globus_client.find_access_token_for_scope(requested_scope, tokens)
                if not token:
                    # 2. try to determine the scope to use based on host-to-scope(s) mappings
                    token = globus_client.find_access_token_for_host(host,
                                                                     scope_map,
                                                                     tokens,
                                                                     match_scope_tag=match_scope_tag)
                if token:
                    creds["bearer-token"] = token
                    if update_bdbag_keychain:
                        globus_client.update_bdbag_keychain(token=token, host=host)
        except Exception as e:
            logging.warning("Exception while getting Globus credentials: %s" % format_exception(e))

    return creds or None

