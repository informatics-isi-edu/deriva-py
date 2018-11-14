from copy import deepcopy
"""Utility functions that really belong in webauthn, but that are here to avoid having deriva-py depend on webauthn"""


def get_wallet_entries(wallet, credential_type="oauth2", **kwargs):
    """
    :param wallet: wallet - the wallet to examine (from a Client object)
    :param credential_type: the type of credential requested from the wallet
    :param kwargs: keyword arguments
    :return: a list of oauth2 credentials obtained from auth.globus.org with the requested scope

    Currently, only "oauth2" is supported as a credential type, and the following keyword args are supported
    (all of these are optional):

        * `credential_source` - where the credentials came from (e.g., "https://auth.globus.org").
        * `resource_server` - the resource server associated with a credential.
        * `scopes` - a list of desired scopes.

    Example #1:
    ::
        wallet = client.wallet.extra_values.get("wallet")
        entries = get_wallet_entries(wallet, "oauth2", resource_server="identifiers.globus.org")

    :return: a list of oauth2 credentials associated with the resource server `identifiers.globus.org`.

    Example #2:
    ::
        entries = get_wallet_entries(wallet, "oauth2", credential_source="https://auth.globus.org")

    :return: a list of oauth2 credentials obtained from `auth.globus.org`.

    Example #3:
    ::
        entries = get_wallet_entries(
                      wallet,
                      "oauth2",
                      credential_source="https://auth.globus.org",
                      scopes=["https://auth.globus.org/scopes/identifiers.globus.org/create_update")

    :return: a list of oauth2 credentials obtained from `auth.globus.org` with the requested scope.

    .. note::
        Eventually, we may support wallets with multiple credential types (at the same time) for talking
        to a variety of remote servers, at which point the implementation will probably become a lot more
        generalized (with registered wallet providers, etc.).

    """
    if wallet is None:
        return []
    wallet = deepcopy(wallet)

    matching_entries = []
    
    # wallet structure is
    # {
    #   credential_type : {
    #     credential_source : [
    #       <oauth entry, w/ "scope", "resource_server", "access_token", etc.>
    
    if credential_type != "oauth2":
        raise NotImplementedError("Only 'oauth2' wallet credentials are supported")
    wallet = wallet.get(credential_type)
    if wallet is None:
        return []

    # wallet structure is now
    #   {
    #     credential_source : [
    #       <oauth entry, w/ "scope", "resource_server", "access_token", etc.>
    
    if kwargs.get('credential_source') is not None:
        matching_entries = wallet.get(kwargs.get('credential_source'))
        if matching_entries is None:
            return []
    else:
        for entries in wallet.values():
            matching_entries = matching_entries + entries

    resource_server = kwargs.get('resource_server')
    to_remove = []    
    if resource_server is not None:
        for entry in matching_entries:
            if entry.get('resource_server') != resource_server:
                to_remove.append(entry)

    for entry in to_remove:
        matching_entries.remove(entry)

    to_remove = []
    if kwargs.get('scopes') is not None:
        for entry in matching_entries:
            if entry.get("scope") is not None:
                entry_scopes = entry.get("scope").split()
                for a_scope in kwargs.get('scopes'):
                    if a_scope not in entry_scopes:
                        to_remove.append(entry)
                        break

    for entry in to_remove:
        matching_entries.remove(entry)
                    
    return matching_entries
