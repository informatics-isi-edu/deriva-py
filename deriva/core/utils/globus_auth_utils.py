import os
import sys
import stat
import json
import logging
import platform
import traceback
import importlib
from pprint import pprint
from requests.exceptions import HTTPError, ConnectionError
from deriva.core import __version__ as VERSION, DEFAULT_CONFIG_PATH, DEFAULT_GLOBUS_CREDENTIAL_FILE, read_config,\
    format_exception, BaseCLI, get_oauth_scopes_for_host
from deriva.core.utils import eprint

GLOBUS_SDK = None
NATIVE_LOGIN = None
NATIVE_APP_CLIENT_ID = "8ef15ba9-2b4a-469c-a163-7fd910c9d111"
CLIENT_CRED_FILE = '/home/secrets/oauth2/client_secret_globus.json'
DEFAULT_SCOPES = ["openid", "profile", "email", "urn:globus:auth:scope:auth.globus.org:view_identities"]


class UsageException(ValueError):
    """Usage exception.
    """

    def __init__(self, message):
        """Initializes the exception.
        """
        super(UsageException, self).__init__(message)


class DependencyError(ImportError):
    """Dependency exception.
    """

    def __init__(self, message):
        """Initializes the exception.
        """
        super(DependencyError, self).__init__(message)


class GlobusAuthAPIException(Exception):
    pass


GlobusAuthAPIError = GlobusAuthAPIException


class GlobusAuthUtil:
    def __init__(self, **kwargs):
        client_id = kwargs.get("client_id")
        client_secret = kwargs.get("client_secret")
        if not (client_id and client_secret):
            cred_file = kwargs.get("credential_file", CLIENT_CRED_FILE)
            creds = read_config(cred_file)
            if creds:
                client = creds.get("web")
                if client:
                    client_id = client.get('client_id')
                    client_secret = client.get('client_secret')
        try:
            global GLOBUS_SDK, GlobusAuthAPIError
            GLOBUS_SDK = importlib.import_module("globus_sdk")
            GlobusAuthAPIError = GLOBUS_SDK.AuthAPIError
        except Exception as e:
            raise DependencyError("Unable to load a required module: %s" % format_exception(e))

        if not (client_id and client_secret):
            logging.warning("Client ID and secret not specified and/or could not be determined.")
        self.client = GLOBUS_SDK.ConfidentialAppAuthClient(client_id, client_secret)
        self.client_id = client_id

    @staticmethod
    def from_json(obj):
        # if "obj" is a dict, use it. Otherwise, see if it's a JSON string or a file containing json.
        if isinstance(obj, dict):
            return obj
        try:
            result = json.loads(obj)
        except ValueError:
            # if this fails, we have nothing left to try, so don't bother catching errors
            f = open(obj, 'r')
            result = json.load(f)
            f.close()
        return result

    def list_all_scopes(self):
        r = self.client.get("/v2/api/scopes")
        return r.data

    def list_scope(self, scope):
        r = self.client.get("/v2/api/scopes/{s}".format(s=scope))
        return r.data

    def create_scope(self, scope):
        if not scope:
            raise UsageException("A supported scope argument is required.")

        r = self.client.post("/v2/api/clients/{client_id}/scopes".format(client_id=self.client_id),
                             json_body=self.from_json(scope))
        return r.data

    def update_scope(self, scope_id, scope):
        if not (scope_id and scope):
            raise UsageException("The scope_id and scope arguments are required.")

        r = self.client.put("/v2/api/scopes/{scope_id}".format(scope_id=scope_id),
                            json_body=self.from_json(scope))
        return r.data

    def add_fqdn_to_client(self, fqdn):
        if not fqdn:
            raise UsageException("A fqdn (fully qualified domain name) argument is required.")
        r = self.client.post('/v2/api/clients/{client_id}/fqdns'.format(client_id=self.client_id),
                             json_body={'fqdn': fqdn})
        return r.data

    def get_client_for_fqdn(self, fqdn):
        if not fqdn:
            raise UsageException("A fqdn (fully qualified domain name) argument is required.")
        r = self.client.get('/v2/api/clients?fqdn={fqdn}'.format(fqdn=fqdn))
        return r.data

    def get_client(self, client_id):
        r = self.client.get('/v2/api/clients/{client_id}'.format(client_id=client_id if client_id else self.client_id))
        return r.data

    def get_clients(self):
        r = self.client.get('/v2/api/clients')
        return r.data

    def verify_access_token(self, token):
        if not token:
            raise UsageException("A token argument is required.")
        r = self.client.oauth2_validate_token(token)
        return r.data

    def introspect_access_token(self, token):
        if not token:
            raise UsageException("A token argument is required.")
        r = self.client.oauth2_token_introspect(token)
        return r.data

    def get_dependent_access_tokens(self, token, refresh=False):
        if not token:
            raise UsageException("A token argument is required.")
        additional_params = {"access_type": "offline"} if refresh else None
        r = self.client.oauth2_get_dependent_tokens(token, additional_params)
        return r.data

    def new_client(self, client):
        r = self.client.post("/v2/api/clients",
                             json_body=self.from_json(client))
        return r.data

    def create_client(self,
                      name,
                      redirect_uris,
                      public=False,
                      visibility="private",
                      project=None,
                      required_idp=None,
                      preselect_idp=None,
                      terms_of_service=None,
                      privacy_policy=None):
        if not (name and redirect_uris):
            raise UsageException("The name and redirect_uris arguments are required.")
        client = {
            "client": {
                "name": name,
                "public_client": public,
                "redirect_uris": redirect_uris,
                "visibility": visibility if (visibility == "private" or visibility == "public") else "private"
            }
        }
        if project:
            client["client"].update({"project": project})
        if required_idp:
            client["client"].update({"required_idp": required_idp})
        if preselect_idp:
            client["client"].update({"preselect_idp": preselect_idp})
        if terms_of_service or privacy_policy:
            client["client"].update({"links": {"terms_of_service": terms_of_service,
                                               "privacy_policy": privacy_policy}})
        return self.new_client(client)

    def update_client(self, client, client_id=None):
        if not client:
            raise UsageException("A client argument is required.")

        r = self.client.put("/v2/api/clients/{client_id}".format(client_id=client_id if client_id else self.client_id),
                            json_body=self.from_json(client))
        return r.data

    def delete_client(self, client_id):
        if not client_id:
            raise UsageException("A client argument is required.")

        r = self.client.delete("/v2/api/clients/{client_id}".format(client_id=client_id))
        return r.data

    def add_redirect_uris(self, redirect_uris):
        if not redirect_uris:
            raise UsageException("A redirect_uris argument is required.")
        d = {
            "client": {
                "redirect_uris": redirect_uris
            }
        }
        return self.update_client(d)

    def get_my_client(self):
        r = self.client.get('/v2/api/clients/{client_id}'.format(client_id=self.client_id))
        return r.data

    def get_scopes_by_name(self, scope_name):
        if not scope_name:
            raise UsageException("A scope_name argument is required.")
        scopes = self.client.get('/v2/api/scopes?scope_strings={sname}'.format(sname=scope_name))
        if scopes is None:
            return None
        else:
            return scopes.get("scopes")

    def get_scopes_by_id(self, id_string):
        if not id_string:
            raise UsageException("A id_string argument is required.")
        scopes = self.client.get('/v2/api/scopes?ids={ids}'.format(ids=id_string))
        if scopes is None:
            return None
        else:
            return scopes.get("scopes")

    def my_scope_ids(self):
        c = self.client.get('/v2/api/clients/{client_id}'.format(client_id=self.client_id))
        me = c.get("client")
        if me is None or me.get('scopes') is None:
            return []
        else:
            return me.get('scopes')

    def my_scope_names(self):
        scope_names = []
        scope_ids = self.my_scope_ids()
        if scope_ids is not None:
            ids = ",".join(scope_ids)
            print(str(ids))
            scopes = self.get_scopes_by_id(ids)
            for scope in scopes:
                scope_names.append(scope.get('scope_string'))
        return scope_names

    def get_grant_types(self):
        grant_types = None
        c = self.client.get('/v2/api/clients/{client_id}'.format(client_id=self.client_id))
        me = c.get("client")
        if me is not None:
            grant_types = me.get('grant_types')
        return grant_types

    def add_scopes(self, new_scopes, by_id=False):
        if not new_scopes:
            raise UsageException("A new_scopes argument is required.")
        scopes = set(self.my_scope_ids())
        if by_id:
            scopes.update(new_scopes)
        else:
            for scope in self.get_scopes_by_name(",".join(new_scopes)):
                scopes.add(scope.get('id'))
        d = {
            "client": {
                "scopes": list(scopes)
            }
        }

        r = self.client.put('/v2/api/clients/{client_id}'.format(client_id=self.client_id), json_body=d)
        return r.data

    def add_dependent_scopes(self, parent_scope_name, child_scopes,
                             by_id=False, optional=False, requires_refresh_token=False):
        if not (parent_scope_name and child_scopes):
            raise UsageException("The parent_scope_name and child_scope_names arguments are required.")

        dependent_scopes = []
        new_child_scope_ids = set()
        parent_scopes = self.get_scopes_by_name(parent_scope_name)
        if parent_scopes is None:
            return "no parent scope"
        if len(parent_scopes) != 1:
            return "{sl} parent scopes: {p}".format(sl=str(len(parent_scopes)), p=str(parent_scopes))
        parent_scope_id = parent_scopes[0].get("id")
        if by_id:
            new_child_scope_ids.update(child_scopes)
        else:
            new_child_scopes = self.get_scopes_by_name(",".join(child_scopes))
            for scope in new_child_scopes:
                new_child_scope_ids.add(scope.get('scope'))
        for scope in parent_scopes[0].get('dependent_scopes'):
            if scope.get('id') not in new_child_scope_ids:
                dependent_scopes.append(scope)
        for scope_id in new_child_scope_ids:
            dependent_scopes.append({'scope': scope_id,
                                     'optional': optional,
                                     'requires_refresh_token': requires_refresh_token})
        d = {
            "scope": {
                "dependent_scopes": dependent_scopes
            }
        }
        print(str(d))
        r = self.client.put('/v2/api/scopes/{i}'.format(i=parent_scope_id),
                            json_body=d)
        return r.data

    def create_scope_with_deps(self, name, description, suffix, dependent_scopes=[], advertised=True,
                               allow_refresh_tokens=True):
        if not (name and description and suffix):
            raise UsageException("The name, description and suffix arguments are required.")
        dependent_scope_arg = []
        if len(dependent_scopes) > 0:
            child_scopes = self.get_scopes_by_name(",".join(dependent_scopes))
            for scope in child_scopes:
                dependent_scope_arg.append({
                    "scope": scope.get("id"),
                    "optional": False,
                    "requires_refresh_token": False
                })
        scope = {
            "scope": {
                "name": name,
                "description": description,
                "scope_suffix": suffix,
                "dependent_scopes": dependent_scope_arg,
                "advertised": advertised,
                "allows_refresh_tokens": allow_refresh_tokens
            }
        }

        r = self.client.post("/v2/api/clients/{client_id}/scopes".format(client_id=self.client_id),
                             json_body=scope)
        return r.data

    def delete_scope(self, scope_name):
        if not scope_name:
            raise UsageException("A scope_name argument is required.")
        scopes = self.get_scopes_by_name(scope_name)
        if scopes is None or len(scopes) != 1:
            return "null or multiple scopes"
        scope_id = scopes[0].get('id')
        if scope_id is None:
            return "no scope id"
        r = self.client.delete('/v2/api/scopes/{scope_id}'.format(scope_id=scope_id))
        return r.data

    def get_dependent_scopes(self, scope):
        if not scope:
            raise UsageException("A supported scope argument is required.")
        result = {"scope_string": scope.get("scope_string"), "dependent_scopes": []}
        for ds in scope.get("dependent_scopes"):
            ds_id = ds.get('scope')
            ds_info = {"id": ds_id}
            d = self.get_scopes_by_id(ds_id)
            if d is not None:
                ds_info['scope_string'] = d[0].get('scope_string')
            result['dependent_scopes'].append(ds_info)
        return result


class DerivaJSONTokenStorage(object):
    """
    Stores tokens in json format on disk in the local directory by default.
    """
    def __init__(self, filename=None, permission=None):
        self.filename = filename or DEFAULT_GLOBUS_CREDENTIAL_FILE
        self.permission = permission or stat.S_IRUSR | stat.S_IWUSR

    def write_tokens(self, tokens, overwrite=False):
        all_tokens = self.read_tokens() if not overwrite else dict()
        all_tokens.update(tokens)
        with open(self.filename, 'w+') as fh:
            json.dump(all_tokens, fh, indent=2)

    def read_tokens(self):
        if not os.path.exists(self.filename):
            return dict()
        with open(self.filename) as fh:
            return json.load(fh)

    def clear_tokens(self, requested_scopes=()):
        if not requested_scopes and os.path.exists(self.filename):
            os.remove(self.filename)
            return

        tokens = self.read_tokens() or dict()
        token_set = list()
        for resource, token in tokens.items():
            scope = token["scope"]
            if scope in requested_scopes:
                token_set.append(resource)
        for resource in token_set:
            logging.info("Clearing token for resource: %s" % resource)
            del tokens[resource]
        self.write_tokens(tokens, overwrite=True)


class GlobusNativeLogin:
    def __init__(self, **kwargs):
        self.client = None
        self.client_id = kwargs.get("client_id") or NATIVE_APP_CLIENT_ID
        self.hosts = kwargs.get("hosts")
        self.config_file = kwargs.get("config_file")
        self.default_scopes = DEFAULT_SCOPES.copy()
        if self.client_id == NATIVE_APP_CLIENT_ID:
            self.default_scopes.append("urn:globus:auth:scope:nexus.api.globus.org:groups")

        try:
            global GLOBUS_SDK, NATIVE_LOGIN
            GLOBUS_SDK = importlib.import_module("globus_sdk")
            NATIVE_LOGIN = importlib.import_module("fair_research_login")
        except Exception as e:
            raise DependencyError("Unable to load a required module: %s" % format_exception(e))

        try:
            storage_file = 'globus-credential%s.json' % \
                           (("-" + self.client_id) if self.client_id != NATIVE_APP_CLIENT_ID else "")
            storage = DerivaJSONTokenStorage(filename=os.path.join(DEFAULT_CONFIG_PATH, storage_file))
            self.client = NATIVE_LOGIN.NativeClient(
                client_id=self.client_id,
                token_storage=storage,
                app_name="Login from deriva-client on %s [%s]%s" %
                         (platform.uname()[1],
                          platform.platform(aliased=True),
                          " to hosts [%s]" % ", ".join(self.hosts) if self.hosts else ""),
                default_scopes=self.default_scopes)
        except Exception as e:
            logging.error("Unable to instantiate a required class: %s" % format_exception(e))

    def user_info(self, host):
        pass

    def is_logged_in(self, hosts=None, requested_scopes=()):
        try:
            scopes = set(requested_scopes)
            scopes.update(self.hosts_to_scope_list(hosts))
            logged_in = True
            token_scopes = [item for sublist in
                            [token["scope"].split() for token in self.client.load_tokens().values()]
                            for item in sublist]
            for scope in scopes:
                if scope not in token_scopes:
                    logged_in = False
            return logged_in
        except NATIVE_LOGIN.LoadError:
            return False

    def hosts_to_scope_list(self, hosts, match_scope_tag=None, all_tagged_scopes=False, force_refresh=False):
        scope_list = list()
        for host in hosts:
            scopes = get_oauth_scopes_for_host(host, self.config_file, force_refresh=force_refresh)
            if scopes:
                for k, v in scopes.items():
                    if match_scope_tag is None:
                        scope_list.append(v)
                        if all_tagged_scopes:
                            continue
                        break
                    elif match_scope_tag == k:
                        scope_list.append(v)
                        break
                    else:
                        continue
        return scope_list

    def login(self,
              hosts=None,
              no_local_server=False,
              no_browser=False,
              requested_scopes=(),
              refresh_tokens=None,
              prefill_named_grant=None,
              additional_params=None,
              force=False,
              match_scope_tag=None):
        scopes = set(requested_scopes)
        scopes.update(self.hosts_to_scope_list(hosts, match_scope_tag, force_refresh=True))
        if not prefill_named_grant:
            prefill_named_grant = self.client.app_name + " with requested scopes [%s] " % ", ".join(scopes)
        return self.client.login(no_local_server=no_local_server,
                                 no_browser=no_browser,
                                 requested_scopes=scopes,
                                 refresh_tokens=refresh_tokens,
                                 prefill_named_grant=prefill_named_grant,
                                 additional_params=additional_params,
                                 force=force)

    def logout(self, hosts, requested_scopes=()):
        scopes = set(requested_scopes)
        scopes.update(self.hosts_to_scope_list(hosts))
        tokens = self.client._load_raw_tokens()

        if not scopes:
            logging.info("Logging out and invalidating tokens for ALL existing scopes.")
            self.client.revoke_token_set(tokens)
            self.client.token_storage.clear_tokens()
            return

        token_set = dict()
        for resource, token in tokens.items():
            if token["scope"] in scopes:
                token_set[resource] = token
        self.client.revoke_token_set(token_set)
        self.client.token_storage.clear_tokens(scopes)


class DerivaGlobusAuthUtilCLIException(Exception):
    def __init__(self, message):
        super(DerivaGlobusAuthUtilCLIException, self).__init__(message)


class DerivaGlobusAuthUtilCLI(BaseCLI):
    """Deriva GlobusClientUtil Command-line Interface.
    """

    def __init__(self, *args, **kwargs):
        super(DerivaGlobusAuthUtilCLI, self).__init__(*args, **kwargs)

        self.gau = None
        self.gnl = None
        self.remove_options(['--host', '--credential-file', '--token', '--oauth2-token'])
        self.parser.add_argument("--pretty", "-p", action="store_true",
                                 help="Pretty-print all result output.")
        parent_mutex_group = self.parser.add_mutually_exclusive_group()
        parent_mutex_group.add_argument('--credential-file', '-f', metavar='<file>',
                                        help="Path to a credential file.")
        parent_mutex_group.add_argument('--client-id', '-c', metavar='<client id>',
                                        help="Globus Auth Client ID")
        self.parser.add_argument('--client-secret', '-k', metavar='<client secret key>',
                                 help="Globus Auth Client Secret")

        self.subparsers = self.parser.add_subparsers(title='sub-commands', dest='subcmd')

        # init subparsers and corresponding functions
        self.get_scopes_init()
        self.put_scope_init()
        self.add_scopes_init()
        self.create_scope_init()
        self.delete_scope_init()
        self.get_client_init()
        self.put_client_init()
        self.create_client_init()
        self.delete_client_init()
        self.client_fqdn_init()
        self.token_init()
        self.login_init()
        self.logout_init()
        # self.user_info_init()

    def get_scopes_init(self):
        def get_scopes(args):
            if args.scope_ids:
                return self.gau.get_scopes_by_id(args.scope_ids)
            elif args.scope_names:
                return self.gau.get_scopes_by_name(args.scope_names)
            else:
                return self.gau.list_all_scopes()

        parser = self.subparsers.add_parser(
            'get-scopes',
            help="Get one or more scopes by name or ID, or all scopes owned by this client if no scope ID is "
                 "specified.")
        parser.add_argument("--scope-ids", metavar="[scopes]",
                            help="A comma-delimited list of scope IDs to retrieve.")
        parser.add_argument("--scope-names", metavar="[scopes]",
                            help="A comma-delimited list of scope names to retrieve.")
        parser.set_defaults(func=get_scopes)

    def put_scope_init(self):
        def put_scope(args):
            if args.scope_id:
                return self.gau.update_scope(args.scope_config)
            else:
                return self.gau.create_scope(args.scope_config)

        parser = self.subparsers.add_parser(
            "put-scope",
            help="Create or update a scope owned by this client from a valid JSON scope configuration in string form, "
                 "or a path to an equivalent file.")
        parser.add_argument("--scope-id", metavar="<scope id>", help="The scope ID.")
        parser.add_argument("scope_config", metavar="<JSON String or path to file>",
                            help="A valid JSON scope configuration in string form, or a path to an equivalent file.")
        parser.set_defaults(func=put_scope)

    def add_scopes_init(self):
        def add_scopes(args):
            if args.parent_scope:
                return self.gau.add_dependent_scopes(args.parent_scope,
                                                     args.scope_names,
                                                     args.by_id,
                                                     args.optional,
                                                     args.requires_refresh_token)
            else:
                return self.gau.add_scopes(args.scope_names, args.by_id)

        parser = self.subparsers.add_parser(
            'add-scopes',
            help="Add one or more scopes by name (or optionally by ID) to this client, or add one or more scopes as "
                 "dependent scopes of a specified parent scope.")
        parser.add_argument("scope_names", metavar="[scopes]", default=list(),
                            type=lambda s: [item.strip() for item in s.split(',')],
                            help="A comma-delimited list of scope names or scope IDs.")
        parser.add_argument("--parent-scope", metavar="<scope name>",
                            help="The parent scope name if adding dependent scopes.")
        parser.add_argument("--by-id", action="store_true",
                            help="Interpret the scope name list as a list of scope IDs. "
                                 "Note: does not validate the listed scopes beforehand. Default false.")
        parser.add_argument("--optional", action="store_true",
                            help="When adding to dependent scopes, sets the scope as optional. Default false.")
        parser.add_argument("--requires-refresh-token", action="store_true",
                            help="When adding to dependent scopes, sets the scope to require the usage of refresh "
                                 "tokens. Default false.")
        parser.set_defaults(func=add_scopes)

    def create_scope_init(self):
        def create_scope(args):
            return self.gau.create_scope_with_deps(args.name,
                                                   args.description,
                                                   args.suffix,
                                                   args.dependent_scope_names,
                                                   args.advertised,
                                                   args.allow_refresh_token)
        parser = self.subparsers.add_parser(
            'create-scope',
            help="Creates a scope for each registered FQDN + the id of the client from command-line parameters.")
        parser.add_argument("name", metavar="<scope name>",
                            help="The new scope name. Max 100 chars.")
        parser.add_argument("description", metavar="<scope desc>",
                            help="A scope description. Max 5000 chars.")
        parser.add_argument("suffix", metavar="<scope suffix>",
                            help="String consisting of lowercase letters, number, and underscores.")
        parser.add_argument("--dependent-scope-names", metavar="[scopes]", default=list(),
                            type=lambda s: [item.strip() for item in s.split(',')],
                            help="A comma-delimited list of dependent scope names.")
        parser.add_argument("--advertised", action="store_true",
                            help="Whether or not the scope should show up in searches. Default: True")
        parser.add_argument("--allow-refresh-token", action="store_true",
                            help="Whether or not the scope allows refresh tokens to be issued. Default: True")
        parser.set_defaults(func=create_scope)

    def delete_scope_init(self):
        def delete_scope(args):
            return self.gau.delete_scope(args.scope_name)

        parser = self.subparsers.add_parser(
            "del-scope",
            help="Delete the specified scope name. Deleting a scope deletes all resources associated with it. This "
                 "operation can cause other apps and services that depend on the scope to stop working. This action "
                 "cannot be undone.")
        parser.add_argument("scope_name", metavar="<scope_name>", help="The scope name to delete.")
        parser.set_defaults(func=delete_scope)

    def get_client_init(self):
        def get_client(args):
            if args.get_client_id:
                return self.gau.get_client(args.get_client_id)
            else:
                return self.gau.get_clients()

        parser = self.subparsers.add_parser(
            'get-client',
            help="Retrieve client information for the specified client, or all clients owned by this client ID if no "
                 "ID specified.")
        parser.add_argument("--get-client-id", metavar="<client ID>",
                            help="Retrieve client information for the specified client ID.")
        parser.set_defaults(func=get_client)

    def put_client_init(self):
        def put_client(args):
            if args.create:
                return self.gau.new_client(args.client_config)
            else:
                return self.gau.update_client(args.client_config, args.put_client_id)

        parser = self.subparsers.add_parser(
            'put-client',
            help="Update this (or another) client OR create a child client of this client from a valid JSON client "
                 "configuration in string form, or a path to an equivalent file.")
        parser.add_argument("client_config", metavar="<JSON String or path to file>",
                            help="A valid JSON client config in string form, or a path to an equivalent file.")
        parser.add_argument("--put-client-id", metavar="<client id>",
                            help="The client ID to update, or implicitly this client's ID if not specified.")
        parser.add_argument("--create", action="store_true",
                            help="Create a new child client from the input client config.")
        parser.set_defaults(func=put_client)

    def create_client_init(self):
        def create_client(args):
            return self.gau.create_client(args.name,
                                          args.redirect_uris,
                                          args.public,
                                          args.visibility,
                                          args.project,
                                          args.required_idp,
                                          args.preselect_idp,
                                          args.terms_of_service,
                                          args.privacy_policy)

        parser = self.subparsers.add_parser('create-client', help="Create a client from command-line parameters.")
        parser.add_argument("name", metavar="<client name>",
                            help="Display name shown to users in consents. String without line-breaks, with no "
                                 "more than 100 characters.")
        parser.add_argument("redirect_uris", metavar="[redirect uris]", default=list(),
                            type=lambda s: [item.strip() for item in s.split(',')],
                            help="A comma-delimited list of URIs that may be used in OAuth authorization flows.")
        parser.add_argument("--public", action="store_true",
                            help="Create a public (native app) client.")
        parser.add_argument("--visibility", choices=["public", "private"],
                            help="\"private\" means that only entities in the same project as the client "
                                 "can view it. \"public\" means that any authenticated entity can view it.")
        parser.add_argument("--project", metavar="<project ID>",
                            help="ID representing the project this client belongs to.")
        parser.add_argument("--required-idp", metavar="<IDP ID>",
                            help="ID representing an Identity Provider. In order to use this client a user must"
                                 " have an identity from this IdP in their identity set.")
        parser.add_argument("--preselect-idp", metavar="<IDP ID>",
                            help="ID representing an Identity Provider. This preselects the given IdP on the "
                                 "Globus Auth login page if the user is not already authenticated.")
        parser.add_argument("--terms-of-service", metavar="<URL>",
                            help="A URL to the terms and conditions statement for this client.")
        parser.add_argument("--privacy_policy", metavar="<URL>",
                            help="A URL to the privacy policy for this client.")
        parser.set_defaults(func=create_client)

    def delete_client_init(self):
        def delete_client(args):
            return self.gau.delete_client(args.del_client_id)

        parser = self.subparsers.add_parser(
            'del-client',
            help="Delete a client by ID. Warning: deletes all resources associated with it the client. This includes "
                 "user consents, scopes (which means this operation can cause other apps and services that depend "
                 "on those scopes to stop working as well), and any child clients owned by the client (which in turn "
                 "means that all resources associated with the child clients would get deleted as well). This action "
                 "cannot be undone.")
        parser.add_argument("del_client_id", metavar="<client id>",
                            help="The client ID to update, or implicitly this client's ID if not specified.")
        parser.set_defaults(func=delete_client)

    def client_fqdn_init(self):
        def client_fqdn(args):
            if args.add:
                return self.gau.add_fqdn_to_client(args.fqdn)
            else:
                return self.gau.get_client_for_fqdn(args.fqdn)

        parser = self.subparsers.add_parser(
            'client-fqdn',
            help="Retrieve client information for an FQDN, or add and FQDN to this client.")
        parser.add_argument("--add", action="store_true",
                            help="Add the specified FQDN to this client ID")
        parser.add_argument("fqdn", metavar="<fqdn>",
                            help="The fully qualified domain name to lookup or add to this client.")
        parser.set_defaults(func=client_fqdn)

    def token_init(self):
        def token(args):
            if args.validate:
                return self.gau.verify_access_token(args.token)
            elif args.dependent:
                return self.gau.get_dependent_access_tokens(args.token, args.refresh)
            else:
                return self.gau.introspect_access_token(args.token)

        parser = self.subparsers.add_parser('token', help="Introspect or validate an access token.")
        parser.add_argument("--validate", action="store_true",
                            help="Validate the access token.")
        parser.add_argument("--dependent", action="store_true",
                            help="Get dependent access token(s).")
        parser.add_argument("--refresh", action="store_true",
                            help="Request refresh tokens when getting dependent access token(s).")
        parser.add_argument("token", metavar="<token>",
                            help="The access token to introspect (or validate).")
        parser.set_defaults(func=token)

    def login_init(self):
        def login(args):
            if self.gnl.is_logged_in(args.hosts, args.requested_scopes) and not args.force:
                return "You are already logged in."
            else:
                response = self.gnl.login(hosts=args.hosts,
                                          no_local_server=args.no_local_server,
                                          no_browser=args.no_browser,
                                          refresh_tokens=args.refresh,
                                          force=args.force,
                                          requested_scopes=args.requested_scopes)
                if args.show_tokens:
                    return response
                else:
                    return "Login Successful"

        parser = self.subparsers.add_parser('login', help="Login with Globus Auth")
        mutex_group = parser.add_mutually_exclusive_group(required=True)
        mutex_group.add_argument("--hosts", metavar="[hostnames]", default=list(),
                                 type=lambda s: [item.strip() for item in s.split(',')],
                                 help="A comma-delimited list of host names to login to. "
                                 "An attempt to determine the required scope will be made by checking the local "
                                 "configuration or (if required) contacting each <host> will be made.")
        mutex_group.add_argument("--requested-scopes", metavar="[scopes]", default=list(),
                                 type=lambda s: [item.strip() for item in s.split(',')],
                                 help="A comma-delimited list of scope names to request tokens for. "
                                 "If not specified, an attempt will be made to determine the required scope by "
                                 "checking the local configuration or (if required) contacting each <host>.")

        parser.add_argument('--client-id', '-c', metavar='<client id>',
                            help="Use a different client ID than the default.")
        parser.add_argument("--no-local-server", action="store_true",
                            help="Do not launch a local server to receive the authorization redirect response.")
        parser.add_argument("--no-browser", action="store_true",
                            help="Do not launch a browser instance on this system for initiating the login flow.")
        parser.add_argument("--refresh", action="store_true",
                            help="Enable the use of refresh tokens to extend the login time until revoked.")
        parser.add_argument("--force", action="store_true",
                            help="Force a login flow even if the current access token set is valid.")
        parser.add_argument("--show-tokens", action="store_true",
                            help="Display the tokens from the authorization response.")
        parser.set_defaults(func=login)

    def logout_init(self):
        def logout(args):
            self.gnl.logout(args.hosts,
                            args.requested_scopes)
            return "You have been logged out."

        parser = self.subparsers.add_parser("logout", help="Revoke and clear tokens. If no arguments are specified, "
                                                           "all tokens will be removed.")
        mutex_group = parser.add_mutually_exclusive_group()
        mutex_group.add_argument("--hosts", metavar="[hostnames]", default=list(),
                                 type=lambda s: [item.strip() for item in s.split(',')],
                                 help="A comma-delimited list of host names to revoke tokens for. "
                                 "An attempt to determine the associated scope(s) will be made by checking the local "
                                 "configuration or (if required) contacting each <host> will be made.")
        mutex_group.add_argument("--requested-scopes", metavar="[scopes]", default=list(),
                                 type=lambda s: [item.strip() for item in s.split(',')],
                                 help="A comma-delimited list of scope names to revoke tokens for. "
                                 "If not specified, an attempt will be made to determine the associated scope(s) by "
                                 "checking the local configuration or (if required) contacting each <host>.")
        parser.set_defaults(func=logout)

    def user_info_init(self):
        def user_info(args):
            if self.gnl.is_logged_in(args.hosts, args.requested_scopes):
                return self.gnl.user_info()
            else:
                return "Login required."

        parser = self.subparsers.add_parser("user-info", help="Display information for the currently logged-in user.")
        parser.set_defaults(func=user_info)

    def main(self):
        args = self.parse_cli()

        def _cmd_error_message(emsg):
            return "{prog} {subcmd}: {msg}".format(
                prog=self.parser.prog, subcmd=args.subcmd, msg=emsg)

        try:
            if not hasattr(args, 'func'):
                self.parser.print_usage()
                return 2

            if args.subcmd == "login" or args.subcmd == "logout" or args.subcmd == "user-info":
                self.gnl = GlobusNativeLogin(**vars(args))
            else:
                self.gau = GlobusAuthUtil(**vars(args))

            response = args.func(args)
            if args.pretty:
                if isinstance(response, dict) or isinstance(response, list):
                    try:
                        print(json.dumps(response, indent=2))
                        return
                    except:
                        pprint(response)
                        return
                elif not isinstance(response, str):
                    pprint(response)
                    return
            print(response)
            return

        except UsageException as e:
            eprint("{prog} {subcmd}: {msg}".format(prog=self.parser.prog, subcmd=args.subcmd, msg=e))
        except ConnectionError as e:
            eprint("{prog}: Connection error occurred".format(prog=self.parser.prog))
        except GlobusAuthAPIError as e:
            if 401 == e.http_status:
                msg = 'Authentication required: %s' % e.message
            elif 403 == e.http_status:
                msg = 'Permission denied: %s' % e.message
            else:
                msg = e
            logging.debug(format_exception(e))
            eprint(_cmd_error_message(msg))
        except DependencyError as e:
            eprint(_cmd_error_message(e))
        except RuntimeError as e:
            logging.debug(format_exception(e))
            eprint('Unexpected runtime error occurred')
        except:
            eprint('Unexpected error occurred')
            traceback.print_exc()
        return 1


def main():
    desc = "DERIVA Globus Auth Utilities"
    info = "For more information see: https://github.com/informatics-isi-edu/deriva-py"
    return DerivaGlobusAuthUtilCLI(desc, info).main()


if __name__ == '__main__':
    sys.exit(main())
