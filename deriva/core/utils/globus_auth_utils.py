import os
import sys
import json
import logging
import traceback
from pprint import pprint
from requests.exceptions import HTTPError, ConnectionError
from deriva.core import __version__ as VERSION, read_config, init_logging, format_exception, BaseCLI
from deriva.core.utils import eprint

CLIENT_CRED_FILE = '/home/secrets/oauth2/client_secret_globus.json'


class UsageException(ValueError):
    """Usage exception.
    """

    def __init__(self, message):
        """Initializes the exception.
        """
        super(UsageException, self).__init__(message)


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
            client_id = creds['web'].get('client_id')
            client_secret = creds['web'].get('client_secret')
        try:
            from globus_sdk import ConfidentialAppAuthClient
            global GlobusAuthAPIError
            from globus_sdk.exc import AuthAPIError as GlobusAuthAPIError
            self.client = ConfidentialAppAuthClient(client_id, client_secret)
            self.client_id = client_id
        except Exception as e:
            logging.error("Unable to instantiate required globus_sdk class ConfidentialAppAuthClient: %s" % e)
            raise

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

    def add_scopes(self, new_scopes):
        if not new_scopes:
            raise UsageException("A new_scopes argument is required.")
        scopes = set(self.my_scope_ids())
        for scope in self.get_scopes_by_name(",".join(new_scopes)):
            scopes.add(scope.get('id'))
        d = {
            "client": {
                "scopes": list(scopes)
            }
        }

        r = self.client.put('/v2/api/clients/{client_id}'.format(client_id=self.client_id), json_body=d)
        return r.data

    def add_dependent_scopes(self, parent_scope_name, child_scope_names, optional=False, requires_refresh_token=False):
        if not (parent_scope_name and child_scope_names):
            raise UsageException("The parent_scope_name and child_scope_names arguments are required.")

        child_scope_ids = set()
        parent_scopes = self.get_scopes_by_name(parent_scope_name)
        if parent_scopes is None:
            return "no parent scope"
        if len(parent_scopes) != 1:
            return "{sl} parent scopes: {p}".format(sl=str(len(parent_scopes)), p=str(parent_scopes))
        parent_scope_id = parent_scopes[0].get("id")
        for scope in parent_scopes[0].get('dependent_scopes'):
            child_scope_ids.add(scope.get('id'))
        new_child_scopes = self.get_scopes_by_name(",".join(child_scope_names))
        for scope in new_child_scopes:
            child_scope_ids.add(scope.get('id'))
        dependent_scopes = []
        for scope_id in child_scope_ids:
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


class DerivaGlobusAuthUtilCLIException(Exception):
    def __init__(self, message):
        super(DerivaGlobusAuthUtilCLIException, self).__init__(message)


class DerivaGlobusAuthUtilCLI(BaseCLI):
    """Deriva GlobusClientUtil Command-line Interface.
    """

    def __init__(self, *args, **kwargs):
        super(DerivaGlobusAuthUtilCLI, self).__init__(*args, **kwargs)

        self.gau = None
        self.remove_options(['--host', '--config-file', '--credential-file', '--token', '--oauth2-token'])
        self.parser.add_argument("--pretty", "-p", action="store_true",
                                 help="Pretty-print all result output.")
        parent_mutex_group = self.parser.add_mutually_exclusive_group(required=True)
        parent_mutex_group.add_argument('--credential-file', '-f', metavar='<file>',
                                        help="Path to a credential file.")
        parent_mutex_group.add_argument('--client-id', '-c', metavar='<client id>',
                                        help="Globus Auth Client ID")
        self.parser.add_argument('--client-secret', '-k', metavar='<client secret key>',
                                 help="Globus Auth Client Secret")

        subparsers = self.parser.add_subparsers(title='sub-commands', dest='subcmd')

        # get-scope parser
        get_scopes = subparsers.add_parser('get-scopes',
                                           help="Get one or more scopes by name or ID, or all scopes owned by this "
                                                "client if no scope ID is specified.")
        get_scopes.add_argument("--scope-ids", metavar="[scopes]",
                                help="A comma-delimited list of scope IDs to retrieve.")
        get_scopes.add_argument("--scope-names", metavar="[scopes]",
                                help="A comma-delimited list of scope names to retrieve.")
        get_scopes.set_defaults(func=self.get_scopes)

        # put-scope parser
        put_scope = subparsers.add_parser("put-scope",
                                          help="Create or update a scope owned by this client from a valid JSON scope "
                                               "configuration in string form, or a path to an equivalent file.")
        put_scope.add_argument("--scope-id", metavar="<scope id>", help="The scope ID.")
        put_scope.add_argument("scope_config", metavar="<JSON String or path to file>",
                               help="A valid JSON scope configuration in string form, or a path to an equivalent file.")
        put_scope.set_defaults(func=self.put_scope)

        # add-scope parser
        add_scopes = subparsers.add_parser('add-scopes',
                                           help="Add one or more scopes by name to this client, or add one or more "
                                                "scopes as dependent scopes of a specified parent scope.")
        add_scopes.add_argument("scope_names", metavar="[scopes]", default=list(),
                                type=lambda s: [item for item in s.split(',')],
                                help="A comma-delimited list of scope names.")
        add_scopes.add_argument("--parent-scope", metavar="<scope name>",
                                help="The parent scope name if adding dependent scopes.")
        add_scopes.add_argument("--optional", action="store_true",
                                help="When adding to dependent scopes, sets the scope as optional. Default false.")
        add_scopes.add_argument("--requires-refresh-token", action="store_true",
                                help="When adding to dependent scopes, sets the scope to require the usage of refresh "
                                     "tokens. Default false.")
        add_scopes.set_defaults(func=self.add_scopes)

        # create-scope parser
        create_scope = subparsers.add_parser('create-scope',
                                             help="Creates a scope for each registered FQDN + the id of the client "
                                                  "from command-line parameters.")
        create_scope.add_argument("name", metavar="<scope name>",
                                  help="The new scope name. Max 100 chars.")
        create_scope.add_argument("description", metavar="<scope desc>",
                                  help="A scope description. Max 5000 chars.")
        create_scope.add_argument("suffix", metavar="<scope suffix>",
                                  help="String consisting of lowercase letters, number, and underscores.")
        create_scope.add_argument("--dependent-scope-names", metavar="[scopes]", default=list(),
                                  type=lambda s: [item for item in s.split(',')],
                                  help="A comma-delimited list of dependent scope names.")
        create_scope.add_argument("--advertised", action="store_true",
                                  help="Whether or not the scope should show up in searches. Default: True")
        create_scope.add_argument("--allow-refresh-token", action="store_true",
                                  help="Whether or not the scope allows refresh tokens to be issued. Default: True")
        create_scope.set_defaults(func=self.create_scope)

        # del-scope parser
        del_scope = subparsers.add_parser("del-scope",
                                          help="Delete the specified scope name. "
                                               "Deleting a scope deletes all resources associated with it. This "
                                               "operation can cause other apps and services that depend on the scope "
                                               "to stop working. This action cannot be undone.")
        del_scope.add_argument("scope_name", metavar="<scope_name>", help="The scope name to delete.")
        del_scope.set_defaults(func=self.delete_scope)

        # get-client parser
        get_client = subparsers.add_parser('get-client',
                                           help="Retrieve client information for the specified client, "
                                                "or all clients owned by this client ID if no ID specified.")
        get_client.add_argument("--get-client-id", metavar="<client ID>",
                                help="Retrieve client information for the specified client ID.")
        get_client.set_defaults(func=self.get_client)

        # put-client parser
        put_client = subparsers.add_parser('put-client',
                                           help="Update this (or another) client OR create a child client of this "
                                                "client from a valid JSON client configuration in string form, or a "
                                                "path to an equivalent file.")
        put_client.add_argument("client_config", metavar="<JSON String or path to file>",
                                help="A valid JSON client config in string form, or a path to an equivalent file.")
        put_client.add_argument("--put-client-id", metavar="<client id>",
                                help="The client ID to update, or implicitly this client's ID if not specified.")
        put_client.add_argument("--create", action="store_true",
                                help="Create a new child client from the input client config.")
        put_client.set_defaults(func=self.put_client)

        # create-client parser
        create_client = subparsers.add_parser('create-client',
                                              help="Create a client from command-line parameters.")
        create_client.add_argument("name", metavar="<client name>",
                                   help="Display name shown to users in consents. String without line-breaks, with no "
                                        "more than 100 characters.")
        create_client.add_argument("redirect_uris", metavar="[redirect uris]", default=list(),
                                   type=lambda s: [item for item in s.split(',')],
                                   help="A comma-delimited list of URIs that may be used in OAuth authorization flows.")
        create_client.add_argument("--public", action="store_true",
                                   help="Create a public (native app) client.")
        create_client.add_argument("--visibility", choices=["public", "private"],
                                   help="\"private\" means that only entities in the same project as the client "
                                        "can view it. \"public\" means that any authenticated entity can view it.")
        create_client.add_argument("--project", metavar="<project ID>",
                                   help="ID representing the project this client belongs to.")
        create_client.add_argument("--required-idp", metavar="<IDP ID>",
                                   help="ID representing an Identity Provider. In order to use this client a user must"
                                        " have an identity from this IdP in their identity set.")
        create_client.add_argument("--preselect-idp", metavar="<IDP ID>",
                                   help="ID representing an Identity Provider. This preselects the given IdP on the "
                                        "Globus Auth login page if the user is not already authenticated.")
        create_client.add_argument("--terms-of-service", metavar="<URL>",
                                   help="A URL to the terms and conditions statement for this client.")
        create_client.add_argument("--privacy_policy", metavar="<URL>",
                                   help="A URL to the privacy policy for this client.")
        create_client.set_defaults(func=self.create_client)

        # delete-client parser
        delete_client = subparsers.add_parser('del-client',
                                              help="Delete a client by ID. Warning: deletes all resources associated "
                                                   "with it the client. This includes user consents, scopes (which "
                                                   "means this operation can cause other apps and services that depend "
                                                   "on those scopes to stop working as well), and any child clients "
                                                   "owned by the client (which in turn means that all resources "
                                                   "associated with the child clients would get deleted as well). "
                                                   "This action cannot be undone.")
        delete_client.add_argument("del_client_id", metavar="<client id>",
                                   help="The client ID to update, or implicitly this client's ID if not specified.")
        delete_client.set_defaults(func=self.delete_client)

        # client-fqdn parser
        client_fqdn = subparsers.add_parser('client-fqdn',
                                            help="Retrieve client information for an FQDN, "
                                                 "or add and FQDN to this client.")
        client_fqdn.add_argument("--add", action="store_true",
                                 help="Add the specified FQDN to this client ID")
        client_fqdn.add_argument("fqdn", metavar="<fqdn>",
                                 help="The fully qualified domain name to lookup or add to this client.")
        client_fqdn.set_defaults(func=self.client_fqdn)

        # token parser
        token = subparsers.add_parser('token',
                                      help="Introspect or validate an access token.")
        token.add_argument("--validate", action="store_true",
                           help="Validate the access token.")
        token.add_argument("token", metavar="<token>",
                           help="The access token to introspect (or validate).")
        token.set_defaults(func=self.token)

    def get_scopes(self, args):
        if args.scope_ids:
            return self.gau.get_scopes_by_id(args.scope_ids)
        elif args.scope_names:
            return self.gau.get_scopes_by_name(args.scope_names)
        else:
            return self.gau.list_all_scopes()

    def put_scope(self, args):
        if args.scope_id:
            return self.gau.update_scope(args.scope_config)
        else:
            return self.gau.create_scope(args.scope_config)

    def add_scopes(self, args):
        if args.parent_scope:
            return self.gau.add_dependent_scopes(args.parent_scope,
                                                 args.scope_names,
                                                 args.optional,
                                                 args.requires_refresh_token)
        else:
            return self.gau.add_scopes(args.scope_names)

    def create_scope(self, args):
        return self.gau.create_scope_with_deps(args.name,
                                               args.description,
                                               args.suffix,
                                               args.dependent_scope_names,
                                               args.advertised,
                                               args.allow_refresh_token)

    def delete_scope(self, args):
        return self.gau.delete_scope(args.scope_name)

    def get_client(self, args):
        if args.get_client_id:
            return self.gau.get_client(args.get_client_id)
        else:
            return self.gau.get_clients()

    def put_client(self, args):
        if args.create:
            return self.gau.new_client(args.client_config)
        else:
            return self.gau.update_client(args.client_config, args.put_client_id)

    def create_client(self, args):
        return self.gau.create_client(args.name,
                                      args.redirect_uris,
                                      args.public,
                                      args.visibility,
                                      args.project,
                                      args.required_idp,
                                      args.preselect_idp,
                                      args.terms_of_service,
                                      args.privacy_policy)

    def delete_client(self, args):
        return self.gau.delete_client(args.del_client_id)

    def client_fqdn(self, args):
        if args.add:
            return self.gau.add_fqdn_to_client(args.fqdn)
        else:
            return self.gau.get_client_for_fqdn(args.fqdn)

    def token(self, args):
        if args.validate:
            return self.gau.verify_access_token(args.token)
        else:
            return self.gau.introspect_access_token(args.token)

    def main(self):
        args = self.parse_cli()

        def _cmd_error_message(emsg):
            return "{prog} {subcmd}: {msg}".format(
                prog=self.parser.prog, subcmd=args.subcmd, msg=emsg)

        try:
            if not hasattr(args, 'func'):
                self.parser.print_usage()
                return 1

            self.gau = GlobusAuthUtil(**vars(args))
            response = args.func(args)
            if args.pretty:
                if isinstance(response, dict) or isinstance(response, list):
                    try:
                        print(json.dumps(response, indent=2))
                    except:
                        pprint(response)
                else:
                    pprint(response)
            else:
                print(response)
            return 0

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

# if __name__ == '__main__':
#    scope_file = sys.argv[1]
#    token = sys.argv[1]
# s = GlobusClientUtil()
# s.add_dependent_scopes('https://auth.globus.org/scopes/0fb084ec-401d-41f4-990e-e236f325010a/deriva_test_withdeps',
#                        ['openid',
#                         'email',
#                         'urn:globus:auth:scope:nexus.api.globus.org:groups',
#                         'https://auth.globus.org/scopes/identifiers.globus.org/create_update'
#                         ])
# print s.add_grant_types([
#     "openid",
#     "email",
#     "profile",
#     "urn:globus:auth:scope:auth.globus.org:view_identities",
#     "urn:globus:auth:scope:nexus.api.globus.org:groups",
#     "https://auth.globus.org/scopes/identifiers.globus.org/create_update"
# ])
#    s.add_scopes(["openid", "email"])
#    print str(s.my_scope_names())
#    print s.update_private_client()
#    pprint.pprint(s.get_scopes_by_name('email,urn:globus:auth:scope:nexus.api.globus.org:groups,urn:globus:auth:scope:transfer.api.globus.org:all'))
#    print s.create_private_client("nih_test_3", ["https://webauthn-dev.isrd.isi.edu/authn/session", "https://nih-commons.derivacloud.org/authn/session"])
#    print s.get_clients()
#    print s.add_scopes(]
#    print s.get_my_client()
#    print s.add_redirect_uris(["https://webauthn-dev.isrd.isi.edu/authn/session", "https://nih-commons.derivacloud.org/authn/session"])
#    print s.create_scope(scope_file)
#    print s.add_fqdn_to_client('nih-commons.derivacloud.org')
# print s.create_scope_with_deps('Deriva Services', 'Use Deriva Services', 'deriva_all',
#                                dependent_scopes = [
#                                    "openid",
#                                    "email",
#                                    "profile",
#                                    "urn:globus:auth:scope:auth.globus.org:view_identities",
#                                    "urn:globus:auth:scope:nexus.api.globus.org:groups",
#                                    "urn:globus:auth:scope:transfer.api.globus.org:all",
#                                    "https://auth.globus.org/scopes/identifiers.globus.org/create_update"
#                                    ])
# print s.delete_scope("https://auth.globus.org/scopes/nih-commons.derivacloud.org/deriva_test_nodeps")
# print s.delete_scope("https://auth.globus.org/scopes/0fb084ec-401d-41f4-990e-e236f325010a/deriva_test_withdeps")
# print s.delete_scope("https://auth.globus.org/scopes/nih-commons.derivacloud.org/deriva_test_withdeps")
# print s.delete_scope("https://auth.globus.org/scopes/0fb084ec-401d-41f4-990e-e236f325010a/deriva_test_3")
# print s.delete_scope("https://auth.globus.org/scopes/nih-commons.derivacloud.org/deriva_test_3")
# print s.delete_scope("https://auth.globus.org/scopes/0fb084ec-401d-41f4-990e-e236f325010a/deriva_test_4")
# print s.delete_scope("https://auth.globus.org/scopes/nih-commons.derivacloud.org/deriva_test_4")
# print(str(s.list_all_scopes()))
#    scope = s.get_scopes_by_name("https://auth.globus.org/scopes/0fb084ec-401d-41f4-990e-e236f325010a/deriva_all")[0]
#    pprint.pprint(s.get_dependent_scopes(scope))
#    print s.verify_access_token(token)
#    print s.introspect_access_token(token)
# print s.update_scope('23b9a3f9-872d-4a40-9c4c-a80a4c61f3bf',
#                      {"name" : "Use Deriva Services",
#                       "description" : "Use all Deriva services"
#                       })
# print s.update_scope('b892c8a9-2f33-4404-9fe3-6eb9093010c3',
#                      {"name" : "Use Deriva Services on nih-commons.derivacloud.org",
#                       "description" : "Use all Deriva services on nih-commons.derivacloud.org"
#                       })
