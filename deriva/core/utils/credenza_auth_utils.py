import sys
import json
import base64
import logging
import traceback
from argparse import SUPPRESS
from pprint import pprint
from requests.exceptions import HTTPError, ConnectionError
from bdbag.fetch.auth import keychain as bdbkc
from deriva.core import DEFAULT_CREDENTIAL_FILE, read_credential, write_credential, format_exception, BaseCLI, \
    get_new_requests_session, urlparse, urljoin
from deriva.core.utils import eprint


logger = logging.getLogger(__name__)

CLIENT_ID = "deriva-py"

# Default resource hint for token introspection calls when the caller
# doesn't provide any explicit --resource values. This is an umbrella resource
# and will only succeed if the token was minted to include it.
DEFAULT_SERVICE_RESOURCE = "urn:deriva:rest:service:all"

# Possible grant_type values for use with Credenza auth requests
AUTHORIZATION_CODE_GRANT    = "authorization_code"
CLIENT_CREDENTIALS_GRANT    = "client_credentials"
TOKEN_EXCHANGE_GRANT        = "urn:ietf:params:oauth:grant-type:token-exchange"
DEVICE_CODE_GRANT           = "urn:ietf:params:oauth:grant-type:device_code"

# Where we request tokens when authenticating as confidential client
TOKEN_ENDPOINT_PATH = "/authn/token"


def host_to_url(host, path="/", protocol="https"):
    if not host:
        return None
    upr = urlparse(host)
    if upr.scheme and upr.netloc:
        url = urljoin(host, path)
    else:
        url = "%s://%s%s" % (protocol, host, path if not host.endswith("/") else "")
    return url.lower()


def _add_resources(url, resources):
    """
    Append one or more ?resource=... params to URL.
    Accepts str or list[str]; returns new URL string.
    """
    if not resources:
        return url
    if isinstance(resources, str):
        resources = [resources]
    from urllib.parse import urlparse as _urlparse, parse_qsl, urlencode, urlunparse
    p = _urlparse(url)
    q = parse_qsl(p.query, keep_blank_values=True)
    q.extend([("resource", r) for r in resources if r])
    return urlunparse(p._replace(query=urlencode(q, doseq=True)))


class UsageException(ValueError):
    """Usage exception."""

    def __init__(self, message):
        super(UsageException, self).__init__(message)


class ConfidentialClient:
    """Interface for Confidential Client auth methods."""

    def name(self):
        raise NotImplementedError

    def add_cli_args(self, parser):
        """Register method-specific CLI flags on the service-token parser."""
        pass

    def prepare(self, session, form, **kwargs):
        """
        Mutate 'form' and/or 'session' headers for /authn/service/token.

        kwargs carry method-specific parameters, e.g.:
          aws_region, aws_expires
          client_id, client_secret
        """
        raise NotImplementedError


_CONFIDENTIAL_CLIENTS = {}  # name -> instance


def register_confidential_client(adapter: ConfidentialClient):
    _CONFIDENTIAL_CLIENTS[adapter.name()] = adapter


def get_confidential_client(name):
    try:
        return _CONFIDENTIAL_CLIENTS[name]
    except KeyError:
        raise UsageException(f"Unknown auth method: {name}. Available: {', '.join(sorted(_CONFIDENTIAL_CLIENTS))}")

def add_argument_once(parser, *option_strings, **kwargs):
    """
    Add an argparse option only if none of its option strings are already registered.

    This avoids conflicts when multiple plugins attempt to add the same flags.
    """
    existing = getattr(parser, "_option_string_actions", {})
    if any(opt in existing for opt in option_strings):
        return
    parser.add_argument(*option_strings, **kwargs)


class AwsPresignedClient(ConfidentialClient):
    def name(self):
        return "aws_presigned"

    def add_cli_args(self, parser):
        parser.add_argument("--aws-region", default="us-west-2",
                       help="STS signing region (default: us-west-2).")
        parser.add_argument("--aws-expires", type=int, default=60,
                       help="Presigned URL validity seconds (default: 60).")
        parser.add_argument("--client-id", help="The client ID. Defaults to %s" % CLIENT_ID, default=CLIENT_ID)

    def prepare(self, session, form, **kwargs):
        aws_region = kwargs.get("aws_region", "us-west-2")
        aws_expires = int(kwargs.get("aws_expires", 60))
        client_id = kwargs.get("client_id")
        form.append(("client_id", client_id))
        try:
            from botocore.session import get_session
            from botocore.awsrequest import AWSRequest
            from botocore.auth import SigV4QueryAuth
        except Exception as e:
            raise UsageException("AWS presign requires botocore; please install it.") from e

        sess = get_session()
        client = sess.create_client("sts", region_name=aws_region)

        url = client.generate_presigned_url(
            "get_caller_identity",
            Params={},
            ExpiresIn=aws_expires,
            HttpMethod="GET",
        )
        form.append(("subject_token", url))
        logger.debug(
            "Successfully generated AWS presigned GetCallerIdentity for client_id %s. URL: %s" % (client_id, url))

class ClientSecretBasicClient(ConfidentialClient):
    def name(self):
        return "client_secret_basic"

    def add_cli_args(self, parser):
        add_argument_once(parser, "--client-id", help="Client ID for client_secret_* methods.")
        add_argument_once(parser, "--client-secret", help="Client secret for client_secret_* methods.")

    def prepare(self, session, form, **kwargs):
        client_id = kwargs.get("client_id")
        client_secret = kwargs.get("client_secret")
        if not client_id or client_secret is None:
            raise UsageException("client_secret_basic requires client_id and client_secret.")

        b64 = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
        session.headers.update({"Authorization": f"Basic {b64}"})
        # Optional hint; server detects Basic regardless:
        form.append(("auth_method", "client_secret_basic"))


class ClientSecretPostClient(ConfidentialClient):
    def name(self):
        return "client_secret_post"

    def add_cli_args(self, parser):
        add_argument_once(parser, "--client-id", help="Client ID for client_secret_* methods.")
        add_argument_once(parser, "--client-secret", help="Client secret for client_secret_* methods.")

    def prepare(self, session, form, **kwargs):
        client_id = kwargs.get("client_id")
        client_secret = kwargs.get("client_secret")
        if not client_id or client_secret is None:
            raise UsageException("client_secret_post requires client_id and client_secret.")
        form.extend([
            ("auth_method", "client_secret_post"),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ])


# Register built-ins at import time so CLI choices are populated
register_confidential_client(AwsPresignedClient())
register_confidential_client(ClientSecretBasicClient())
register_confidential_client(ClientSecretPostClient())


class CredenzaAuthUtil:
    """
    Reusable programmatic API for Credenza auth utilities (no argparse coupling).
    """

    def __init__(self, credential_file = None):
        self.credential_file = credential_file or DEFAULT_CREDENTIAL_FILE
        self.credentials = None  # loaded lazily


    @staticmethod
    def update_bdbag_keychain(token=None, host=None, keychain_file=None, allow_redirects=False, delete=False):
        if (token is None) or (host is None):
            return
        keychain_file = keychain_file or bdbkc.DEFAULT_KEYCHAIN_FILE
        entry = {
            "uri": host_to_url(host),
            "auth_type": "bearer-token",
            "auth_params": {
                "token": token,
                "allow_redirects_with_token": True if allow_redirects else False
            }
        }
        bdbkc.update_keychain(entry, keychain_file=keychain_file, delete=delete)


    def ensure_credentials(self):
        if self.credentials is None:
            self.credentials = read_credential(self.credential_file, create_default=True)


    def load_credential(self, host):
        self.ensure_credentials()
        credential = self.credentials.get(host, self.credentials.get(host.lower()))
        if not credential:
            return None
        return credential


    def save_credential(self, host, credential=None, auth_type="user"):
        self.ensure_credentials()
        if credential is not None:
            self.credentials[host] = {"bearer-token": credential, "auth_type": auth_type}
        else:
            self.credentials.pop(host, None)
        write_credential(self.credential_file, self.credentials)


    def show_token(self, host: str):
        credential = self.load_credential(host) or {}
        return credential.get("bearer-token")


    def get_session(self, host: str, *, resources=None):
        """
        GET /authn/session with Authorization: Bearer <token>.
        Applies default resource (service umbrella) if resources is falsy.
        """
        credential = self.load_credential(host)
        if not credential:
            return None
        token = credential.get("bearer-token")

        url = host_to_url(host, "/authn/session")
        resources = resources or [DEFAULT_SERVICE_RESOURCE]
        url = _add_resources(url, resources)

        session = get_new_requests_session(url)
        session.headers.update({"Authorization": f"Bearer {token}"})
        resp = session.get(url)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 404:
            return None
        else:
            resp.raise_for_status()
            return None


    def put_session(self, host: str, *, refresh_upstream: bool = False, resources=None):
        """
        PUT /authn/session to extend the session (or refresh upstream if user session & enabled).
        Applies default resource (service umbrella) if resources is falsy.
        """
        credential = self.load_credential(host)
        if not credential:
            raise UsageException("Credential not found. Login required.")
        token = credential.get("bearer-token")

        path = "/authn/session"
        qs = "refresh_upstream=true" if refresh_upstream else ""
        url = host_to_url(host, path + (("?" + qs) if qs else ""))
        resources = resources or [DEFAULT_SERVICE_RESOURCE]
        url = _add_resources(url, resources)

        session = get_new_requests_session(url)
        session.headers.update({"Authorization": f"Bearer {token}"})
        resp = session.put(url)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 404:
            return None
        else:
            resp.raise_for_status()
            return None


    def service_login(self,
                     host: str,
                     *,
                     auth_method,
                     resources=None,
                     scope = None,
                     requested_ttl_seconds = None,
                     no_bdbag_keychain= False,
                     bdbag_keychain_file = None,
                     **method_kwargs):
        """
        Issue a service/M2M token via /authn/token.

        """
        base = host_to_url(host, TOKEN_ENDPOINT_PATH)
        session = get_new_requests_session(base)

        # Common body
        form = [("grant_type", CLIENT_CREDENTIALS_GRANT)]
        # Default to umbrella resource if caller omitted resources
        resources = resources or [DEFAULT_SERVICE_RESOURCE]
        for r in resources:
            form.append(("resource", r))
        if scope:
            form.append(("scope", scope))
        if requested_ttl_seconds is not None:
            form.append(("requested_ttl_seconds", str(int(requested_ttl_seconds))))

        # Delegate method-specific preparation
        adapter = get_confidential_client(auth_method)
        adapter.prepare(session, form, **method_kwargs)

        resp = session.post(base, data=form)
        resp.raise_for_status()
        body = resp.json()

        token = body.get("access_token")
        if token:
            self.save_credential(host, token, auth_type="service")
            if not no_bdbag_keychain:
                self.update_bdbag_keychain(host=host,
                                           token=token,
                                           keychain_file=bdbag_keychain_file or bdbkc.DEFAULT_KEYCHAIN_FILE)
        return body


class CredenzaAuthUtilCLI(BaseCLI):
    """Command-line Interface that wraps CredenzaAuthUtil (API)."""

    def __init__(self, *args, **kwargs):
        super(CredenzaAuthUtilCLI, self).__init__(*args, **kwargs)
        self.remove_options(['--host', '--token', '--oauth2-token'])
        self.parser.add_argument('--host', required=True, metavar='<host>', help="Fully qualified host name.")
        self.parser.add_argument("--pretty", "-p", action="store_true", help="Pretty-print all result output.")
        self.args = None
        self.api = None

        # init subparsers and corresponding functions
        self.subparsers = self.parser.add_subparsers(title='sub-commands', dest='subcmd')
        self.login_init()
        self.logout_init()
        self.get_session_init()
        self.put_session_init()
        self.show_token_init()
        self.service_login_init()


    def login_init(self):
        parser = self.subparsers.add_parser('login',
                                            help="Login with device flow and get tokens for resource access.")
        parser.add_argument("--no-bdbag-keychain", action="store_true",
                            help="Do not update the bdbag keychain file with result access tokens. Default false.")
        parser.add_argument('--bdbag-keychain-file', metavar='<file>',
                            help="Non-default path to a bdbag keychain file.")
        parser.add_argument("--refresh", action="store_true",
                            help="Allow the session manager to automatically refresh access tokens on the user's behalf "
                                 "until either the refresh token expires or the user logs out.")
        parser.add_argument("--force", action="store_true",
                            help="Force a login flow even if the current access token is valid.")
        parser.add_argument("--show-token", action="store_true",
                            help="Display the token from the authorization response.")
        parser.add_argument("--scope", metavar='<scope>',
                            help="Space-delimited scope string to request at device authorization time.")
        parser.add_argument("--resource", dest="resource", action="append", default=SUPPRESS,
                            metavar='<resource>',
                            help="Resource URI to request (repeatable). Server policy applies if omitted.")
        parser.set_defaults(func=self.login)


    def logout_init(self):
        parser = self.subparsers.add_parser("logout", help="Logout and revoke all access and refresh tokens.")
        parser.add_argument("--no-bdbag-keychain", action="store_true",
                            help="Do not update the bdbag keychain file by removing access tokens on logout. Default false.")
        parser.add_argument('--bdbag-keychain-file', metavar='<file>',
                            help="Non-default path to a bdbag keychain file.")
        parser.set_defaults(func=self.logout)


    def get_session_init(self):
        parser = self.subparsers.add_parser("get-session",
                                            help="Retrieve information about the current session (user or service).")
        parser.add_argument(
            "--resource",
            action="append",
            default=SUPPRESS,  # avoid auto-mixing a default with user-specified values
            help=f"Resource hint (repeatable). Defaults to {DEFAULT_SERVICE_RESOURCE} when omitted."
        )
        parser.set_defaults(func=self.get_session)


    def put_session_init(self):
        parser = self.subparsers.add_parser("put-session",
                                            help="Extend the current session (user or service).")
        parser.add_argument(
            "--resource",
            action="append",
            default=SUPPRESS,
            help=f"Resource hint (repeatable). Defaults to {DEFAULT_SERVICE_RESOURCE} when omitted."
        )
        parser.add_argument("--refresh-upstream", action="store_true",
                            help="Attempt to refresh access tokens, other dependent tokens, and claims from the "
                                 "upstream identity provider (user sessions only).")
        parser.set_defaults(func=self.put_session)


    def show_token_init(self):
        parser = self.subparsers.add_parser("show-token",
                                            help="Print access token for a given host. Use with caution.")
        parser.set_defaults(func=self.show_token)


    def service_login_init(self):
        parser = self.subparsers.add_parser(
            "service-login",
            help=f"Authenticate as a service/M2M client via {TOKEN_ENDPOINT_PATH} and store the resulting token."
        )
        parser.add_argument(
            "--resource", dest="resource", action="append", default=SUPPRESS,
            help=f"Resource hint (repeatable). Defaults to {DEFAULT_SERVICE_RESOURCE} when omitted."
        )
        parser.add_argument("--scope", help="Optional scope string (space-delimited).")
        parser.add_argument("--requested-ttl-seconds", type=int, help="Requested TTL; server may cap/deny.")
        parser.add_argument("--no-bdbag-keychain", action="store_true", help="Do not update bdbag keychain.")
        parser.add_argument("--show-token", action="store_true",
                            help="Display the token from the authorization response.")
        # Method selection
        parser.add_argument("--auth-method",
                            choices=sorted(_CONFIDENTIAL_CLIENTS),
                            required=True,
                            help="Service auth method to use.")

        # Each adapter contributes its own flags
        for adapter in _CONFIDENTIAL_CLIENTS.values():
            adapter.add_cli_args(parser)

        parser.set_defaults(func=self.service_login)


    def _api(self):
        if self.api is None:
            credential_file = (
                self.args.credential_file) if hasattr(self.args, "credential_file") else DEFAULT_CREDENTIAL_FILE
            self.api = CredenzaAuthUtil(credential_file=credential_file)
        return self.api


    def show_token(self, args):
        return self._api().show_token(args.host)


    def get_session(self, args, check_only=False):
        # Default resource if omitted
        resources = args.resource if hasattr(args, "resource") else [DEFAULT_SERVICE_RESOURCE]
        result = self._api().get_session(args.host, resources=resources)
        if not result and check_only:
            return None
        if result is None and not check_only:
            return f"No valid session found for host '{args.host}'."
        return result


    def put_session(self, args):
        resources = args.resource if hasattr(args, "resource") else [DEFAULT_SERVICE_RESOURCE]
        result = self._api().put_session(args.host, refresh_upstream=args.refresh_upstream, resources=resources)
        if result is None:
            return f"No valid session found for host '{args.host}'."
        return result


    # Device login/logout are interactive; kept in CLI wrapper for now
    def login(self, args):
        if not sys.stdin.isatty():
            raise RuntimeError("Interactive TTY required for device login.")

        if not args.force:
            resp = self.get_session(args, check_only=True)
            if resp:
                token = self.show_token(args)
                token_display = f"Bearer token: {token}" if args.show_token else ""
                return f"You are already logged in to host '{args.host}'. {token_display}"

        url = host_to_url(args.host, "/authn/device_authorization")
        session = get_new_requests_session(url)
        form = [("client_id", CLIENT_ID)]
        if args.refresh:
            form.append(("refresh", "true"))
        if getattr(args, "scope", None):
            form.append(("scope", args.scope))
        for r in getattr(args, "resource", []):
            form.append(("resource", r))
        response = session.post(url, data=form)
        response.raise_for_status()
        body = response.json()
        verification_url = body["verification_uri"]

        login_prompt = f"""

    Device login initiated to {args.host}.

    1. Please visit {verification_url} in a browser to complete authentication.
    2. After that, return here and enter "y" or "yes" at the prompt below to proceed.

        """
        sys.stdout.write(login_prompt)
        sys.stdout.flush()
        try:
            while True:
                sys.stdout.write("\nProceed? (y/N): ")
                sys.stdout.flush()
                response = sys.stdin.readline()

                ans = response.strip().lower()
                if ans in {"y", "yes"}:
                    break
                elif ans in {"n", "no", ""}:  # default is No on empty/enter
                    return f"Login cancelled for '{args.host}'."
        except KeyboardInterrupt:
            sys.stdout.write("\nCancelled by user (Ctrl-C).\n")
            return f"Login cancelled for '{args.host}'."

        token_response = session.post(
            host_to_url(args.host, TOKEN_ENDPOINT_PATH),
            data=[
                ("grant_type",  DEVICE_CODE_GRANT),
                ("client_id",   CLIENT_ID),
                ("device_code", body["device_code"]),
            ]
        )
        token_response.raise_for_status()
        body = token_response.json()
        token = body["access_token"]

        self._api().save_credential(args.host, token)
        if not args.no_bdbag_keychain:
            self._api().update_bdbag_keychain(host=args.host,
                                              token=token,
                                              keychain_file=args.bdbag_keychain_file or bdbkc.DEFAULT_KEYCHAIN_FILE)
        token_display = f"Bearer token: {token}" if args.show_token else ""
        return f"You have been successfully logged in to host '{args.host}'. {token_display}"


    def logout(self, args):
        credential = self._api().load_credential(args.host)
        if not credential:
            return "Credential not found. Not logged in."
        token = credential.get("bearer-token")

        # RFC 7009: token and client_id go in the form body; no Authorization header.
        # Public clients (like deriva-py) only need to supply client_id.
        url = host_to_url(args.host, "/authn/revoke")
        session = get_new_requests_session(url)
        response = session.post(url, data=[("token", token), ("client_id", CLIENT_ID)])
        response.raise_for_status()

        self._api().save_credential(args.host, None)
        if not args.no_bdbag_keychain:
            self._api().update_bdbag_keychain(host=args.host,
                                              token=token,
                                              delete=True,
                                              keychain_file=args.bdbag_keychain_file or bdbkc.DEFAULT_KEYCHAIN_FILE)

        return f"Successfully logged out of host '{args.host}'."


    def service_login(self, args):
        kw = vars(args).copy()
        host = kw.pop("host")
        resources = None
        if "resource" in kw:
            resources = kw.pop("resource")
        scope = kw.pop("scope", None)
        requested_ttl_seconds = kw.pop("requested_ttl_seconds", None)
        auth_method = kw.pop("auth_method")
        no_bdbag_keychain = kw.pop("no_bdbag_keychain", False)
        credential_file = kw.pop("credential_file", None)  # honored by API init, not per-call
        bdbag_keychain_file = kw.pop("bdbag_keychain_file", None)


        if credential_file and (self.api is None or self.api.credential_file != credential_file):
            self.api = CredenzaAuthUtil(credential_file)

        response = self._api().service_login(
            host,
            auth_method=auth_method,
            scope=scope,
            resources=resources,
            requested_ttl_seconds=requested_ttl_seconds,
            no_bdbag_keychain=no_bdbag_keychain,
            bdbag_keychain_file=bdbag_keychain_file,
            **kw  # remaining CLI params become method_kwargs for adapters
        )
        token = response["access_token"]
        token_type = response["token_type"]
        token_display = f"Token: {token}" if args.show_token else ""

        return f"Access token ({token_type}) granted by host '{args.host}'. {token_display}"


    def main(self):
        args = self.args = self.parse_cli()

        def _cmd_error_message(emsg):
            return "{prog} {subcmd}: {msg}".format(
                prog=self.parser.prog, subcmd=args.subcmd, msg=emsg)

        try:
            if not hasattr(args, 'func'):
                self.parser.print_usage()
                return 2

            response = args.func(args)
            if isinstance(response, dict) or isinstance(response, list):
                print(json.dumps(response, indent=2 if args.pretty else None))
            elif not isinstance(response, str):
                pprint(response)
            else:
                print(response)
            return 0

        except UsageException as e:
            eprint("{prog} {subcmd}: {msg}".format(prog=self.parser.prog, subcmd=args.subcmd, msg=e))
        except ConnectionError as e:
            eprint("{prog}: Connection error occurred: {err}".format(prog=self.parser.prog, err=format_exception(e)))
        except HTTPError as e:
            if 401 == e.response.status_code:
                msg = 'Authentication required: %s' % format_exception(e)
            elif 403 == e.response.status_code:
                msg = 'Permission denied: %s' % format_exception(e)
            else:
                msg = format_exception(e)
            eprint(_cmd_error_message(msg))
        except RuntimeError as e:
            logging.debug(format_exception(e))
            eprint('An unexpected runtime error occurred')
        except:
            eprint('An unexpected error occurred')
            traceback.print_exc()
        return 1


def main():
    desc = "Credenza Auth Utilities"
    info = "For more information see: https://github.com/informatics-isi-edu/deriva-py"
    return CredenzaAuthUtilCLI(desc, info).main()


if __name__ == '__main__':
    sys.exit(main())
