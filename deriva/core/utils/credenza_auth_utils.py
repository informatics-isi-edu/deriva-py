import sys
import json
import logging
import traceback
from pprint import pprint
from requests.exceptions import HTTPError, ConnectionError
from bdbag.fetch.auth import keychain as bdbkc
from deriva.core import DEFAULT_CREDENTIAL_FILE, read_credential, write_credential, format_exception, BaseCLI, \
    get_new_requests_session, urlparse, urljoin
from deriva.core.utils import eprint

logger = logging.getLogger(__name__)


def host_to_url(host, path="/", protocol="https"):
    if not host:
        return None
    upr = urlparse(host)
    if upr.scheme and upr.netloc:
        url = urljoin(host, path)
    else:
        url = "%s://%s%s" % (protocol, host, path if not host.endswith("/") else "")
    return url.lower()

class UsageException(ValueError):
    """Usage exception.
    """
    def __init__(self, message):
        """Initializes the exception.
        """
        super(UsageException, self).__init__(message)

class CredenzaAuthUtilCLI(BaseCLI):
    """CredenzaAuthUtil Command-line Interface.
    """
    def __init__(self, *args, **kwargs):
        super(CredenzaAuthUtilCLI, self).__init__(*args, **kwargs)
        self.remove_options(['--token', '--oauth2-token'])
        self.parser.add_argument("--pretty", "-p", action="store_true",
                                 help="Pretty-print all result output.")
        self.credentials = None

        # init subparsers and corresponding functions
        self.subparsers = self.parser.add_subparsers(title='sub-commands', dest='subcmd')
        self.login_init()
        self.logout_init()
        self.get_session_init()
        self.put_session_init()

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

    def load_credential(self, host, credential_file=None):
        if not self.credentials:
            self.credentials = read_credential(credential_file or DEFAULT_CREDENTIAL_FILE, create_default=True)
        credential = self.credentials.get(host, self.credentials.get(host.lower()))
        if not credential:
            return None
        return credential.get("bearer-token")

    def save_credential(self, host, credential_file=None, credential=None):
        if not self.credentials:
            self.credentials = read_credential(credential_file or DEFAULT_CREDENTIAL_FILE, create_default=True)

        if credential is not None:
            self.credentials[host] = {"bearer-token": credential}
        else:
            self.credentials.pop(host, None)

        write_credential(credential_file or DEFAULT_CREDENTIAL_FILE, self.credentials)

    def get_session(self, args, check_only=False):
        credential = self.load_credential(args.host, args.credential_file)
        if not credential:
            return None if check_only else "Credential not found. Login required."

        url = host_to_url(args.host, "/authn/session")
        session = get_new_requests_session(url)
        session.headers.update({"Authorization": f"Bearer {credential}"})
        response = session.get(url)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None if check_only else f"No valid session found for host '{args.host}'."
        else:
            response.raise_for_status()
            return None

    def put_session(self, args):
        credential = self.load_credential(args.host, args.credential_file)
        if not credential:
            return "Credential not found. Login required."

        path = "/authn/session"
        if args.refresh_upstream:
            path += "?refresh_upstream=true"
        url = host_to_url(args.host, path)
        session = get_new_requests_session(url)
        session.headers.update({"Authorization": f"Bearer {credential}"})
        response = session.put(url)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return f"No valid session found for host '{args.host}'."
        else:
            response.raise_for_status()
            return None

    def login(self, args):

        if not sys.stdin.isatty():
            raise RuntimeError("Interactive TTY required for device login.")

        if not args.force:
            resp = self.get_session(args, check_only=True)
            if resp:
                return f"You are already logged in to host '{args.host}'"

        path = "/authn/device/start"
        if args.refresh:
            path += "?refresh=true"
        url = host_to_url(args.host, path)
        session = get_new_requests_session(url)
        response = session.post(url)
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
            f"https://{args.host}/authn/device/token",
            json={"device_code": body["device_code"]}
        )
        token_response.raise_for_status()
        body = token_response.json()
        token = body["access_token"]
        self.save_credential(args.host, args.credential_file, token)
        if not args.no_bdbag_keychain:
            self.update_bdbag_keychain(host=args.host,
                                       token=token,
                                       keychain_file=args.bdbag_keychain_file or bdbkc.DEFAULT_KEYCHAIN_FILE)
        token_display = f"Access token: {token}" if args.show_token else ""
        return f"You have been successfully logged in to host '{args.host}'. {token_display}"

    def logout(self, args):
        credential = self.load_credential(args.host, args.credential_file)
        if not credential:
            return "Credential not found. Not logged in."

        url = host_to_url(args.host, "/authn/device/logout")
        session = get_new_requests_session(url)
        session.headers.update({"Authorization": f"Bearer {credential}"})
        response = session.post(url)
        response.raise_for_status()

        self.save_credential(args.host, args.credential_file)
        if not args.no_bdbag_keychain:
            self.update_bdbag_keychain(host=args.host,
                                       token=credential,
                                       delete=True,
                                       keychain_file=args.bdbag_keychain_file or bdbkc.DEFAULT_KEYCHAIN_FILE)

        return f"You have been successfully logged out of host '{args.host}'."

    def login_init(self):
        parser = self.subparsers.add_parser('login',
                                            help="Login with Globus Auth and get OAuth tokens for resource access.")

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
                                            help="Retrieve information about the currently logged-in user.")
        parser.set_defaults(func=self.get_session)

    def put_session_init(self):
        parser = self.subparsers.add_parser("put-session",
                                            help="Extend the current logged-in user's session.")
        parser.add_argument("--refresh-upstream", action="store_true",
                            help="Attempt to refresh access tokens, other dependent tokens, and claims from the "
                                 "upstream identity provider.")
        parser.set_defaults(func=self.put_session)

    def main(self):
        args = self.parse_cli()

        def _cmd_error_message(emsg):
            return "{prog} {subcmd}: {msg}".format(
                prog=self.parser.prog, subcmd=args.subcmd, msg=emsg)

        try:
            if not hasattr(args, 'func'):
                self.parser.print_usage()
                return 2

            if args.subcmd == "login" or args.subcmd == "logout" or args.subcmd == "session":
                pass
            else:
                pass

            response = args.func(args)
            if args.pretty:
                if isinstance(response, dict) or isinstance(response, list):
                    try:
                        print(json.dumps(response, indent=2))
                        return 0
                    except:
                        pprint(response)
                        return 0
                elif not isinstance(response, str):
                    pprint(response)
                    return 0
            print(response)
            return 0

        except UsageException as e:
            eprint("{prog} {subcmd}: {msg}".format(prog=self.parser.prog, subcmd=args.subcmd, msg=e))
        except ConnectionError as e:
            eprint("{prog}: Connection error occurred".format(prog=self.parser.prog))
        except HTTPError as e:
            if 401 == e.response.status_code:
                msg = 'Authentication required: %s' % format_exception(e)
            elif 403 == e.response.status_code:
                msg = 'Permission denied: %s' % format_exception(e)
            else:
                msg = e
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