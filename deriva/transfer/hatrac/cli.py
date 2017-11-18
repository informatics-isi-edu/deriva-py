import logging
import requests
from requests.exceptions import HTTPError
import sys
import traceback
from deriva.core import __version__ as VERSION, BaseCLI, HatracStore, get_credential, urlquote

if sys.version_info > (3,):
    from urllib.parse import urlparse
else:
    from urlparse import urlparse


class DerivaHatracCLI (BaseCLI):
    """Deriva Hatrac Command-line Interface.
    """
    def __init__(self, description, epilog):
        """Initializes the CLI.
        """
        BaseCLI.__init__(self, description, epilog, VERSION)
        self.parser.add_argument("--token", default=None, metavar="<auth-token>", help="Authorization bearer token.")
        self.remove_options(['--host'])
        self.parser.add_argument('host', metavar='<host>', help="Fully qualified host name.")
        subparsers = self.parser.add_subparsers(title='sub-commands')
        ls_parser = subparsers.add_parser('list', aliases=['ls'], help="list contents of a hatrac namespace")
        ls_parser.add_argument("namespace", metavar="<namespace>", type=str, help="namespace")
        ls_parser.set_defaults(func=self.list)

    def _get_credential(self, host_name, token=None):
        if token:
            return {"cookie": "webauthn=%s" % token}
        else:
            return get_credential(host_name)

    def list(self, args):
        """Implements the list sub-command.
        """
        host_name = args.host
        namespace = urlquote(args.namespace, '/')
        credentials = self._get_credential(host_name, args.token)
        store = HatracStore('https', host_name, credentials)
        try:
            namespaces = store.retrieve_namespace(namespace)
            for name in namespaces:
                print(name)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                print('%s not found' % (namespace,))
                logging.debug(e)
            elif e.response.status_code == requests.codes.conflict:
                # this just means the namespace has no contents - ok
                logging.debug(e)
            else:
                raise e

    def main(self):
        """Main routine of the CLI.
        """
        args = self.parse_cli()
        try:
            if not hasattr(args, 'func'):
                self.parser.print_usage()
                return 1
            args.func(args)
        except RuntimeError:
            return 1
        except Exception:
            traceback.print_exc()
            return 1
        return 0


def main():
    DESC = "Deriva Hatrac Command-Line Interface"
    INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-py"
    return DerivaHatracCLI(DESC, INFO).main()


if __name__ == '__main__':
    sys.exit(main())
