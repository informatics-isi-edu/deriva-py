import logging
import requests
from requests.exceptions import HTTPError
import sys
import traceback
from deriva.core import __version__ as VERSION, BaseCLI, HatracStore, get_credential, urlquote


class DerivaHatracCLI (BaseCLI):
    """Deriva Hatrac Command-line Interface.
    """
    def __init__(self, description, epilog):
        """Initializes the CLI.
        """
        BaseCLI.__init__(self, description, epilog, VERSION)

        # initialized after argument parsing
        self.resource = None
        self.store = None

        # parent arg parser
        self.parser.add_argument("--token", default=None, metavar="<auth-token>", help="Authorization bearer token.")
        self.remove_options(['--host'])
        self.parser.add_argument('host', metavar='<host>', help="Fully qualified host name.")
        subparsers = self.parser.add_subparsers(title='sub-commands')

        # list parser
        ls_parser = subparsers.add_parser('list', aliases=['ls'], help="list contents of a hatrac namespace")
        ls_parser.add_argument("resource", metavar="<namespace>", type=str, help="namespace")
        ls_parser.set_defaults(func=self.list)

        # mkdir parser
        mkdir_parser = subparsers.add_parser('mkdir', help="make a hatrac resource")
        mkdir_parser.add_argument("-p", "--parents", action="store_true",
                                  help="Create intermediate parent namespaces as required")
        mkdir_parser.add_argument("resource", metavar="<namespace>", type=str, help="namespace")
        mkdir_parser.set_defaults(func=self.mkdir)

        # rmdir parser
        rmdir_parser = subparsers.add_parser('rmdir', help="remove a hatrac namespace")
        rmdir_parser.add_argument("resource", metavar="<namespace>", type=str, help="namespace")
        rmdir_parser.set_defaults(func=self.rmdir)

        # getacl parser
        getacl_parser = subparsers.add_parser('getacl', help="get ACL")
        getacl_parser.add_argument("resource", metavar="<resource-name>", type=str, help="object or namespace")
        getacl_parser.add_argument("--access", default=None, metavar="<access-mode>",
                                   help="Optionally specify 'access' mode.")
        getacl_parser.add_argument("--role", default=None, metavar="<role>",
                                   help="Optionally specify 'role'. Must specify 'access' with this option.")
        getacl_parser.set_defaults(func=self.getacl)

        # setacl parser
        setacl_parser = subparsers.add_parser('setacl', help="set ACL")
        setacl_parser.add_argument("resource", metavar="<resource-name>", type=str, help="object or namespace")
        setacl_parser.add_argument("access", metavar="<access-mode>", help="access mode")
        setacl_parser.add_argument("roles", nargs='+', metavar="<role>", help="role")
        setacl_parser.add_argument("--add", action="store_true", help="add a single role to the ACL")
        setacl_parser.set_defaults(func=self.setacl)

        # detacl parser
        delacl_parser = subparsers.add_parser('delacl', help="delete ACL")
        delacl_parser.add_argument("resource", metavar="<resource-name>", type=str, help="object or namespace")
        delacl_parser.add_argument("access", metavar="<access-mode>", help="access mode")
        delacl_parser.add_argument("role", nargs='?', metavar="<role>", help="role")
        delacl_parser.set_defaults(func=self.delacl)

    @staticmethod
    def _get_credential(host_name, token=None):
        if token:
            return {"cookie": "webauthn=%s" % token}
        else:
            return get_credential(host_name)

    def _post_parser_init(self, args):
        """Shared initialization for all sub-commands.
        """
        self.resource = urlquote(args.resource, '/')
        self.store = HatracStore('https', args.host, DerivaHatracCLI._get_credential(args.host, args.token))

    def list(self, args):
        """Implements the list sub-command.
        """
        try:
            namespaces = self.store.retrieve_namespace(self.resource)
            for name in namespaces:
                print(name)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                print('%s: no such object or namespace.' % self.resource)
                logging.debug(e)
                return 1
            elif e.response.status_code == requests.codes.conflict:
                # this just means the namespace has no contents - ok
                logging.debug(e)
            else:
                raise e
        return 0

    def mkdir(self, args):
        """Implements the mkdir sub-command.
        """
        try:
            self.store.create_namespace(self.resource, parents=args.parents)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                print("Parent namespace not found. Use '--parents' to create parent namespace.")
                logging.debug(e)
                return 1
            elif e.response.status_code == requests.codes.conflict:
                print("%s: namespace exists or the parent path is not a namespace." % self.resource)
                logging.debug(e)
                return 1
            else:
                raise e
        return 0

    def rmdir(self, args):
        """Implements the mkdir sub-command.
        """
        try:
            self.store.delete_namespace(self.resource)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                print('%s: no such object or namespace.' % self.resource)
                logging.debug(e)
                return 1
            elif e.response.status_code == requests.codes.conflict:
                print("%s: namespace not empty." % self.resource)
                logging.debug(e)
                return 1
            else:
                raise e
        return 0

    def getacl(self, args):
        """Implements the getacl sub-command.
        """
        if args.role and not args.access:
            print('Must use --access option with --role option.')
            return 1

        try:
            acls = self.store.get_acl(self.resource, args.access, args.role)
            for access in acls:
                print("%s:" % access)
                for role in acls.get(access, []):
                    print("  %s" % role)
            return 0
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                print('%s: no such object or namespace or ACL entry.' % args.resource)
                logging.debug(e)
            elif e.response.status_code == requests.codes.bad_request:
                print('%s: invalid ACL name %s.' % (args.resource, args.access))
                logging.debug(e)
            else:
                raise e
            return 1

    def setacl(self, args):
        """Implements the setacl sub-command.
        """
        if args.add and len(args.roles) > 1:
            print("Option '--add' is only valid for a single role.")
            return 1

        try:
            self.store.set_acl(self.resource, args.access, args.roles, args.add)
            return 0
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                print('%s: no such object or namespace.' % args.resource)
                logging.debug(e)
            elif e.response.status_code == requests.codes.bad_request:
                print('%s: resource cannot be updated as requested.' % args.resource)
                logging.debug(e)
            else:
                raise e
            return 1

    def delacl(self, args):
        """Implements the getacl sub-command.
        """
        try:
            self.store.del_acl(self.resource, args.access, args.role)
            return 0
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                print('%s: no such object or namespace or ACL entry.' % args.resource)
                logging.debug(e)
            elif e.response.status_code == requests.codes.bad_request:
                print('%s: resource cannot be updated as requested.' % args.resource)
                logging.debug(e)
            else:
                raise e
            return 1

    def main(self):
        """Main routine of the CLI.
        """
        args = self.parse_cli()
        try:
            if not hasattr(args, 'func'):
                self.parser.print_usage()
                return 1
            self._post_parser_init(args)
            return args.func(args)
        except HTTPError as e:
            if e.response.status_code == requests.codes.unauthorized:
                print('Authentication required.')
                logging.debug(e)
                return 1
            elif e.response.status_code == requests.codes.forbidden:
                print('Permission denied.')
                logging.debug(e)
                return 1
            else:
                raise e
        except RuntimeError:
            return 1
        except Exception:
            traceback.print_exc()
            return 1


def main():
    DESC = "Deriva Hatrac Command-Line Interface"
    INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-py"
    return DerivaHatracCLI(DESC, INFO).main()


if __name__ == '__main__':
    sys.exit(main())
