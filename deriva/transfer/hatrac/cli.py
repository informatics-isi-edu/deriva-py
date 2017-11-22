from json.decoder import JSONDecodeError
import logging
from os.path import basename
import requests
from requests.exceptions import HTTPError
import sys
import traceback
from deriva.core import __version__ as VERSION, BaseCLI, HatracStore, HatracHashMismatch, get_credential, urlquote, \
    format_exception
from deriva.core.utils import mime_utils as mu


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
        self.remove_options(['--host', '--config-file', '--credential-file'])
        self.parser.add_argument('host', metavar='<host>', help="Fully qualified host name.")
        subparsers = self.parser.add_subparsers(title='sub-commands')

        # list parser
        ls_parser = subparsers.add_parser('list', aliases=['ls'], help="list the elements of a namespace")
        ls_parser.add_argument("resource", metavar="<path>", type=str, help="namespace path")
        ls_parser.set_defaults(func=self.list)

        # mkdir parser
        mkdir_parser = subparsers.add_parser('mkdir', help="create a namespace")
        mkdir_parser.add_argument("-p", "--parents", action="store_true",
                                  help="Create intermediate parent namespaces as required")
        mkdir_parser.add_argument("resource", metavar="<path>", type=str, help="namespace path")
        mkdir_parser.set_defaults(func=self.mkdir)

        # rmdir parser
        rmdir_parser = subparsers.add_parser('rmdir', help="remove a namespace")
        rmdir_parser.add_argument("resource", metavar="<path>", type=str, help="namespace path")
        rmdir_parser.set_defaults(func=self.rmdir)

        # getacl parser
        getacl_parser = subparsers.add_parser('getacl', help="get ACL")
        getacl_parser.add_argument("resource", metavar="<path>", type=str, help="object or namespace path")
        getacl_parser.add_argument("--access", default=None, metavar="<access-mode>",
                                   help="Optionally specify 'access' mode.")
        getacl_parser.add_argument("--role", default=None, metavar="<role>",
                                   help="Optionally specify 'role'. Must specify 'access' with this option.")
        getacl_parser.set_defaults(func=self.getacl)

        # setacl parser
        setacl_parser = subparsers.add_parser('setacl', help="set ACL")
        setacl_parser.add_argument("resource", metavar="<path>", type=str, help="object or namespace path")
        setacl_parser.add_argument("access", metavar="<access-mode>", help="access mode")
        setacl_parser.add_argument("roles", nargs='+', metavar="<role>", help="role")
        setacl_parser.add_argument("--add", action="store_true", help="add a single role to the ACL")
        setacl_parser.set_defaults(func=self.setacl)

        # detacl parser
        delacl_parser = subparsers.add_parser('delacl', help="delete ACL")
        delacl_parser.add_argument("resource", metavar="<path>", type=str, help="object or namespace path")
        delacl_parser.add_argument("access", metavar="<access-mode>", help="access mode")
        delacl_parser.add_argument("role", nargs='?', metavar="<role>", help="role")
        delacl_parser.set_defaults(func=self.delacl)

        # getobj parser
        getobj_parser = subparsers.add_parser('getobj', aliases=['get'], help="get object")
        getobj_parser.add_argument("resource", metavar="<path>", type=str, help="object path")
        getobj_parser.add_argument('outfile', metavar="<outfile>", nargs='?', type=str, help="output filename or -")
        getobj_parser.set_defaults(func=self.getobj)

        # putobj parser
        putobj_parser = subparsers.add_parser('putobj', aliases=['put'], help="put object")
        putobj_parser.add_argument('infile', metavar="<infile>", type=str, help="input filename")
        putobj_parser.add_argument("resource", metavar="<path>", type=str, help="object path")
        putobj_parser.add_argument("--content-type", metavar="<type>", type=str, help="HTTP Content-Type header value")
        putobj_parser.set_defaults(func=self.putobj)

        # delobj parser
        delobj_parser = subparsers.add_parser('delobj', aliases=['del', 'rm'], help="delete object")
        delobj_parser.add_argument("resource", metavar="<path>", type=str, help="object path")
        delobj_parser.set_defaults(func=self.delobj)

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
                print('%s: No such object or namespace' % self.resource)
                logging.debug(format_exception(e))
                return 1
            elif e.response.status_code == requests.codes.conflict:
                # this just means the namespace has no contents - ok
                logging.debug(format_exception(e))
            else:
                raise e
        except JSONDecodeError:
            print('%s: Not a namespace' % self.resource)
        return 0

    def mkdir(self, args):
        """Implements the mkdir sub-command.
        """
        try:
            self.store.create_namespace(self.resource, parents=args.parents)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                print("Parent namespace not found (use '--parents' to create parent namespace)")
                logging.debug(format_exception(e))
                return 1
            elif e.response.status_code == requests.codes.conflict:
                print("%s: Namespace exists or the parent path is not a namespace" % self.resource)
                logging.debug(format_exception(e))
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
                print('%s: No such object or namespace' % self.resource)
                logging.debug(format_exception(e))
                return 1
            elif e.response.status_code == requests.codes.conflict:
                print("%s: Namespace not empty" % self.resource)
                logging.debug(format_exception(e))
                return 1
            else:
                raise e
        return 0

    def getacl(self, args):
        """Implements the getacl sub-command.
        """
        if args.role and not args.access:
            print('Must use --access option with --role option')
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
                print('%s: No such object or namespace or ACL entry' % args.resource)
                logging.debug(format_exception(e))
            elif e.response.status_code == requests.codes.bad_request:
                print('%s: Invalid ACL name %s' % (args.resource, args.access))
                logging.debug(format_exception(e))
            else:
                raise e
            return 1

    def setacl(self, args):
        """Implements the setacl sub-command.
        """
        if args.add and len(args.roles) > 1:
            print("Option '--add' is only valid for a single role")
            return 1

        try:
            self.store.set_acl(self.resource, args.access, args.roles, args.add)
            return 0
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                print('%s: No such object or namespace' % args.resource)
                logging.debug(format_exception(e))
            elif e.response.status_code == requests.codes.bad_request:
                print('%s: Resource cannot be updated as requested' % args.resource)
                logging.debug(format_exception(e))
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
                print('%s: No such object or namespace or ACL entry' % args.resource)
                logging.debug(format_exception(e))
            elif e.response.status_code == requests.codes.bad_request:
                print('%s: Resource cannot be updated as requested' % args.resource)
                logging.debug(format_exception(e))
            else:
                raise e
            return 1

    def getobj(self, args):
        """Implements the getobj sub-command.
        """
        try:
            if args.outfile and args.outfile == '-':
                r = self.store.get_obj(self.resource)
                logging.debug('Content encoding: %s' % r.apparent_encoding)
                assert r.text, 'content cannot be read as text'
                sys.stdout.write(r.text)
            else:
                outfilename = args.outfile if args.outfile else basename(self.resource)
                self.store.get_obj(self.resource, destfilename=outfilename)
            return 0
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                print('%s: No such object' % args.resource)
                logging.debug(format_exception(e))
            else:
                raise e
            return 1

    def putobj(self, args):
        """Implements the putobj sub-command.
        """
        try:
            content_type = args.content_type if args.content_type else mu.guess_content_type(args.infile)
            loc = self.store.put_obj(self.resource, args.infile, headers={"Content-Type": content_type})
            print(loc)
            return 0
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                print('%s: Parent path does not exit' % args.resource)
                logging.debug(format_exception(e))
            elif e.response.status_code == requests.codes.conflict:
                # this just means the object may have once existed
                print('%s: Cannot create object (parent path is not a namespace or object name is in use)' %
                      args.resource)
                logging.debug(format_exception(e))
            else:
                raise e
            return 1

    def delobj(self, args):
        """Implements the delobj sub-command.
        """
        try:
            self.store.del_obj(self.resource)
            return 0
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                print('%s: No such object' % args.resource)
                logging.debug(format_exception(e))
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
                print('Authentication required')
                logging.debug(format_exception(e))
            elif e.response.status_code == requests.codes.forbidden:
                print('Permission denied')
                logging.debug(format_exception(e))
            else:
                print(format_exception(e))
        except HatracHashMismatch as e:
            print('Checksum verification failed: %s' % format_exception(e))
        except RuntimeError as e:
            print(format_exception(e))
        except Exception:
            traceback.print_exc()
        return 1


def main():
    DESC = "Deriva Hatrac Command-Line Interface"
    INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-py"
    return DerivaHatracCLI(DESC, INFO).main()


if __name__ == '__main__':
    sys.exit(main())
