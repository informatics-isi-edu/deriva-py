import logging
import os
from os.path import basename
import requests
from requests.exceptions import HTTPError, ConnectionError
import sys
import traceback
from deriva.core import __version__ as VERSION, BaseCLI, DerivaPathError, HatracStore, HatracHashMismatch, \
    get_credential, format_credential, format_exception, DEFAULT_CHUNK_SIZE
from deriva.core.utils import eprint, mime_utils as mu


class DerivaHatracCLIException (Exception):
    """Base exception class for DerivaHatracCli.
    """
    def __init__(self, message):
        """Initializes the exception.
        """
        super(DerivaHatracCLIException, self).__init__(message)


class UsageException (DerivaHatracCLIException):
    """Usage exception.
    """
    def __init__(self, message):
        """Initializes the exception.
        """
        super(UsageException, self).__init__(message)


class ResourceException (DerivaHatracCLIException):
    """Remote resource exception.
    """
    def __init__(self, message, cause):
        """Initializes the exception.
        """
        super(ResourceException, self).__init__(message)
        self.cause = cause


class DerivaHatracCLI (BaseCLI):
    """Deriva Hatrac Command-line Interface.
    """
    def __init__(self, description, epilog):
        """Initializes the CLI.
        """
        super(DerivaHatracCLI, self).__init__(description, epilog, VERSION)

        # initialized after argument parsing
        self.args = None
        self.host = None
        self.resource = None
        self.store = None

        # parent arg parser
        self.remove_options(['--config-file', '--credential-file'])
        subparsers = self.parser.add_subparsers(title='sub-commands', dest='subcmd')

        # list parser
        ls_parser = subparsers.add_parser('ls', help="list the elements of a namespace")
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
        getobj_parser = subparsers.add_parser('get', help="get object")
        getobj_parser.add_argument("resource", metavar="<path>", type=str, help="object path")
        getobj_parser.add_argument('outfile', metavar="<outfile>", nargs='?', type=str, help="output filename or -")
        getobj_parser.set_defaults(func=self.getobj)

        # putobj parser
        putobj_parser = subparsers.add_parser('put', help="put object")
        putobj_parser.add_argument('infile', metavar="<infile>", type=str, help="input filename")
        putobj_parser.add_argument("resource", metavar="<path>", type=str, help="object path")
        putobj_parser.add_argument("--content-type", metavar="<type>", type=str, help="HTTP Content-Type header value")
        putobj_parser.add_argument("--chunk-size", metavar="<bytes>", type=int, default=DEFAULT_CHUNK_SIZE,
                                   help="Chunk size in bytes")
        putobj_parser.add_argument("--parents", action="store_true",
                                   help="Create intermediate parent namespaces as required")
        putobj_parser.set_defaults(func=self.putobj)

        # delobj parser
        delobj_parser = subparsers.add_parser('del', help="delete object")
        delobj_parser.add_argument("resource", metavar="<path>", type=str, help="object path")
        delobj_parser.set_defaults(func=self.delobj)

    @staticmethod
    def _get_credential(host_name, token=None, oauth2_token=None):
        if token or oauth2_token:
            return format_credential(token=token, oauth2_token=oauth2_token)
        else:
            return get_credential(host_name)

    def _post_parser_init(self, args):
        """Shared initialization for all sub-commands.
        """
        self.host = args.host if args.host else 'localhost'
        self.resource = args.resource
        self.store = HatracStore('https', args.host, DerivaHatracCLI._get_credential(self.host,
                                                                                     token=args.token,
                                                                                     oauth2_token=args.oauth2_token))

    def list(self, args):
        """Implements the list sub-command.
        """
        try:
            namespaces = self.store.retrieve_namespace(self.resource)
            for name in namespaces:
                print(name)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('No such object or namespace', e)
            elif e.response.status_code != requests.codes.conflict:
                # 'conflict' just means the namespace has no contents - ok
                raise e
        except ValueError as e:
            raise ResourceException('Not a namespace', e)

    def mkdir(self, args):
        """Implements the mkdir sub-command.
        """
        try:
            self.store.create_namespace(self.resource, parents=args.parents)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException("Parent namespace not found (use '--parents' to create parent namespace)", e)
            elif e.response.status_code == requests.codes.conflict:
                raise ResourceException("Namespace exists or the parent path is not a namespace", e)
            else:
                raise e

    def rmdir(self, args):
        """Implements the mkdir sub-command.
        """
        try:
            self.store.delete_namespace(self.resource)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('No such object or namespace', e)
            elif e.response.status_code == requests.codes.conflict:
                raise ResourceException("Namespace not empty", e)
            else:
                raise e

    def getacl(self, args):
        """Implements the getacl sub-command.
        """
        if args.role and not args.access:
            raise UsageException("Must use '--access' option with '--role' option")

        try:
            acls = self.store.get_acl(self.resource, args.access, args.role)
            for access in acls:
                print("%s:" % access)
                for role in acls.get(access, []):
                    print("  %s" % role)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('No such object or namespace or ACL entry', e)
            elif e.response.status_code == requests.codes.bad_request:
                raise ResourceException('Invalid ACL name %s' % args.access, e)
            else:
                raise e

    def setacl(self, args):
        """Implements the setacl sub-command.
        """
        if args.add and len(args.roles) > 1:
            raise UsageException("Option '--add' is only valid for a single role")

        try:
            self.store.set_acl(self.resource, args.access, args.roles, args.add)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('No such object or namespace', e)
            elif e.response.status_code == requests.codes.bad_request:
                raise ResourceException('Resource cannot be updated as requested', e)
            else:
                raise e

    def delacl(self, args):
        """Implements the getacl sub-command.
        """
        try:
            self.store.del_acl(self.resource, args.access, args.role)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('No such object or namespace or ACL entry', e)
            elif e.response.status_code == requests.codes.bad_request:
                raise ResourceException('Resource cannot be updated as requested', e)
            else:
                raise e

    def getobj(self, args):
        """Implements the getobj sub-command.
        """
        try:
            if args.outfile and args.outfile == '-':
                r = self.store.get_obj(self.resource)
                logging.debug('Content encoding: %s' % r.apparent_encoding)
                assert r.content, 'content cannot be read as bytes'  # never expected from the requests API
                os.write(sys.stdout.fileno(), r.content)
            else:
                outfilename = args.outfile if args.outfile else basename(self.resource)
                self.store.get_obj(self.resource, destfilename=outfilename)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('No such object', e)
            else:
                raise e

    def putobj(self, args):
        """Implements the putobj sub-command.
        """
        try:
            content_type = args.content_type if args.content_type else mu.guess_content_type(args.infile)
            file_size = os.path.getsize(args.infile)
            loc = self.store.put_loc(
                self.resource,
                args.infile,
                content_type=content_type,
                chunked=True if file_size > args.chunk_size else False,
                chunk_size=args.chunk_size,
                create_parents=args.parents)
            print(loc)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException("Parent namespace not found (use '--parents' to create parent namespace)", e)
            elif e.response.status_code == requests.codes.conflict:
                raise ResourceException(
                    'Cannot create object (parent path is not a namespace or object name is in use)', e)
            else:
                raise e

    def delobj(self, args):
        """Implements the delobj sub-command.
        """
        try:
            self.store.del_obj(self.resource)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('No such object', e)
            else:
                raise e

    def main(self):
        """Main routine of the CLI.
        """
        args = self.parse_cli()

        def _resource_error_message(emsg):
            return "{prog} {subcmd}: {resource}: {msg}".format(
                prog=self.parser.prog, subcmd=args.subcmd, resource=args.resource, msg=emsg)

        try:
            if not hasattr(args, 'func'):
                self.parser.print_usage()
                return 1

            self._post_parser_init(args)
            args.func(args)
            return 0
        except UsageException as e:
            eprint("{prog} {subcmd}: {msg}".format(prog=self.parser.prog, subcmd=args.subcmd, msg=e))
        except ConnectionError as e:
            eprint("{prog}: Connection error occurred".format(prog=self.parser.prog))
        except DerivaPathError as e:
            eprint(e)
        except HTTPError as e:
            if e.response.status_code == requests.codes.unauthorized:
                msg = 'Authentication required'
            elif e.response.status_code == requests.codes.forbidden:
                msg = 'Permission denied'
            else:
                msg = e
            logging.debug(format_exception(e))
            eprint(_resource_error_message(msg))
        except ResourceException as e:
            logging.debug(format_exception(e.cause))
            eprint(_resource_error_message(e))
        except HatracHashMismatch as e:
            logging.debug(format_exception(e))
            eprint(_resource_error_message('Checksum verification failed'))
        except RuntimeError as e:
            logging.debug(format_exception(e))
            eprint('Unexpected runtime error occurred')
        except:
            eprint('Unexpected error occurred')
            traceback.print_exc()
        return 1


def main():
    DESC = "DERIVA HATRAC Command-Line Interface"
    INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-py"
    return DerivaHatracCLI(DESC, INFO).main()


if __name__ == '__main__':
    sys.exit(main())
