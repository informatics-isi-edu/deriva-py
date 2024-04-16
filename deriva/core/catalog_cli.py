import logging
import os
import sys
import requests
import traceback
from pprint import pp
from requests.exceptions import HTTPError, ConnectionError
from deriva.core import __version__ as VERSION, BaseCLI, KeyValuePairArgs, DerivaServer, DerivaPathError, \
    get_credential, format_credential, format_exception, DEFAULT_HEADERS
from deriva.core.ermrest_model import nochange
from deriva.core.utils import eprint


class DerivaCatalogCLIException (Exception):
    """Base exception class for DerivaHatracCli.
    """
    def __init__(self, message):
        """Initializes the exception.
        """
        super(DerivaCatalogCLIException, self).__init__(message)


class UsageException (DerivaCatalogCLIException):
    """Usage exception.
    """
    def __init__(self, message):
        """Initializes the exception.
        """
        super(UsageException, self).__init__(message)


class ResourceException (DerivaCatalogCLIException):
    """Remote resource exception.
    """
    def __init__(self, message, cause):
        """Initializes the exception.
        """
        super(ResourceException, self).__init__(message)
        self.cause = cause


class DerivaCatalogCLI (BaseCLI):
    """Deriva Hatrac Command-line Interface.
    """
    def __init__(self, description, epilog):
        """Initializes the CLI.
        """
        super(DerivaCatalogCLI, self).__init__(description, epilog, VERSION)

        # initialized after argument parsing
        self.args = None
        self.host = None
        self.protocol = None
        self.catalog = None

        # parent arg parser
        self.remove_options(['--config-file', '--credential-file'])
        self.parser.add_argument("-p", "--protocol", choices=["http", "https"], default='https',
                                 help="transport protocol: 'http' or 'https'")
        subparsers = self.parser.add_subparsers(title='sub-commands', dest='subcmd')

        # exists parser
        exists_parser = subparsers.add_parser('exists', help="Check if catalog exists.")
        exists_parser.add_argument("id", metavar="<id>", type=str, help="Catalog ID")
        exists_parser.set_defaults(func=self.catalog_exists)

        # create parser
        create_parser = subparsers.add_parser('create', help="Create a new catalog.")
        create_parser.add_argument("--id", metavar="<id>", type=str, help="Catalog ID")
        create_parser.add_argument("-o", "--owner", metavar="<owner> <owner> ...",
                                   nargs="+", help="List of quoted user or group identifier strings.")
        create_parser.add_argument("-a", "--auto-configure", action="store_true",
                                   help="Configure the new catalog with a set of baseline defaults")
        create_parser.add_argument("--configure-args", metavar="[key=value key=value ...]",
                                   nargs='+', action=KeyValuePairArgs, default={},
                                   help="Variable length of whitespace-delimited key=value pair arguments used for "
                                        "controlling automatic catalog configuration settings. "
                                        "For example: --configure-args key1=value1 key2=value2")
        create_parser.set_defaults(func=self.catalog_create)

        # get parser
        get_parser = subparsers.add_parser('get', help="Send a HTTP GET request to the catalog.")
        get_parser.add_argument("id", metavar="<id>", type=str, help="Catalog ID")
        get_parser.add_argument("path", metavar="<request-path>", help="The ERMRest API path.")
        get_parser.add_argument("-o", "--output-file", metavar="<output file path>", help="Path to output file.")
        get_parser.add_argument("-f", "--output-format", choices=["json", "json-stream", "csv"], default="json",
                                help="The output file format. Defaults to 'json'")
        get_parser.add_argument("-a", "--auto-delete", action="store_true",
                                help="Automatically delete output file if no results are returned.")
        get_parser.add_argument("--headers", metavar="[key=value key=value ...]",
                                nargs='+', action=KeyValuePairArgs, default={},
                                help="Variable length of whitespace-delimited key=value pair arguments used for "
                                     "specifying or overriding HTTP Request Headers. "
                                     "For example: --headers key1=value1 key2=value2")
        get_parser.set_defaults(func=self.catalog_get)

        # put parser
        put_parser = subparsers.add_parser('put', help="Send a HTTP PUT request to the catalog.")
        put_parser.add_argument("id", metavar="<id>", type=str, help="Catalog ID")
        put_parser.add_argument("path", metavar="<request-path>", help="The ERMRest API path.")
        put_parser.add_argument("input-file", metavar="<input file path>",
                                help="Path to an input file containing the request message body.")
        put_parser.add_argument("-f", "--input-format", choices=["json", "json-stream", "csv"], default="json",
                                help="The input file format. Defaults to 'json'")
        put_parser.add_argument("--headers", metavar="[key=value key=value ...]",
                                nargs='+', action=KeyValuePairArgs, default={},
                                help="Variable length of whitespace-delimited key=value pair arguments used for "
                                     "specifying or overriding HTTP Request Headers. "
                                     "For example: --headers key1=value1 key2=value2")
        put_parser.set_defaults(func=self.catalog_put)

        # post parser
        post_parser = subparsers.add_parser('post', help="Send a HTTP POST request to the catalog.")
        post_parser.add_argument("id", metavar="<id>", type=str, help="Catalog ID")
        post_parser.add_argument("path", metavar="<request-path>", help="The ERMRest API path.")
        post_parser.add_argument("input-file", metavar="<input file path>",
                                 help="Path to an input file containing the request message body.")
        post_parser.add_argument("-f", "--input-format", choices=["json", "json-stream", "csv"], default="json",
                                 help="The input file format. Defaults to 'json'")
        post_parser.add_argument("--headers", metavar="[key=value key=value ...]",
                                 nargs='+', action=KeyValuePairArgs, default={},
                                 help="Variable length of whitespace-delimited key=value pair arguments used for "
                                      "specifying or overriding HTTP Request Headers. "
                                      "For example: --headers key1=value1 key2=value2")
        post_parser.set_defaults(func=self.catalog_post)

        # delete parser
        del_parser = subparsers.add_parser('delete', help="Send a HTTP DELETE request to the catalog. "
                                                          "Use the 'drop' command to delete the entire catalog.")
        del_parser.add_argument("id", metavar="<id>", type=str, help="Catalog ID")
        del_parser.add_argument("path", metavar="<request-path>", help="The ERMRest API path.")
        del_parser.add_argument("--headers", metavar="[key=value key=value ...]",
                                nargs='+', action=KeyValuePairArgs, default={},
                                help="Variable length of whitespace-delimited key=value pair arguments used for "
                                     "specifying or overriding HTTP Request Headers. "
                                     "For example: --headers key1=value1 key2=value2")
        del_parser.set_defaults(func=self.catalog_delete)

        # drop parser
        drop_parser = subparsers.add_parser('drop', help="Delete a catalog.")
        drop_parser.add_argument("id", metavar="<id>", type=str, help="Catalog ID")
        drop_parser.set_defaults(func=self.catalog_drop)

        # clone parser
        clone_parser = subparsers.add_parser('clone', help="Clone a source catalog to a new destination catalog.")
        clone_parser.add_argument("id", metavar="<id>", type=str, help="Catalog ID")
        clone_parser.add_argument("--no-copy-data", action="store_false",
                                  help="Do not copy table contents.")
        clone_parser.add_argument("--no-copy-annotations", action="store_false",
                                  help="Do not copy annotations.")
        clone_parser.add_argument("--no-copy-policy", action="store_false",
                                  help="Do not copy ACL (Access Control List) policies.")
        clone_parser.add_argument("--no-truncate-after", action="store_false",
                                  help="Do not truncate destination history after cloning.")
        clone_parser.add_argument("--exclude-schemas", metavar="<schema-name> <schema-name> ...",
                                  nargs="+", help="List of schema names to exclude from the cloning process.")
        clone_parser.set_defaults(func=self.catalog_clone)

        # create_alias parser
        create_alias_parser = subparsers.add_parser('create-alias', help="Create a new catalog alias")
        create_alias_parser.add_argument("--id", metavar="<id>", type=str, help="The alias id.")
        create_alias_parser.add_argument("-t", "--alias-target", metavar="<alias>", help="The target catalog id.")
        create_alias_parser.add_argument("-o", "--owner", metavar="<owner> <owner> ...",
                                         nargs="+", help="List of quoted user or group identifier strings.")
        create_alias_parser.set_defaults(func=self.catalog_alias_create)

        # get_alias parser
        get_alias_parser = subparsers.add_parser('get-alias', help="Get catalog alias metadata")
        get_alias_parser.add_argument("id", metavar="<id>", type=str, help="The alias id.")
        get_alias_parser.set_defaults(func=self.catalog_alias_get)

        # update_alias parser
        update_alias_parser = subparsers.add_parser('update-alias', help="Update an existing catalog alias")
        update_alias_parser.add_argument("--id", metavar="<id>", type=str, help="The alias id.")
        update_alias_parser.add_argument("-t", "--alias-target", metavar="<alias>", nargs='?', default=nochange, const=None,
                                         help="The target catalog id. If specified without a catalog id as an argument "
                                              "value, the existing alias target will be cleared ")
        update_alias_parser.add_argument("-o", "--owner", metavar="<owner> <owner> ...", nargs='+', default=nochange,
                                         help="List of quoted user or group identifier strings.")
        update_alias_parser.set_defaults(func=self.catalog_alias_update)

        # delete_alias parser
        del_alias_parser = subparsers.add_parser('delete-alias', help="Delete a catalog alias.")
        del_alias_parser.add_argument("id", metavar="<id>", type=str, help="The alias id.")
        del_alias_parser.set_defaults(func=self.catalog_alias_delete)

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
        self.protocol = args.protocol
        self.id = args.id
        self.server = DerivaServer(self.protocol,
                                   args.host,
                                   credentials=DerivaCatalogCLI._get_credential(
                                       self.host,
                                       token=args.token,
                                       oauth2_token=args.oauth2_token))

    @staticmethod
    def _decorate_headers(headers, file_format, method="get"):

        header_format_map = {
            "json": "application/json",
            "json-stream": "application/x-json-stream",
            "csv": "text/csv"
        }

        format_type = header_format_map.get(file_format)
        if format_type is None:
            raise UsageException("Unsupported format: %s" % file_format)
        if str(method).lower() in ["get", "head"]:
            headers["accept"] = format_type
        elif str(method).lower() in ["post", "put"]:
            headers["content-type"] = format_type
        else:
            raise UsageException("Unsupported method: %s" % method)

    def catalog_exists(self, args):
        """Implements the catalog_exists sub-command.
        """
        catalog = self.server.connect_ermrest(self.id)
        pp(catalog.exists())

    def catalog_create(self, args):
        """Implements the catalog_create sub-command.
        """
        try:
            if args.id and self.server.connect_ermrest(args.id).exists():
                print("Catalog already exists")
                return
            owner = args.owner if args.owner else None
            catalog = self.server.create_ermrest_catalog(args.id, owner)
            if args.auto_configure:
                model = catalog.getCatalogModel()
                model.configure_baseline_catalog(**args.configure_args)
            if not args.quiet:
                print("Created new catalog %s with the following default configuration:\n" % catalog.catalog_id)
                pp(catalog.get('/').json())
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('Catalog not found', e)
            elif e.response.status_code == requests.codes.conflict:
                raise ResourceException("Catalog already exists", e)
            else:
                raise e

    def catalog_clone(self, args):
        """Implements the catalog_clone sub-command.
        """
        try:
            catalog = self.server.connect_ermrest(args.id)
            print("Attempting to clone catalog %s into new catalog. Please wait..." % args.id)
            dest_cat = catalog.clone_catalog(copy_data=args.no_copy_data,
                                             copy_annotations=args.no_copy_annotations,
                                             copy_policy=args.no_copy_policy,
                                             truncate_after=args.no_truncate_after,
                                             exclude_schemas=args.exclude_schemas)
            print("Catalog successfully cloned into new catalog: %s" % dest_cat.catalog_id)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('Catalog not found', e)
            else:
                raise e

    def catalog_get(self, args):
        """Implements the catalog_get sub-command.
        """
        headers = DEFAULT_HEADERS.copy()
        headers.update(args.headers)
        self._decorate_headers(headers, args.output_format)
        catalog = self.server.connect_ermrest(args.id)
        try:
            if args.output_file:
                catalog.getAsFile(args.path,
                                  destfilename=args.output_file,
                                  headers=headers,
                                  delete_if_empty=args.auto_delete)
            else:
                pp(catalog.get(args.path, headers=headers).json())
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('Catalog not found', e)
        except:
            if args.output_file and os.path.isfile(args.output_file):
                logging.info("Deleting empty file: %s" % args.output_file)
                os.remove(args.output_file)
            raise

    def catalog_put(self, args):
        """Implements the catalog_put sub-command.
        """
        headers = DEFAULT_HEADERS.copy()
        headers.update(args.headers)
        self._decorate_headers(headers, args.input_format, "put")
        try:
            catalog = self.server.connect_ermrest(args.id)
            with open(args.input_file, "rb") as input_file:
                resp = catalog.put(args.path, data=input_file, headers=headers)
                if not args.quiet:
                    pp(resp.json())
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('Catalog not found', e)
            else:
                raise e

    def catalog_post(self, args):
        """Implements the catalog_post sub-command.
        """
        headers = DEFAULT_HEADERS.copy()
        headers.update(args.headers)
        self._decorate_headers(headers, args.input_format, "post")
        try:
            catalog = self.server.connect_ermrest(args.id)
            with open(args.input_file, "rb") as input_file:
                resp = catalog.post(args.path, data=input_file, headers=headers)
                if not args.quiet:
                    pp(resp.json())
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('Catalog not found', e)
            else:
                raise e

    def catalog_delete(self, args):
        """Implements the catalog_delete sub-command.
        """
        headers = DEFAULT_HEADERS.copy()
        headers.update(args.headers)
        try:
            catalog = self.server.connect_ermrest(args.id)
            catalog.delete(args.path, headers)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('Catalog not found', e)
            else:
                raise e

    def catalog_drop(self, args):
        """Implements the catalog_drop sub-command.
        """
        try:
            catalog = self.server.connect_ermrest(args.id)
            catalog.delete_ermrest_catalog(really=True)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('Catalog not found', e)
            else:
                raise e

    def catalog_alias_create(self, args):
        """Implements the catalog_alias_create sub-command.
        """
        try:
            if args.id:
                alias = self.server.connect_ermrest_alias(args.id)
                try:
                    if alias.retrieve():
                        print("Catalog alias already exists")
                        return
                except requests.HTTPError as e:
                    if e.response.status_code == 404:
                        pass
                    else:
                        raise
            owner = args.owner if args.owner else None
            alias = self.server.create_ermrest_alias(args.id, owner, args.alias_target)
            if not args.quiet:
                print("Created new catalog alias %s with the following configuration:\n" % alias.alias_id)
                pp(alias.retrieve())
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('Catalog alias not found', e)
            elif e.response.status_code == requests.codes.conflict:
                raise ResourceException("Catalog alias already exists", e)
            else:
                raise

    def catalog_alias_get(self, args):
        """Implements the catalog_alias_get sub-command.
        """
        try:
            alias = self.server.connect_ermrest_alias(args.id)
            response = alias.retrieve()
            if not args.quiet:
                pp(response)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('Catalog alias not found', e)
            else:
                raise e

    def catalog_alias_update(self, args):
        try:
            owner = args.owner if args.owner else None
            alias = self.server.connect_ermrest_alias(args.id)
            response = alias.update(owner, args.alias_target, args.id)
            print("Updated catalog alias %s with the following configuration:\n" % alias.alias_id)
            pp(response)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('Catalog alias not found', e)
            elif e.response.status_code == requests.codes.conflict:
                raise ResourceException("Catalog alias already exists", e)
            else:
                raise

    def catalog_alias_delete(self, args):
        """Implements the catalog_alias_delete sub-command.
        """
        try:
            alias = self.server.connect_ermrest_alias(args.id)
            alias.delete_ermrest_alias(really=True)
        except HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise ResourceException('Catalog alias not found', e)
            else:
                raise e

    def main(self):
        """Main routine of the CLI.
        """
        args = self.parse_cli()

        def _resource_error_message(emsg):
            return "{prog} {subcmd}: {id}: {msg}".format(
                prog=self.parser.prog, subcmd=args.subcmd, id=args.id, msg=emsg)

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
        except RuntimeError as e:
            logging.warning(format_exception(e))
            eprint('Unexpected runtime error occurred')
        except:
            eprint('Unexpected error occurred')
            traceback.print_exc()
        return 1


def main():
    DESC = "DERIVA Catalog Utility Command-Line Interface"
    INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-py"
    return DerivaCatalogCLI(DESC, INFO).main()


if __name__ == '__main__':
    sys.exit(main())
