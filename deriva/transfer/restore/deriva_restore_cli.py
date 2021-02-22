import os
import sys
import json
import traceback
import argparse
import requests
from requests.exceptions import HTTPError, ConnectionError
from deriva.core import BaseCLI, KeyValuePairArgs, format_credential, format_exception, urlparse
from deriva.transfer import DerivaRestore, DerivaRestoreError, DerivaRestoreConfigurationError, \
    DerivaRestoreAuthenticationError, DerivaRestoreAuthorizationError


class DerivaRestoreCLI(BaseCLI):
    def __init__(self, description, epilog, **kwargs):

        BaseCLI.__init__(self, description, epilog, **kwargs)
        self.parser.add_argument("--catalog", metavar="<1>", help="Catalog number. If a catalog number is not "
                                                                  "specified, a new catalog will be created.")
        self.parser.add_argument("input_path", metavar="<input_path>", help="Path to backup file or directory.")
        mutex_group = self.parser.add_mutually_exclusive_group()
        mutex_group.add_argument("--no-data", action="store_true",
                                 help="Do not restore table data, restore schema only.")
        mutex_group.add_argument("--no-schema", action="store_true",
                                 help="Do not restore schema, restore data only.")
        self.parser.add_argument("--no-assets", action="store_true",
                                 help="Do not restore asset data, if present.")
        self.parser.add_argument("--no-annotations", action="store_true",
                                 help="Do not restore annotations.")
        self.parser.add_argument("--no-policy", action="store_true",
                                 help="Do not restore access policy and ACLs.")
        self.parser.add_argument("--no-bag-materialize", action="store_true",
                                 help="If the input format is a bag, do not materialize prior to restore.")
        self.parser.add_argument("--weak-bag-validation", action="store_true",
                                 help="If the input format is a bag, "
                                      "do not abort the restore if the bag fails validation.")
        self.parser.add_argument("--exclude-object", type=lambda s: [item.strip() for item in s.split(',')],
                                 metavar="<schema>, <schema:table>, ...",
                                 help="List of comma-delimited schema-name and/or schema-name/table-name to "
                                      "exclude from the restore process, in the form <schema> or <schema:table>.")
        self.parser.add_argument("--exclude-data", type=lambda s: [item.strip() for item in s.split(',')],
                                 metavar="<schema>, <schema:table>, ...",
                                 help="List of comma-delimited schema-name and/or schema-name/table-name to "
                                      "exclude from the restore process, in the form <schema> or <schema:table>.")
        self.parser.add_argument("envars", metavar="[key=value key=value ...]",
                                 nargs=argparse.REMAINDER, action=KeyValuePairArgs, default={},
                                 help="Variable length of whitespace-delimited key=value pair arguments used for "
                                      "populating the processing environment with parameters for keyword substitution."
                                      "For example: key1=value1 key2=value2")

    def main(self):
        try:
            args = self.parse_cli()
        except ValueError as e:
            sys.stderr.write(str(e))
            return 2
        if not args.quiet:
            sys.stderr.write("\n")

        try:
            assert args.host, "A hostname is required!"
            server = dict()
            server["catalog_id"] = args.catalog
            if args.host.startswith("http"):
                url = urlparse(args.host)
                server["protocol"] = url.scheme
                server["host"] = url.netloc
            else:
                server["protocol"] = "https"
                server["host"] = args.host

            restorer = DerivaRestore(server, **vars(args), dcctx_cid="cli/" + self.__class__.__name__)
            try:
                restorer.restore()
            except ConnectionError as e:
                raise DerivaRestoreError("Connection error occurred. %s" % format_exception(e))
            except HTTPError as e:
                if e.response.status_code == requests.codes.unauthorized:
                    raise DerivaRestoreAuthenticationError(
                        "The requested service requires authentication and a valid login session could "
                        "not be found for the specified host. Server responded: %s" % e)
                elif e.response.status_code == requests.codes.forbidden:
                    raise DerivaRestoreAuthorizationError(
                        "A requested operation was forbidden. Server responded: %s" % e)
                raise DerivaRestoreError(format_exception(e))
        except (DerivaRestoreError, DerivaRestoreConfigurationError,
                DerivaRestoreAuthenticationError, DerivaRestoreAuthorizationError) as e:
            sys.stderr.write(("\n" if not args.quiet else "") + format_exception(e))
            if args.debug:
                traceback.print_exc()
            return 1
        except:
            sys.stderr.write("An unexpected error occurred.")
            traceback.print_exc()
            return 1
        finally:
            if not args.quiet:
                sys.stderr.write("\n\n")
        return 0
