import os
import sys
import json
import traceback
import argparse
import requests
from requests.exceptions import HTTPError, ConnectionError
from deriva.transfer import GenericDownloader
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError, \
    DerivaDownloadAuthenticationError, DerivaDownloadAuthorizationError, DerivaDownloadTimeoutError, \
    DerivaDownloadBaggingError
from deriva.core import BaseCLI, KeyValuePairArgs, format_credential, format_exception, urlparse


class DerivaDownloadCLI(BaseCLI):
    def __init__(self, description, epilog, **kwargs):

        BaseCLI.__init__(self, description, epilog, **kwargs)
        self.parser.add_argument("--catalog", default=1, metavar="<1>", help="Catalog number. Default: 1")
        self.parser.add_argument("--timeout", metavar="<seconds>",
                                 help="Total number of seconds elapsed before the download is aborted.")
        self.parser.add_argument("output_dir", metavar="<output dir>", help="Path to an output directory.")
        self.parser.add_argument("envars", metavar="[key=value key=value ...]",
                                 nargs=argparse.REMAINDER, action=KeyValuePairArgs, default={},
                                 help="Variable length of whitespace-delimited key=value pair arguments used for "
                                      "string interpolation in specific parts of the configuration file. "
                                      "For example: key1=value1 key2=value2")

    @classmethod
    def get_downloader(cls, *args, **kwargs):
        return GenericDownloader(*args, dcctx_cid="cli/" + DerivaDownloadCLI.__name__, **kwargs)

    @classmethod
    def download(cls, args):

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

        downloader = cls.get_downloader(server, **vars(args))

        return downloader.download()

    def main(self):
        try:
            args = self.parse_cli()
        except ValueError as e:
            sys.stderr.write(str(e))
            return 2
        if not args.quiet:
            sys.stderr.write("\n")

        try:
            try:
                downloaded = self.download(args)
                sys.stdout.write("\n%s\n" % (json.dumps(downloaded)))
            except ConnectionError as e:
                raise DerivaDownloadError("Connection error occurred. %s" % format_exception(e))
            except HTTPError as e:
                if e.response.status_code == requests.codes.unauthorized:
                    raise DerivaDownloadAuthenticationError(
                        "The requested service requires authentication and a valid login session could "
                        "not be found for the specified host. Server responded: %s" % e)
                elif e.response.status_code == requests.codes.forbidden:
                    raise DerivaDownloadAuthorizationError(
                        "A requested operation was forbidden. Server responded: %s" % e)
        except (DerivaDownloadError, DerivaDownloadConfigurationError, DerivaDownloadAuthenticationError,
                DerivaDownloadAuthorizationError, DerivaDownloadTimeoutError, DerivaDownloadBaggingError) as e:
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
