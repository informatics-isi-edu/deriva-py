import os
import sys
import json
import traceback
import argparse
from deriva.transfer import GenericDownloader
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError, \
    DerivaDownloadAuthenticationError, DerivaDownloadAuthorizationError
from deriva.core import BaseCLI, KeyValuePairArgs, format_credential, urlparse, __version__


class DerivaDownloadCLI(BaseCLI):
    def __init__(self, description, epilog):

        BaseCLI.__init__(self, description, epilog, __version__)
        self.remove_options(['--host', '--config-file'])
        self.parser.add_argument("--catalog", default=1, metavar="<1>", help="Catalog number. Default: 1")
        self.parser.add_argument('host', default='localhost', metavar='<host>', help="Fully qualified host name.")
        self.parser.add_argument('config', metavar='<config file>', help="Path to a configuration file.")
        self.parser.add_argument("path", metavar="<output dir>", help="Path to an output directory.")
        self.parser.add_argument("kwargs", metavar="[key=value key=value ...]",
                                 nargs=argparse.REMAINDER, action=KeyValuePairArgs,
                                 help="Variable length of whitespace-delimited key=value pair arguments used for "
                                      "string interpolation in specific parts of the configuration file. "
                                      "For example: key1=value1 key2=value2")

    @staticmethod
    def download(
               output_path,
               hostname,
               catalog=1,
               token=None,
               envars=None,
               config_file=None,
               credential_file=None):

        assert hostname, "A hostname is required!"
        server = dict()
        server["catalog_id"] = catalog
        if hostname.startswith("http"):
            url = urlparse(hostname)
            server["protocol"] = url.scheme
            server["host"] = url.netloc
        else:
            server["protocol"] = "https"
            server["host"] = hostname

        downloader = GenericDownloader(server,
                                       output_dir=output_path,
                                       envars=envars,
                                       config_file=config_file,
                                       credential_file=credential_file)
        if token:
            downloader.setCredentials(format_credential(token))

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
            downloaded = DerivaDownloadCLI.download(output_path=os.path.abspath(args.path),
                                                    hostname=args.host,
                                                    catalog=args.catalog,
                                                    token=args.token,
                                                    envars=args.kwargs,
                                                    config_file=args.config,
                                                    credential_file=args.credential_file)
            sys.stdout.write("\n%s\n" % (json.dumps(downloaded)))
        except DerivaDownloadAuthenticationError:
            sys.stderr.write(("\n" if not args.quiet else "") +
                             "The requested service requires authentication and a valid login session could not be "
                             "found for the specified host.")
            return 1
        except (DerivaDownloadError, DerivaDownloadConfigurationError, DerivaDownloadAuthorizationError) as e:
            sys.stderr.write(("\n" if not args.quiet else "") + str(e))
            return 1
        except:
            traceback.print_exc()
            return 1
        finally:
            if not args.quiet:
                sys.stderr.write("\n\n")
        return 0
