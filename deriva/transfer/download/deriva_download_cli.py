import os
import sys
import traceback
import argparse
from deriva.transfer import GenericDownloader
from deriva.core import BaseCLI, KeyValuePairArgs, urlparse, __version__


class DerivaDownloadCLI(BaseCLI):
    def __init__(self, description, epilog):

        BaseCLI.__init__(self, description, epilog, __version__)
        self.remove_options(['--host', '--config-file'])
        self.parser.add_argument("--catalog", default=1, metavar="<1>", help="Catalog number. Default: 1")
        self.parser.add_argument("--token", metavar="<auth-token>", help="Authorization bearer token.")
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
               kwargs=None,
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
                                       kwargs=kwargs,
                                       config_file=config_file,
                                       credential_file=credential_file)
        if token:
            auth_token = {"cookie": "webauthn=%s" % token}
            downloader.setCredentials(auth_token)
        downloader.download()

    def main(self):
        try:
            args = self.parse_cli()
        except ValueError as e:
            sys.stderr.write(str(e))
            return 2
        if not args.quiet:
            sys.stderr.write("\n")

        try:
            DerivaDownloadCLI.download(os.path.abspath(args.path),
                                       args.host,
                                       args.catalog,
                                       args.token,
                                       args.kwargs,
                                       args.config,
                                       args.credential_file)
        except RuntimeError as e:
            sys.stderr.write(("\n" if not args.quiet else "") + str(e))
            return 1
        except:
            traceback.print_exc()
            return 1
        finally:
            if not args.quiet:
                sys.stderr.write("\n\n")
        return 0
