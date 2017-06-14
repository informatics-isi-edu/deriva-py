import os
import sys
import traceback
import urllib.parse
from deriva_common.base_cli import BaseCLI
from deriva_io.deriva_upload import DerivaUpload


class DerivaUploadCLI(BaseCLI):
    def __init__(self, uploader, description, epilog):
        BaseCLI.__init__(self, description, epilog)
        self.parser.add_argument("data_path", nargs="?", metavar="<dir>", help="Path to the input directory")
        self.uploader = uploader

    @staticmethod
    def upload(uploader, data_path, config_file=None, credential_file=None, hostname=None):
        if not issubclass(uploader, DerivaUpload):
            raise TypeError("DerivaUpload subclass required")

        server = None
        if hostname:
            server = dict()
            if hostname.startswith("http"):
                url = urllib.parse.urlparse(hostname)
                server["protocol"] = url.scheme
                server["host"] = url.netloc
            else:
                server["protocol"] = "https"
                server["host"] = hostname

        deriva_uploader = uploader(config_file, credential_file, server)
        deriva_uploader.scanDirectory(data_path, False)
        deriva_uploader.uploadFiles()
        deriva_uploader.cleanup()

    def main(self):
        sys.stderr.write("\n")
        args = self.parse_cli()
        if args.data_path is None:
            print("\nError: Input directory not specified.\n")
            self.parser.print_usage()
            return 1

        try:
            DerivaUploadCLI.upload(self.uploader,
                                   os.path.abspath(args.data_path),
                                   args.config_file,
                                   args.credential_file,
                                   args.host)
        except:
            traceback.print_exc()
            return 1
        finally:
            sys.stderr.write("\n\n")
        return 0
