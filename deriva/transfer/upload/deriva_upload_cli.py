import os
import sys
import traceback
from deriva.transfer import DerivaUpload
from deriva.core import BaseCLI, write_config, format_credential, urlparse


class DerivaUploadCLI(BaseCLI):
    def __init__(self, uploader, description, epilog):
        if not issubclass(uploader, DerivaUpload):
            raise TypeError("DerivaUpload subclass required")

        BaseCLI.__init__(self, description, epilog, uploader.getVersion(), hostname_required=True)
        self.parser.add_argument('--no-config-update', action="store_true",
                                 help="Do not check for (and download) an updated configuration from the server.")
        self.parser.add_argument("--catalog", default=1, metavar="<1>", help="Catalog number. Default: 1")
        self.parser.add_argument("path", metavar="<dir>", help="Path to an input directory.")
        self.uploader = uploader

    @staticmethod
    def upload(uploader,
               data_path,
               hostname,
               catalog=1,
               token=None,
               config_file=None,
               credential_file=None,
               no_update=False):

        if not issubclass(uploader, DerivaUpload):
            raise TypeError("DerivaUpload subclass required")

        assert hostname
        server = dict()
        server["catalog_id"] = catalog
        if hostname.startswith("http"):
            url = urlparse(hostname)
            server["protocol"] = url.scheme
            server["host"] = url.netloc
        else:
            server["protocol"] = "https"
            server["host"] = hostname

        deriva_uploader = uploader(config_file, credential_file, server)
        if token:
            deriva_uploader.setCredentials(format_credential(token))
        if not config_file and not no_update:
            config = deriva_uploader.getUpdatedConfig()
            if config:
                write_config(deriva_uploader.getDeployedConfigFilePath(), config)
        if not deriva_uploader.isVersionCompatible():
            raise RuntimeError("Version incompatibility detected", "Current version: [%s], required version(s): %s." % (
                deriva_uploader.getVersion(), deriva_uploader.getVersionCompatibility()))
        deriva_uploader.scanDirectory(data_path, False)
        deriva_uploader.uploadFiles(file_callback=deriva_uploader.defaultFileCallback)
        deriva_uploader.cleanup()

    def main(self):
        sys.stderr.write("\n")
        args = self.parse_cli()
        if args.path is None:
            sys.stderr.write("\nError: Input directory not specified.\n")
            self.parser.print_usage()
            return 2

        try:
            DerivaUploadCLI.upload(self.uploader,
                                   os.path.abspath(args.path),
                                   args.host,
                                   args.catalog,
                                   args.token,
                                   args.config_file,
                                   args.credential_file,
                                   args.no_config_update)
        except RuntimeError as e:
            sys.stderr.write(str(e))
            return 1
        except:
            traceback.print_exc()
            return 1
        finally:
            sys.stderr.write("\n\n")
        return 0
