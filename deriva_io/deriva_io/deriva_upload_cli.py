import os
import sys
from deriva_common import read_config, copy_config, read_credential, format_exception
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

        if not (config_file and os.path.isfile(config_file)):
            config_file = uploader.getDeployedConfigFilePath(uploader)
            if not (config_file and os.path.isfile(config_file)):
                copy_config(uploader.getDefaultConfigFilePath(uploader), config_file)
        config = read_config(config_file)
        if hostname:
            config['server']['host'] = hostname
        credential = read_credential(credential_file)
        deriva_uploader = uploader.getInstance(config, credential)
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
        except Exception as e:
            sys.stderr.write(format_exception(e))
            return 1
        finally:
            sys.stderr.write("\n\n")
        return 0
