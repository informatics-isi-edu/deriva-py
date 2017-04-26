import os
import sys
from deriva_common import read_config, read_credential, format_exception
from deriva_common.base_cli import BaseCLI
from deriva_io.deriva_upload import DerivaUpload


class DerivaUploadCLI(BaseCLI):
    def __init__(self, uploader, description, epilog):
        BaseCLI.__init__(self, description, epilog)
        self.parser.add_argument("data_path", nargs="?", metavar="<dir>", help="Path to the input directory")
        self.uploader = uploader

    @staticmethod
    def upload(uploader, data_path, config_file=None, credential_file=None):
        if not issubclass(uploader, DerivaUpload):
            raise ValueError("DerivaUpload subclass required")

        if not (config_file and os.path.isfile(config_file)):
            config_file = uploader.getDefaultConfigFilePath()
        config = read_config(config_file, create_default=True, default=uploader.getDefaultConfig())
        credential = read_credential(credential_file, create_default=False)
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
                                   args.credential_file)
        except Exception as e:
            sys.stderr.write(format_exception(e))
            return 1
        finally:
            sys.stderr.write("\n\n")
        return 0
