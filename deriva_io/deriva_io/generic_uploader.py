import os
import sys
from deriva_common import read_config, write_config
import deriva_io
from deriva_io.deriva_upload import DerivaUpload
from deriva_io.deriva_upload_cli import DerivaUploadCLI

DESC = "Deriva Data Upload Utility"
INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-py/deriva_io"


class GenericUploader(DerivaUpload):

    def __init__(self, config_file=None, credential_file=None, server=None):
        DerivaUpload.__init__(self, config_file, credential_file, server)

    @classmethod
    def getVersion(cls):
        return deriva_io.__version__

    @classmethod
    def getConfigPath(cls):
        return "~/.deriva/upload/"

    @classmethod
    def getServers(cls):
        return read_config(os.path.join(
            cls.getDeployedConfigPath(), cls.DefaultServerListFileName), create_default=True, default=[])

    @classmethod
    def setServers(cls, servers):
        return write_config(os.path.join(cls.getDeployedConfigPath(), cls.DefaultServerListFileName), servers)


def main():
    cli = DerivaUploadCLI(GenericUploader, DESC, INFO)
    cli.main()


if __name__ == '__main__':
    sys.exit(main())
