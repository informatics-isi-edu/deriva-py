import os
from deriva.core import read_config, write_config, __version__ as VERSION
from deriva.transfer import DerivaUpload


class GenericUploader(DerivaUpload):

    def __init__(self, config_file=None, credential_file=None, server=None):
        DerivaUpload.__init__(self, config_file, credential_file, server)

    @classmethod
    def getVersion(cls):
        return VERSION

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


