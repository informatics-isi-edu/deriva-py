import os
import errno

LOCAL_PATH_KEY = "local_path"
REMOTE_URLS_KEY = "remote_urls"
IDENTIFIER_KEY = "identifier"


class BaseProcessor(object):
    """
    Base class for Processor classes
    """

    def __init__(self, envars=None, **kwargs):
        self.kwargs = kwargs
        self.envars = envars if (envars is not None) else dict()

    @classmethod
    def process(cls):
        raise NotImplementedError("Must be implemented by subclass")

    @staticmethod
    def create_paths(base_path, sub_path=None, ext='', is_bag=False, envars=None):
        relpath = sub_path if sub_path else ''
        if not os.path.splitext(sub_path)[1][1:]:
            relpath += ext
        if isinstance(envars, dict):
            relpath = relpath.format(**envars)

        abspath = os.path.abspath(
            os.path.join(base_path, 'data' if is_bag else '', relpath))

        return relpath, abspath

    @staticmethod
    def make_dirs(path):
        if not os.path.isdir(path):
            try:
                os.makedirs(path)
            except OSError as error:
                if error.errno != errno.EEXIST:
                    raise

