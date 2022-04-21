import os
import datetime
import logging
from deriva.core import urlquote
from deriva.core.utils import mime_utils as mu, hash_utils as hu

PROCESSOR_PARAMS_KEY = "processor_params"
SERVICE_URL_KEY = "deriva_service_url"
SOURCE_URL_KEY = "source_url"
LOCAL_PATH_KEY = "local_path"
REMOTE_PATHS_KEY = "remote_paths"
FILE_SIZE_KEY = "size"
MD5_KEY = "md5"
SHA256_KEY = "sha256"
CONTENT_TYPE_KEY = "content_type"
IDENTIFIER_KEY = "identifier"
IDENTIFIER_LANDING_PAGE = "identifier_landing_page"

logger = logging.getLogger(__name__)


class BaseProcessor(object):
    """
    Base class for Processor classes
    """

    def __init__(self, envars=None, **kwargs):
        self.envars = envars if (envars is not None) else dict()
        self._urlencode_envars()
        self.kwargs = kwargs
        self.outputs = kwargs["inputs"]
        self.parameters = kwargs.get(PROCESSOR_PARAMS_KEY, dict()) or dict()
        self.identity = kwargs.get("identity", dict()) or dict()
        self.wallet = kwargs.get("wallet", dict()) or dict()
        self.timeout = kwargs.get("timeout", None)
        self.callback = self.parameters.get("callback", self.default_callback)
        self.last_timestamp = datetime.datetime.now()

    def _urlencode_envars(self, safe_overrides=None):
        urlencoded = dict()
        if not safe_overrides:
            safe_overrides = dict()
        for k, v in self.envars.items():
            if k.endswith("_urlencoded"):
                continue
            urlencoded[k + "_urlencoded"] = urlquote(str(v), safe_overrides.get(k, ""))
        self.envars.update(urlencoded)

    @classmethod
    def process(cls):
        raise NotImplementedError("Must be implemented by subclass")

    @staticmethod
    def create_paths(base_path, sub_path=None, filename=None, ext=None, is_bag=False, envars=None):
        relpath = sub_path if sub_path else ''
        if filename:
            relpath = ''.join([relpath, "/", filename]) if relpath else filename

        if isinstance(envars, dict):
            relpath = relpath.format(**envars)

        cur_ext = os.path.splitext(relpath)[1]
        if ext:
            ext = ext if ext.startswith(".") else "." + ext
            if not cur_ext == ext:
                relpath += ext

        abspath = os.path.abspath(
            os.path.join(base_path, 'data' if is_bag else '', relpath))

        return relpath, abspath

    @staticmethod
    def make_file_output_values(file_path, input_dict, make_file_hashes=True):
        input_dict[FILE_SIZE_KEY] = input_dict.get(FILE_SIZE_KEY, os.path.getsize(file_path))
        input_dict[CONTENT_TYPE_KEY] = input_dict.get(CONTENT_TYPE_KEY, mu.guess_content_type(file_path))
        has_file_hashes = input_dict.get(MD5_KEY) is not None and input_dict.get(SHA256_KEY) is not None
        if not has_file_hashes and make_file_hashes:
            input_dict.update(hu.compute_file_hashes(file_path, [MD5_KEY, SHA256_KEY]))

    def should_abort(self):
        try:
            if self.timeout and isinstance(self.timeout, datetime.datetime):
                now = datetime.datetime.now()
                elapsed = now - self.last_timestamp
                elapsed_time = str("Elapsed time since last timestamp: %s." % elapsed) if \
                    (elapsed > datetime.timedelta(milliseconds=0)) else ""
                if elapsed_time:
                    logging.debug(elapsed_time)
                if now > self.timeout:
                    return True
            return False
        finally:
            self.last_timestamp = datetime.datetime.now()

    def default_callback(self, **kwargs):
        progress = kwargs.get("progress", None)
        if progress and logger.isEnabledFor(logging.DEBUG):
            logger.debug(progress)
        if self.should_abort():
            return False
        return True
