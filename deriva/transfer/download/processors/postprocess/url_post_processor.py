import logging
from deriva.transfer.download import DerivaDownloadConfigurationError, DerivaDownloadError
from deriva.transfer.download.processors.base_processor import *

logger = logging.getLogger(__name__)

INPUT_PATHS_KEY = 'input_paths'
REMOTE_PATH_KEY = 'remote_path'
OUTPUT_KEY = 'output'
OUTPUT_URLENCODED_KEY = 'output_urlencoded'
OUTPUT_URL_KEY = 'output_url'
OUTPUT_URL_URLENCODED_KEY = 'output_url_urlencoded'


class UrlRewritePostProcessor(BaseProcessor):
    """Post processor that rewrites the output remote paths.
    
    This can be used for scenarios where the client should be directed to a secondary service that consumes the export
    service's output file.

    Service params:
      `input_paths`: a list of input path names which will be post processed.
      `remote_path`: the remote path pattern formatted with the following env.

    Formatting environment:
      - copy of all `envars` in the current environment, plus
      - `output`: the current output key
      - `output_url`: the url to the current output formed by `deriva_service_url` + `/` + `output`
      - and `..._urlencoded` variants of the above using the deriva urlquote() function.
    """

    def __init__(self, envars=None, **kwargs):
        super(UrlRewritePostProcessor, self).__init__(envars, **kwargs)

        # get remote path pattern
        self.remote_path_pattern = self.parameters.get(REMOTE_PATH_KEY)
        if not self.remote_path_pattern:
            raise DerivaDownloadConfigurationError(
                    "%s is missing required parameter '%s' from %s" %
                    (self.__class__.__name__, REMOTE_PATH_KEY, PROCESSOR_PARAMS_KEY))
        logger.debug("remote path pattern: {}".format(self.remote_path_pattern))

        # get input paths
        self.input_paths = self.parameters.get(INPUT_PATHS_KEY)
        if not self.input_paths:
            raise DerivaDownloadConfigurationError(
                    "%s is missing required parameter '%s' from %s" %
                    (self.__class__.__name__, INPUT_PATHS_KEY, PROCESSOR_PARAMS_KEY))

    def process(self):
        env = self.envars.copy()
        for input_path in self.input_paths:
            k = input_path
            v = self.outputs.get(k)
            if not v:
                raise DerivaDownloadError(
                    "%s is missing required input path '%s'" % (self.__class__.__name__, k))
            env[OUTPUT_KEY] = k
            env[OUTPUT_URLENCODED_KEY] = urlquote(k)
            env[OUTPUT_URL_KEY] = self.envars.get(SERVICE_URL_KEY, '') + '/' + k
            env[OUTPUT_URL_URLENCODED_KEY] = urlquote(env[OUTPUT_URL_KEY])
            v[REMOTE_PATHS_KEY] = [self.remote_path_pattern.format(**env)]

        return self.outputs
