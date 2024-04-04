PROCESSOR_NAME_KEY = "processor"
PROCESSOR_TYPE_KEY = "processor_type"
PROCESSOR_PARAMS_KEY = "processor_params"
PRE_PROCESSORS_KEY = "pre_processors"
POST_PROCESSORS_KEY = "post_processors"
PROCESSOR_MODIFIED_FILE_PATH_KEY = "processor_modified_file_path"
PROCESSOR_REQUIRES_METADATA_QUERY_KEY = "requires_metadata_query"


class BaseProcessor(object):
    """
    Base class for upload Processor classes
    """

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.parameters = kwargs.get(PROCESSOR_PARAMS_KEY, dict()) or dict()

    @classmethod
    def process(cls):
        raise NotImplementedError("Must be implemented by subclass")


