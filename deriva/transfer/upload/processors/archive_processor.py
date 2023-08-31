import logging
from deriva.transfer.upload.processors import BaseProcessor
from bdbag import bdbag_api as bdb

logger = logging.getLogger(__name__)


class ArchiveProcessor(BaseProcessor):
    """
    """
    def __init__(self, **kwargs):
        super(ArchiveProcessor, self).__init__(**kwargs)


class BagArchiveProcessor(ArchiveProcessor):
    """
    Upload preprocessor that takes an input directory and creates a BDBag archive from it.
    """
    def __init__(self, **kwargs):
        super(BagArchiveProcessor, self).__init__(**kwargs)
        self.file_path = kwargs.get("file_path")
        assert self.file_path is not None
        self.processor_params = kwargs.get("processor_params", dict())

    def process(self):
        bdb.make_bag(self.file_path)
        archive_file = bdb.archive_bag(self.file_path, self.processor_params.get("format", "zip"))
        bdb.revert_bag(self.file_path)
        processor_output = self.kwargs.get("processor_output")
        if processor_output is not None:
            processor_output.update({"modified_file_path": archive_file})
        return processor_output

