import os
import pathlib
import logging
from deriva.core import stob
from deriva.transfer.upload.processors.base_processor import BaseProcessor, PROCESSOR_MODIFIED_FILE_PATH_KEY
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
        self.metadata = kwargs.get("metadata")
        assert self.file_path is not None
        self.processor_params = kwargs.get("processor_params") or dict()

    def process(self):
        processor_output = self.kwargs.get("processor_output")
        idempotent_archive = stob(self.processor_params.get("idempotent_archive", True))
        bdb.make_bag(self.file_path, update=True, idempotent=idempotent_archive)
        try:
            archive_file = (
                bdb.archive_bag(self.file_path,
                                self.processor_params.get("format", "zip"),
                                idempotent=idempotent_archive))
            self.metadata["file_name"] = os.path.basename(archive_file)
            self.metadata["file_size"] = os.path.getsize(archive_file)
            self.metadata["base_path"] = os.path.dirname(archive_file)
            self.metadata["file_ext"] = "".join(pathlib.PurePath(archive_file).suffixes)
            self.metadata["base_name"] = self.metadata["file_name"].rsplit(self.metadata["file_ext"])[0]
            if processor_output is not None:
                processor_output.update({PROCESSOR_MODIFIED_FILE_PATH_KEY: archive_file})
        except:
            raise
        finally:
            bdb.revert_bag(self.file_path)
        return processor_output

