import os
import shutil
import pathlib
import logging
import re
from deriva.transfer.upload.processors.base_processor import BaseProcessor, PROCESSOR_MODIFIED_FILE_PATH_KEY

logger = logging.getLogger(__name__)


class RenameProcessor(BaseProcessor):
    """
    """
    def __init__(self, **kwargs):
        super(RenameProcessor, self).__init__(**kwargs)


class FileRenameProcessor(RenameProcessor):
    """
    Upload preprocessor that renames an input file before uploading it. Note that by default this processor does not
    modify the actual input file name on disk, but rather the associated file path metadata that is propagated to the
    catalog and object store. If physical file renaming is desired, the processor param "rename_physical_file" can be
    set to True. Depending on how this processor is configured, a physically renamed file may no longer match the scan
    criteria of the upload configuration if the upload process is run again.
    """
    def __init__(self, **kwargs):
        super(FileRenameProcessor, self).__init__(**kwargs)
        self.file_path = kwargs.get("file_path")
        self.metadata = kwargs.get("metadata")
        assert self.file_path is not None
        self.processor_params = kwargs.get("processor_params") or dict()

    def process(self):
        path_dict = dict()
        processor_output = self.kwargs.get("processor_output")
        pattern = self.processor_params.get("pattern")
        if pattern:
            path_dict.update(re.match(pattern, self.file_path).groupdict() or dict())
        repl = self.processor_params.get("replacement")
        path_dict.update(self.metadata)
        file_path = os.path.realpath(repl.format(**path_dict))
        self.metadata["file_name"] = os.path.basename(file_path)
        self.metadata["file_ext"] = "".join(pathlib.PurePath(file_path).suffixes)
        self.metadata["base_path"] = os.path.dirname(file_path)
        self.metadata["base_name"] = self.metadata["file_name"].rsplit(self.metadata["file_ext"])[0]
        if file_path != self.file_path and self.processor_params.get("rename_physical_file", False):
            shutil.move(self.file_path, file_path)
            logger.info("Renamed input file %s to: %s" % (self.file_path, file_path))
            if processor_output is not None:
                processor_output.update({PROCESSOR_MODIFIED_FILE_PATH_KEY: file_path})

        return processor_output

