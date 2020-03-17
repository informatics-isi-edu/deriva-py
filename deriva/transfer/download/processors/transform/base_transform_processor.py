import os
from deriva.core import stob
from deriva.core.utils.mime_utils import guess_content_type
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.base_processor import BaseProcessor, \
    LOCAL_PATH_KEY, FILE_SIZE_KEY, SOURCE_URL_KEY
from bdbag import bdbag_ro as ro


class BaseTransformProcessor(BaseProcessor):
    """
    Base class for TransformProcessor classes
    """

    def __init__(self, envars=None, **kwargs):
        super(BaseTransformProcessor, self).__init__(envars, **kwargs)
        self.base_path = kwargs["base_path"]
        self.input_paths = self.parameters.get("input_paths", [])
        if not self.input_paths:
            self.input_paths = [self.parameters["input_path"]]  # for backward compatibility
        self.sub_path = self.parameters.get("output_path")
        self.output_filename = self.parameters.get("output_filename")
        self.is_bag = kwargs.get("bag", False)
        self.transformed_output = self.outputs.get(self.input_path, dict())
        self.url = self.transformed_output.get(SOURCE_URL_KEY)
        self.ro_file_provenance = stob(self.parameters.get("ro_file_provenance", False if not self.is_bag else True))
        self.ro_manifest = self.kwargs.get("ro_manifest")
        self.ro_author_name = self.kwargs.get("ro_author_name")
        self.ro_author_orcid = self.kwargs.get("ro_author_orcid")
        self.delete_input = stob(self.parameters.get("delete_input", True))
        self.input_relpaths = []
        self.input_abspaths = []
        self.output_relpath = None
        self.output_abspath = None

    @property
    def input_path(self):  # for backward compatibility
        return self.input_paths[0]

    @property
    def input_relpath(self):  # for backward compatibility
        return self.input_relpaths[0]

    @property
    def input_abspath(self):  # for backward compatibility
        return self.input_abspaths[0]

    def _create_input_output_paths(self):
        for input_path in self.input_paths:
            relpath, abspath = self.create_paths(self.base_path,
                                                 sub_path=input_path,
                                                 is_bag=self.is_bag,
                                                 envars=self.envars)
            self.input_relpaths.append(relpath)
            self.input_abspaths.append(abspath)
        self.output_relpath, self.output_abspath = self.create_paths(self.base_path,
                                                                     sub_path=self.sub_path,
                                                                     filename=self.output_filename,
                                                                     is_bag=self.is_bag,
                                                                     envars=self.envars)

    def _delete_input(self):
        for input_abspath in self.input_abspaths:
            if os.path.isfile(input_abspath):
                os.remove(input_abspath)
        for input_relpath in self.input_relpaths:
            del self.outputs[input_relpath]

    def process(self):
        if self.ro_manifest and self.ro_file_provenance:
            ro.add_file_metadata(self.ro_manifest,
                                 source_url=self.url,
                                 local_path=self.output_relpath,
                                 media_type=guess_content_type(self.output_abspath),
                                 retrieved_on=ro.make_retrieved_on(),
                                 retrieved_by=ro.make_retrieved_by(self.ro_author_name, orcid=self.ro_author_orcid),
                                 bundled_as=ro.make_bundled_as())
        if self.delete_input:
            self._delete_input()

        self.outputs.update({self.output_relpath: {LOCAL_PATH_KEY: self.output_abspath,
                                                   FILE_SIZE_KEY: os.path.getsize(self.output_abspath),
                                                   SOURCE_URL_KEY: self.url}})
        return self.outputs



