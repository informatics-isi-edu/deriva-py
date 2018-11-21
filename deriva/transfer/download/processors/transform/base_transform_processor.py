import os
from deriva.core.utils.mime_utils import guess_content_type
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.base_processor import BaseProcessor, LOCAL_PATH_KEY, SOURCE_URL_KEY
from bdbag import bdbag_ro as ro


class BaseTransformProcessor(BaseProcessor):
    """
    Base class for TransformProcessor classes
    """

    def __init__(self, envars=None, **kwargs):
        super(BaseTransformProcessor, self).__init__(envars, **kwargs)
        self.base_path = kwargs["base_path"]
        self.input_path = self.parameters["input_path"]
        self.sub_path = self.parameters.get("output_path", "")
        self.is_bag = kwargs.get("bag", False)
        self.transformed_output = self.outputs.get(self.input_path, dict())
        self.url = self.transformed_output.get(SOURCE_URL_KEY)
        self.ro_file_provenance = True
        self.ro_manifest = self.kwargs.get("ro_manifest")
        self.ro_author_name = self.kwargs.get("ro_author_name")
        self.ro_author_orcid = self.kwargs.get("ro_author_orcid")
        self.input_relpath = None
        self.input_abspath = None
        self.output_relpath = None
        self.output_abspath = None

    def process(self):
        if self.ro_manifest and self.ro_file_provenance:
            ro.add_file_metadata(self.ro_manifest,
                                 source_url=self.url,
                                 local_path=self.output_relpath,
                                 media_type=guess_content_type(self.output_abspath),
                                 retrieved_on=ro.make_retrieved_on(),
                                 retrieved_by=ro.make_retrieved_by(self.ro_author_name, orcid=self.ro_author_orcid),
                                 bundled_as=ro.make_bundled_as())
        return self.outputs



