import logging
from deriva.transfer.download.processors.base_processor import BaseProcessor


class IdentifierPostProcessor(BaseProcessor):
    """
    Post processor that mints identifiers for download results
    """

    def __init__(self, envars=None, **kwargs):
        super(IdentifierPostProcessor, self).__init__(envars, **kwargs)
        self.outputs = kwargs.get("input_files", {})

    def process(self):
        return self.outputs
