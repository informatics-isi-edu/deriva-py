import logging
from deriva.transfer.download.processors.base_processor import BaseProcessor


class TransferPostProcessor(BaseProcessor):
    """
    Post processor that transfers download results to remote systems.
    """

    def __init__(self, envars=None, **kwargs):
        super(TransferPostProcessor, self).__init__(envars, **kwargs)
        self.outputs = kwargs.get("input_files", {})

    def process(self):
        return self.outputs
