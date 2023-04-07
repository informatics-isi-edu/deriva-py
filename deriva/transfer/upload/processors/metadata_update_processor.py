import json
import logging
from deriva.transfer.upload.processors import BaseProcessor

logger = logging.getLogger(__name__)


class MetadataUpdateProcessor(BaseProcessor):
    """
    Upload processor used to load external JSON into a dict for use as upload metadata.
    """
    def __init__(self, **kwargs):
        super(MetadataUpdateProcessor, self).__init__(**kwargs)
        self.metadata = kwargs.get("metadata", dict()) or dict

    def process(self):
        input_file = self.parameters.get("input_file")
        if not input_file:
            return

        with open(input_file, "r") as input_data:
            data = json.load(input_data)
            if not isinstance(data, dict):
                logger.warning("Type mismatch: expected dict object from loaded JSON file: %s" % input_file)
            for key in data.keys():
                if key in self.metadata:
                    logger.warning(
                        "Duplicate key '%s' encountered in metadata input file [%s] existing key will be "
                        "overwritten." % (key, input_file))
            self.metadata.update(data)
