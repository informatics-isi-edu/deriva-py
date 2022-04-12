import os
import io
import csv
import json
import logging
from deriva.core import json_item_handler
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.transform.base_transform_processor import BaseTransformProcessor

logger = logging.getLogger(__name__)
csv.register_dialect('unix_comma_quotemin', delimiter=',', lineterminator='\n', quoting=csv.QUOTE_MINIMAL)


class JSONtoCSVTransformProcessor(BaseTransformProcessor):
    """ Convert JSON or JSON-Stream to CSV transform processor.
    """
    def __init__(self, envars=None, **kwargs):
        super(JSONtoCSVTransformProcessor, self).__init__(envars, **kwargs)
        self._create_input_output_paths()
        self.writer = None

        # get custom params
        self.include_header = self.parameters.get('include_header', True)
        self.csv_dialect = self.parameters.get('csv_dialect', "unix_comma_quotemin")

    def process(self):
        """Reads a JSON or JSON-Stream input file, converts to CSV output with optional header row.
        """
        try:
            with io.open(self.output_abspath, mode='w', encoding='utf-8', newline='') as output_file:
                def row_handler(item):
                    if not self.writer:
                        self.writer = csv.DictWriter(output_file, fieldnames=item.keys(), dialect=self.csv_dialect)
                        if self.include_header:
                            self.writer.writeheader()
                    self.writer.writerow(item)

                json_item_handler(self.input_abspath, row_handler)

        except IOError as e:
            raise DerivaDownloadError("JSONtoCSV transform failed", e)

        return super(JSONtoCSVTransformProcessor, self).process()




