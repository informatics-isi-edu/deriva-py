import io
import os
import json
import logging
from enum import Enum
from deriva.core import json_item_handler
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.transform.base_transform_processor import BaseTransformProcessor

logger = logging.getLogger(__name__)


ColumnFunctions = Enum("ColumnFunctions", ["add", "replace", "delete"])


class ColumnTransformProcessor(BaseTransformProcessor):
    """Column transform processor.
    """
    def __init__(self, envars=None, **kwargs):
        super(ColumnTransformProcessor, self).__init__(envars, **kwargs)
        self._create_input_output_paths()
        self.ro_file_provenance = False
        # get custom params
        self.column_transforms = self.parameters.get("column_transforms", dict())
        self.writer = None

    def process(self):
        """Reads a json-stream input file, performs column manipulation functions, and writes to output file.
        """
        try:
            with open(self.output_abspath, mode='w', encoding='utf-8') as output_file:
                def row_handler(item):
                    if not self.writer:
                        self.writer = io.open(self.output_abspath, 'w', encoding='utf-8')
                    for k, v in self.column_transforms.items():
                        func = v["fn"]
                        value = v.get("value")
                        if func == ColumnFunctions.add.name:
                            item.update({k: value})
                        elif func == ColumnFunctions.delete.name:
                            del item[k]
                        elif func == ColumnFunctions.replace.name:
                            repl_k = value["key"]
                            repl_v = value.get("value")
                            if repl_v:
                                item.update({k: item[repl_k][repl_v]})
                            else:
                                item.update({k: item[repl_k]})
                        else:
                            raise DerivaDownloadError(
                                "Unknown function '%s' in column transform [%s]" % (func, v))
                    output_file.writelines(''.join(
                        [json.dumps(item, ensure_ascii=False), '\n']))
                json_item_handler(self.input_abspath, row_handler)
                self.writer.flush()
                self.writer.close()

        except IOError as e:
            raise DerivaDownloadError("Column transform failed", e)

        return super(ColumnTransformProcessor, self).process()
