import os
import json
from collections import OrderedDict
from deriva.transfer.download.processors.transform.base_transform_processor import BaseTransformProcessor


class FastaExportTransformProcessor(BaseTransformProcessor):
    def __init__(self, envars=None, **kwargs):
        super(FastaExportTransformProcessor, self).__init__(envars, **kwargs)
        self._create_input_output_paths()

    def process(self):
        self.convert_json_file_to_fasta(self.input_abspath, self.output_abspath, self.processor_params)
        return super(FastaExportTransformProcessor, self).process()

    @staticmethod
    def convert_json_obj_to_fasta_lines(obj, params):
        column_map = None
        comment_line = ''
        data_line = ''
        if params:
            column_map = params.get("column_map", None)

        if (len(obj) > 1) and (not column_map):
            raise RuntimeError("FASTA converter input data has more than one element and no column map was specified.")
        elif len(obj) == 1:
            k, v = obj.popitem()
            lines = str("%s\n" % v)
        else:
            for k, v in obj.items():
                if column_map.get(k, None) == "comment":
                    if comment_line:
                        comment_line += str(" | %s" % v)
                    else:
                        comment_line = str("> %s" % v)
                elif column_map.get(k, None) == "data":
                    data_line += v
            lines = str("%s\n%s\n" % (comment_line, data_line))

        return lines

    @staticmethod
    def convert_json_file_to_fasta(input_file, output_file, params):
        with open(input_file, "r") as input_data, open(output_file, "w") as output_data:
            line = input_data.readline().lstrip()
            input_data.seek(0)
            is_json_stream = False
            if line.startswith('{'):
                data = input_data
                is_json_stream = True
            else:
                try:
                    data = json.load(input_data, object_pairs_hook=OrderedDict)
                except ValueError:
                    data = {}

            for entry in data:
                if is_json_stream:
                    entry = json.loads(entry, object_pairs_hook=OrderedDict)

                lines = convert_json_obj_to_fasta_lines(entry, params)
                output_data.writelines(lines)
