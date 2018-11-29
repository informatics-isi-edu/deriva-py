import json
from string import Template
import logging
from deriva.transfer.download import DerivaDownloadError
from deriva.transfer.download.processors.transform.base_transform_processor import BaseTransformProcessor

logger = logging.getLogger(__name__)


class InterpolationTransformProcessor(BaseTransformProcessor):
    """String interpolation transform processor.
    """
    def __init__(self, envars=None, **kwargs):
        super(InterpolationTransformProcessor, self).__init__(envars, **kwargs)

        self.input_relpath, self.input_abspath = self.create_paths(
            self.base_path, self.input_path, is_bag=self.is_bag, envars=envars)
        self.output_relpath, self.output_abspath = self.create_paths(
            self.base_path, self.sub_path, is_bag=self.is_bag, envars=envars)

        self.template = Template(self.parameters.get('template'))
        logger.debug("Interpolating with template: {}".format(self.template.template))

    def process(self):
        """Reads a json-stream input file, transforms according to 'template', and writes to output file.
        """
        try:
            with open(self.input_abspath) as inputfile, \
                 open(self.output_abspath, mode='w') as outputfile:
                for line in inputfile:
                    row = json.loads(line)
                    output = self.template.safe_substitute(row)
                    outputfile.write(output)
        except IOError as e:
            raise DerivaDownloadError("Interpolation transform failed", e)

        return super(InterpolationTransformProcessor, self).process()
