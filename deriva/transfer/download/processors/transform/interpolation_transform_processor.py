import json
import re
from string import Template
import logging
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.transform.base_transform_processor import BaseTransformProcessor, \
    LOCAL_PATH_KEY, SOURCE_URL_KEY

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
        """Reads a json-stream input file, performs string interpolation, and writes to output file.
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

        self.outputs.update({self.output_relpath: {LOCAL_PATH_KEY: self.output_abspath, SOURCE_URL_KEY: self.url}})
        return super(InterpolationTransformProcessor, self).process()


class StrSubTransformProcessor(BaseTransformProcessor):
    """String substitution transform processor.
    """
    def __init__(self, envars=None, **kwargs):
        super(StrSubTransformProcessor, self).__init__(envars, **kwargs)

        self.input_relpath, self.input_abspath = self.create_paths(
            self.base_path, self.input_path, is_bag=self.is_bag, envars=envars)
        self.output_relpath, self.output_abspath = self.create_paths(
            self.base_path, self.sub_path, is_bag=self.is_bag, envars=envars)

        self.substitutions = self.parameters.get('substitutions', [])
        logger.debug("String substitutions: {}".format(self.substitutions))
        # validate strsubs
        for strsub in self.substitutions:
            missing = set(strsub.keys()) - {'pattern', 'repl', 'input', 'output'}
            if missing:
                raise DerivaDownloadConfigurationError("Missing required key(s) %s in 'substitutions' parameter" % missing)

    def process(self):
        """Reads a json-stream input file, performs string substitutions, and writes to output file.
        """
        try:
            with open(self.input_abspath) as inputfile, \
                 open(self.output_abspath, mode='w') as outputfile:
                for line in inputfile:
                    row = json.loads(line)
                    for strsub in self.substitutions:
                        row[strsub['output']] = re.sub(strsub['pattern'], strsub['repl'], row[strsub['input']])
                    outputfile.write(json.dumps(row))
                    outputfile.write('\n')
        except IOError as e:
            raise DerivaDownloadError("Interpolation transform failed", e)
        except KeyError as e:
            raise DerivaDownloadError("Required input attribute not found in row", e)

        self.outputs.update({self.output_relpath: {LOCAL_PATH_KEY: self.output_abspath, SOURCE_URL_KEY: self.url}})
        return super(StrSubTransformProcessor, self).process()
