from pydoc import locate
from deriva.transfer.download.processors.base_processor import BaseProcessor
from download.processors.query.base_query_processor import CSVQueryProcessor, \
    JSONEnvUpdateProcessor, JSONQueryProcessor, JSONStreamQueryProcessor
from download.processors.query.bag_fetch_query_processor import BagFetchQueryProcessor
from download.processors.query.file_download_query_processor import FileDownloadQueryProcessor
from download.processors.query.fasta_output_query_processor import FastaOutputQueryProcessor
from download.processors.postprocess.identifier_post_processor import IdentifierPostProcessor
from download.processors.postprocess.transfer_post_processor import TransferPostProcessor

DEFAULT_QUERY_PROCESSORS = {
    "csv": CSVQueryProcessor,
    "env": JSONEnvUpdateProcessor,
    "json": JSONQueryProcessor,
    "json-stream": JSONStreamQueryProcessor,
    "download": FileDownloadQueryProcessor,
    "fetch": BagFetchQueryProcessor,
    "fasta": FastaOutputQueryProcessor
}

DEFAULT_PRE_PROCESSORS = {
    "identifier": IdentifierPostProcessor,
    "transfer": TransferPostProcessor
}

DEFAULT_POST_PROCESSORS = {
    "identifier": IdentifierPostProcessor,
    "transfer": TransferPostProcessor
}


def find_processor(processor_name, processor_type=None, defaults={}):
    if not processor_type:
        if processor_name in defaults:
            return defaults[processor_name]
        else:
            raise RuntimeError("Unsupported processor type: %s" % processor_name)

    if not is_processor_whitelisted(processor_type):
        raise RuntimeError("Unknown external processor type %s: this processor must be added to the whitelist.")

    clazz = locate(processor_type)
    if not clazz:
        raise RuntimeError("Unable to locate specified processor class %s" % processor_type)

    if not issubclass(clazz, BaseProcessor):
        raise NotImplementedError("The class %s is not a subclass of %s" %
                                  (processor_type, BaseProcessor.__module__ + "." + BaseProcessor.__name__))

    return clazz


def is_processor_whitelisted(processor_type):
    # TODO: implement this!
    return True


def find_query_processor(processor_name, processor_type=None):
    return find_processor(processor_name, processor_type, DEFAULT_QUERY_PROCESSORS)


def find_pre_processor(processor_name, processor_type=None):
    return find_processor(processor_name, processor_type, DEFAULT_POST_PROCESSORS)


def find_post_processor(processor_name, processor_type=None):
    return find_processor(processor_name, processor_type, DEFAULT_POST_PROCESSORS)
