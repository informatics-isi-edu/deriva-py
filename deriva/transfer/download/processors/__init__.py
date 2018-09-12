from pydoc import locate
from deriva.core import read_config
from deriva.transfer.download.processors.base_processor import BaseProcessor
from download.processors.query.base_query_processor import CSVQueryProcessor, \
    JSONEnvUpdateProcessor, JSONQueryProcessor, JSONStreamQueryProcessor
from download.processors.query.bag_fetch_query_processor import BagFetchQueryProcessor
from download.processors.query.file_download_query_processor import FileDownloadQueryProcessor
from download.processors.query.fasta_output_query_processor import FastaOutputQueryProcessor
from download.processors.postprocess.identifier_post_processor import GlobusIdentifierPostProcessor, \
    MinidIdentifierPostProcessor
from download.processors.postprocess.transfer_post_processor import Boto3UploadPostProcessor, \
    LibcloudUploadPostProcessor
from download.processors.postprocess.workspace_post_processor import GlobusWorkspacePortalPostProcessor

DEFAULT_QUERY_PROCESSORS = {
    "csv": CSVQueryProcessor,
    "env": JSONEnvUpdateProcessor,
    "json": JSONQueryProcessor,
    "json-stream": JSONStreamQueryProcessor,
    "download": FileDownloadQueryProcessor,
    "fetch": BagFetchQueryProcessor,
    "fasta": FastaOutputQueryProcessor
}

DEFAULT_OUTPUT_PROCESSORS = {

}

DEFAULT_POST_PROCESSORS = {
    "identifier": GlobusIdentifierPostProcessor,
    "cloud_upload": Boto3UploadPostProcessor,
    "libcloud_upload": LibcloudUploadPostProcessor,
    "workspace": GlobusWorkspacePortalPostProcessor
}


def find_processor(processor_name, processor_type=None, defaults={}, **kwargs):
    if not processor_type:
        if processor_name in defaults:
            return defaults[processor_name]
        else:
            raise RuntimeError("Unsupported processor type: %s" % processor_name)

    if not is_processor_whitelisted(processor_type, **kwargs):
        raise RuntimeError(
            "Unknown external processor type [%s]: this processor must be added to the whitelist." % processor_type)

    clazz = locate(processor_type)
    if not clazz:
        raise RuntimeError("Unable to locate specified processor class %s" % processor_type)

    if not issubclass(clazz, BaseProcessor):
        raise NotImplementedError("The class %s is not a subclass of %s" %
                                  (processor_type, BaseProcessor.__module__ + "." + BaseProcessor.__name__))

    return clazz


def is_processor_whitelisted(processor_type, **kwargs):
    config_file_path = kwargs.get("config_file_path")
    config = read_config(config_file=config_file_path)
    whitelist = config.get("download_processor_whitelist", [])

    return processor_type in whitelist


def find_query_processor(processor_name, processor_type=None, **kwargs):
    return find_processor(processor_name, processor_type, DEFAULT_QUERY_PROCESSORS, **kwargs)


def find_output_processor(processor_name, processor_type=None, **kwargs):
    return find_processor(processor_name, processor_type, DEFAULT_OUTPUT_PROCESSORS, **kwargs)


def find_post_processor(processor_name, processor_type=None, **kwargs):
    return find_processor(processor_name, processor_type, DEFAULT_POST_PROCESSORS, **kwargs)
