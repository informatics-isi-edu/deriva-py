from importlib import import_module
from deriva.core import read_config, format_exception
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.base_processor import BaseProcessor
from deriva.transfer.download.processors.query.base_query_processor import CSVQueryProcessor, \
    JSONEnvUpdateProcessor, JSONQueryProcessor, JSONStreamQueryProcessor
from deriva.transfer.download.processors.query.bag_fetch_query_processor import BagFetchQueryProcessor
from deriva.transfer.download.processors.query.file_download_query_processor import FileDownloadQueryProcessor
from deriva.transfer.download.processors.query.fasta_output_query_processor import FastaOutputQueryProcessor
from deriva.transfer.download.processors.postprocess.identifier_post_processor import GlobusIdentifierPostProcessor, \
    MinidIdentifierPostProcessor
from deriva.transfer.download.processors.postprocess.transfer_post_processor import Boto3UploadPostProcessor, \
    LibcloudUploadPostProcessor
from deriva.transfer.download.processors.postprocess.workspace_post_processor import GlobusWorkspacePortalPostProcessor

DEFAULT_QUERY_PROCESSORS = {
    "csv": CSVQueryProcessor,
    "env": JSONEnvUpdateProcessor,
    "json": JSONQueryProcessor,
    "json-stream": JSONStreamQueryProcessor,
    "download": FileDownloadQueryProcessor,
    "fetch": BagFetchQueryProcessor,
    "fasta": FastaOutputQueryProcessor
}

DEFAULT_TRANSFORM_PROCESSORS = {

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
            raise DerivaDownloadConfigurationError("Unsupported processor type: %s" % processor_name)

    if not is_processor_whitelisted(processor_type, **kwargs):
        raise DerivaDownloadConfigurationError(
            "Unknown external processor type [%s]: this processor must be added to the whitelist." % processor_type)

    clazz = None
    try:
        module_name, class_name = processor_type.rsplit(".", 1)
        try:
            module = sys.modules[module_name]
        except KeyError:
            module = import_module(module_name)
        clazz = getattr(module, class_name) if module else None
    except (ImportError, AttributeError):
        pass
    if not clazz:
        raise DerivaDownloadConfigurationError("Unable to import specified processor class %s" % processor_type)

    if not issubclass(clazz, BaseProcessor):
        raise DerivaDownloadError(format_exception(NotImplementedError("The imported class %s is not a subclass of %s" %
                                  (processor_type, BaseProcessor.__module__ + "." + BaseProcessor.__name__))))

    return clazz


def is_processor_whitelisted(processor_type, **kwargs):
    config_file_path = kwargs.get("config_file_path")
    config = read_config(config_file=config_file_path)
    whitelist = config.get("download_processor_whitelist", [])

    return processor_type in whitelist


def find_query_processor(processor_name, processor_type=None, **kwargs):
    return find_processor(processor_name, processor_type, DEFAULT_QUERY_PROCESSORS, **kwargs)


def find_transform_processor(processor_name, processor_type=None, **kwargs):
    return find_processor(processor_name, processor_type, DEFAULT_TRANSFORM_PROCESSORS, **kwargs)


def find_post_processor(processor_name, processor_type=None, **kwargs):
    return find_processor(processor_name, processor_type, DEFAULT_POST_PROCESSORS, **kwargs)
