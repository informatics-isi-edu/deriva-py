import sys
from importlib import import_module
from deriva.core import read_config, format_exception
from deriva.transfer.upload import DerivaUploadConfigurationError
from deriva.transfer.upload.processors.base_processor import BaseProcessor
from deriva.transfer.upload.processors.archive_processor import BagArchiveProcessor
from deriva.transfer.upload.processors.rename_processor import FileRenameProcessor
from deriva.transfer.upload.processors.logging_processor import LoggingProcessor
from deriva.transfer.upload.processors.metadata_update_processor import MetadataUpdateProcessor

DEFAULT_PROCESSORS = {
    "BagArchiveProcessor": BagArchiveProcessor,
    "FileRenameProcessor": FileRenameProcessor,
    "LoggingProcessor": LoggingProcessor,
    "MetadataProcessor": MetadataUpdateProcessor
}


def find_processor(processor_name, processor_type=None, defaults=DEFAULT_PROCESSORS.copy(), **kwargs):
    if not processor_type:
        if processor_name in defaults:
            return defaults[processor_name]
        else:
            raise DerivaUploadConfigurationError("Unsupported processor type: %s" % processor_name)

    if not _is_processor_whitelisted(processor_type, **kwargs):
        raise DerivaUploadConfigurationError(
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
        raise DerivaUploadConfigurationError("Unable to import specified processor class: %s" % processor_type)

    if not issubclass(clazz, BaseProcessor):
        raise DerivaUploadConfigurationError(format_exception(
            NotImplementedError("The imported class %s is not a subclass of %s" %
                                (processor_type, BaseProcessor.__module__ + "." + BaseProcessor.__name__))))

    return clazz


def _is_processor_whitelisted(processor_type, **kwargs):
    if kwargs.get("bypass_whitelist", False):
        return True
    config_file_path = kwargs.get("config_file_path")
    config = read_config(config_file=config_file_path)
    whitelist = config.get("upload_processor_whitelist", [])

    return processor_type in whitelist
