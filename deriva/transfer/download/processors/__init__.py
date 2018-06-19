from pydoc import locate
from deriva.transfer.download.processors.base_download_processor import BaseDownloadProcessor, CSVDownloadProcessor, \
    JSONEnvUpdateProcessor, JSONDownloadProcessor, JSONStreamDownloadProcessor
from deriva.transfer.download.processors.bag_fetch_download_processor import BagFetchDownloadProcessor
from deriva.transfer.download.processors.file_download_processor import FileDownloadProcessor

DEFAULT_DOWNLOAD_PROCESSORS = {
    "csv": CSVDownloadProcessor,
    "env": JSONEnvUpdateProcessor,
    "json": JSONDownloadProcessor,
    "json-stream": JSONStreamDownloadProcessor,
    "download": FileDownloadProcessor,
    "fetch": BagFetchDownloadProcessor
}


def findProcessor(output_format, format_class=None):
    if not format_class:
        if output_format in DEFAULT_DOWNLOAD_PROCESSORS:
            return DEFAULT_DOWNLOAD_PROCESSORS[output_format]
        else:
            raise RuntimeError("Unsupported output type: %s" % output_format)

    clazz = locate(format_class)
    if not clazz:
        raise RuntimeError("Unable to locate specified download processor class %s" % format_class)

    return clazz
