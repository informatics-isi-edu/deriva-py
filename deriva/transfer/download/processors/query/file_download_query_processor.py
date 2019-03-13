import os
import json
import datetime
import logging
import requests
import certifi
from bdbag import bdbag_ro as ro
from deriva.core import urlsplit, format_exception, get_transfer_summary, make_dirs, DEFAULT_CHUNK_SIZE
from deriva.core.utils.mime_utils import parse_content_disposition
from deriva.transfer.download.processors.query.base_query_processor import BaseQueryProcessor, LOCAL_PATH_KEY
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError


class FileDownloadQueryProcessor(BaseQueryProcessor):
    def __init__(self, envars=None, **kwargs):
        super(FileDownloadQueryProcessor, self).__init__(envars, **kwargs)
        self.content_type = "application/x-json-stream"
        self.output_relpath, self.output_abspath = self.create_paths(self.base_path, "download-manifest.json")
        self.ro_file_provenance = False

    def process(self):
        super(FileDownloadQueryProcessor, self).process()
        self.outputs.update(self.downloadFiles(self.output_abspath))
        return self.outputs

    def getExternalFile(self, url, output_path, headers=None):
        host = urlsplit(url).netloc
        if output_path:
            if not headers:
                headers = self.HEADERS
            else:
                headers.update(self.HEADERS)
            session = self.getExternalSession(host)
            r = session.get(url, headers=headers, stream=True, verify=certifi.where())
            if r.status_code != 200:
                file_error = "File [%s] transfer failed." % output_path
                url_error = 'HTTP GET Failed for url: %s' % url
                host_error = "Host %s responded:\n\n%s" % (urlsplit(url).netloc, r.text)
                raise DerivaDownloadError('%s\n\n%s\n%s' % (file_error, url_error, host_error))
            else:
                total = 0
                start = datetime.datetime.now()
                logging.debug("Transferring file %s to %s" % (url, output_path))
                with open(output_path, 'wb') as data_file:
                    for chunk in r.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                        data_file.write(chunk)
                        total += len(chunk)
                elapsed = datetime.datetime.now() - start
                summary = get_transfer_summary(total, elapsed)
                logging.info("File [%s] transfer successful. %s" % (output_path, summary))
                return output_path, r

    def downloadFiles(self, input_manifest):
        logging.info("Attempting to download file(s) based on the results of query: %s" % self.query)
        try:
            with open(input_manifest, "r") as in_file:
                file_list = dict()
                for line in in_file:
                    entry = json.loads(line)
                    url = entry.get('url')
                    if not url:
                        logging.warning(
                            "Skipping download due to missing required attribute \"url\" in download manifest entry %s"
                            % json.dumps(entry))
                        continue
                    store = self.getHatracStore(url)
                    filename = entry.get('filename')
                    envvars = self.envars.copy()
                    envvars.update(entry)
                    subdir = self.sub_path.format(**envvars)
                    if not filename:
                        if store:
                            try:
                                head = store.head(url, headers=self.HEADERS)
                            except requests.HTTPError as e:
                                raise DerivaDownloadError("HEAD request for [%s] failed: %s" % (url, e))
                            content_disposition = head.headers.get("Content-Disposition") if head.ok else None
                            filename = os.path.basename(filename).split(":")[0] if not content_disposition else \
                                parse_content_disposition(content_disposition)
                        else:
                            filename = os.path.basename(url)
                    file_path = os.path.abspath(os.path.join(
                        self.base_path, 'data' if self.is_bag else '', subdir, filename))
                    output_dir = os.path.dirname(file_path)
                    make_dirs(output_dir)
                    if store:
                        try:
                            resp = store.get_obj(url, self.HEADERS, file_path)
                        except requests.HTTPError as e:
                            raise DerivaDownloadError("File [%s] transfer failed: %s" % (file_path, e))
                        length = int(resp.headers.get('Content-Length'))
                        content_type = resp.headers.get("Content-Type")
                        url = self.getExternalUrl(url)
                    else:
                        url = self.getExternalUrl(url)
                        file_path, resp = self.getExternalFile(url, file_path, self.HEADERS)
                        length = int(resp.headers.get('Content-Length'))
                        content_type = resp.headers.get("Content-Type")
                    file_bytes = os.path.getsize(file_path)
                    if length != file_bytes:
                        raise DerivaDownloadError(
                            "File size of %s does not match expected size of %s for file %s" %
                            (length, file_bytes, file_path))
                    output_path = ''.join([subdir, "/", filename]) if subdir else filename
                    if self.ro_manifest:
                        ro.add_file_metadata(self.ro_manifest,
                                             source_url=url,
                                             local_path=output_path,
                                             media_type=content_type,
                                             retrieved_on=ro.make_retrieved_on(),
                                             retrieved_by=ro.make_retrieved_by(
                                                 self.ro_author_name, orcid=self.ro_author_orcid),
                                             bundled_as=ro.make_bundled_as())
                    file_list.update({output_path: {LOCAL_PATH_KEY: file_path}})
                return file_list
        finally:
            os.remove(input_manifest)
