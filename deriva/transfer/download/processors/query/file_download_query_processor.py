import os
import json
import uuid
import datetime
import logging
import requests
from bdbag import bdbag_ro as ro
from deriva.core import urlsplit, format_exception, get_transfer_summary, make_dirs, DEFAULT_CHUNK_SIZE
from deriva.core.utils.mime_utils import parse_content_disposition
from deriva.transfer.download.processors.query.base_query_processor import BaseQueryProcessor, \
    LOCAL_PATH_KEY, FILE_SIZE_KEY
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError, \
    DerivaDownloadAuthenticationError


class FileDownloadQueryProcessor(BaseQueryProcessor):
    def __init__(self, envars=None, **kwargs):
        super(FileDownloadQueryProcessor, self).__init__(envars, **kwargs)
        self.content_type = "application/x-json-stream"
        filename = ''.join(['download-manifest_', str(uuid.uuid4()), ".json"])
        self.output_relpath, self.output_abspath = self.create_paths(self.base_path, filename=filename)
        self.ro_file_provenance = False
        self.allow_anonymous = kwargs.get("allow_anonymous", True)

    def process(self):
        if not self.identity and not self.allow_anonymous:
            raise DerivaDownloadAuthenticationError(
                "Unauthenticated (anonymous) users are not permitted to request direct file downloads.")
        super(FileDownloadQueryProcessor, self).process()
        self.outputs.update(self.downloadFiles(self.output_abspath))
        return self.outputs

    def getExternalFile(self, url, output_path, headers=None):
        host = urlsplit(url).netloc
        if output_path:
            if not headers:
                headers = self.HEADERS.copy()
            else:
                headers.update(self.HEADERS)
            session = self.getExternalSession(host)
            with session.get(url, headers=headers, stream=True) as r:
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
                    length = int(r.headers.get('Content-Length'))
                    content_type = r.headers.get("Content-Type")
                    return output_path, length, content_type

    def downloadFiles(self, input_manifest):
        logging.info("Attempting to download file(s) based on the results of query: %s" % self.query)
        try:
            with open(input_manifest, "r", encoding='utf-8') as in_file:
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
                    filename = entry.get('filename') if not self.output_filename else self.output_filename
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
                    env = self.envars.copy()
                    env.update(entry)
                    rel_path, file_path = self.create_paths(self.base_path,
                                                            sub_path=self.sub_path,
                                                            filename=filename,
                                                            is_bag=self.is_bag,
                                                            envars=env)
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
                        file_path, length, content_type = self.getExternalFile(url, file_path)
                    file_bytes = os.path.getsize(file_path)
                    if length != file_bytes:
                        raise DerivaDownloadError(
                            "File size of %s does not match expected size of %s for file %s" %
                            (length, file_bytes, file_path))
                    if self.ro_manifest:
                        ro.add_file_metadata(self.ro_manifest,
                                             source_url=url,
                                             local_path=rel_path,
                                             media_type=content_type,
                                             retrieved_on=ro.make_retrieved_on(),
                                             retrieved_by=ro.make_retrieved_by(
                                                 self.ro_author_name, orcid=self.ro_author_orcid),
                                             bundled_as=ro.make_bundled_as())
                    file_list.update({rel_path: {LOCAL_PATH_KEY: file_path, FILE_SIZE_KEY: file_bytes}})
                    if self.callback:
                        if not self.callback(progress="Downloaded [%s] to: %s" % (url, file_path)):
                            break

                return file_list
        finally:
            os.remove(input_manifest)
