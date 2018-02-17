import os
import json
import uuid
import datetime
import logging
import certifi
from bdbag import bdbag_ro as ro
from deriva.core import urlsplit, get_transfer_summary, DEFAULT_CHUNK_SIZE
from deriva.core.utils.mime_utils import parse_content_disposition
from deriva.transfer.download.processors import BaseDownloadProcessor


class FileDownloadProcessor(BaseDownloadProcessor):
    def __init__(self, envars=None, **kwargs):
        super(FileDownloadProcessor, self).__init__(envars, **kwargs)
        self.content_type = "application/x-json-stream"
        self.output_relpath, self.output_abspath = self.createPaths(self.base_path, "download-manifest.json")
        self.ro_file_provenance = False

    def process(self):
        super(FileDownloadProcessor, self).process()
        self.downloadFiles(self.output_abspath)

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
                file_error = "File transfer failed: [%s]" % output_path
                url_error = 'HTTP GET Failed for url: %s' % url
                host_error = "Host %s responded:\n\n%s" % (urlsplit(url).netloc, r.text)
                raise RuntimeError('%s\n\n%s\n%s' % (file_error, url_error, host_error))
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
        logging.info("Retrieving file(s)...")
        subdir = self.sub_path
        try:
            with open(input_manifest, "r") as in_file:
                for line in in_file:
                    entry = json.loads(line)
                    url = entry.get('url')
                    if not url:
                        raise RuntimeError(
                            "Missing required attribute \"url\" in download manifest entry %s" % json.dumps(entry))
                    store = self.getHatracStore(url)
                    filename = entry.get('filename')
                    if not filename:
                        envvars = self.envars.copy()
                        envvars.update(entry)
                        subdir = self.sub_path % envvars
                        if store:
                            head = store.head(url, headers=self.HEADERS)
                            content_disposition = head.headers.get("Content-Disposition") if head.ok else None
                            filename = os.path.basename(filename).split(":")[0] if not content_disposition else \
                                parse_content_disposition(content_disposition)
                        else:
                            filename = os.path.basename(url)
                    file_path = os.path.abspath(os.path.join(
                        self.base_path, 'data' if self.is_bag else '', subdir, filename))
                    output_dir = os.path.dirname(file_path)
                    self.makeDirs(output_dir)
                    if store:
                        resp = store.get_obj(url, self.HEADERS, file_path)
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
                        raise RuntimeError(
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
                                             bundled_as=ro.make_bundled_as(uri="urn:uuid:%s" % str(uuid.uuid4())))
        finally:
            os.remove(input_manifest)
