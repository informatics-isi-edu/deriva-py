import os
import json
import uuid
import datetime
import logging
import certifi
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bdbag import bdbag_ro as ro
from deriva.core import urlquote, urlsplit, get_transfer_summary, DEFAULT_CHUNK_SIZE, DEFAULT_SESSION_CONFIG
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

    def getExternalSession(self, host):
        sessions = self.sessions
        cookies = self.args.get("cookies")
        auth_url = self.args.get("auth_url")
        login_params = self.args.get("login_params")
        session_config = self.args.get("session_config")

        session = sessions.get(host)
        if session is not None:
            return session

        session = requests.session()
        if not session_config:
            session_config = DEFAULT_SESSION_CONFIG
        retries = Retry(connect=session_config['retry_connect'],
                        read=session_config['retry_read'],
                        backoff_factor=session_config['retry_backoff_factor'],
                        status_forcelist=session_config['retry_status_forcelist'])

        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))

        if cookies:
            session.cookies.update(cookies)
        if login_params and auth_url:
            r = session.post(auth_url, data=login_params, verify=certifi.where())
            if r.status_code > 203:
                raise RuntimeError('GetExternalSession Failed with Status Code: %s\n%s\n' % (r.status_code, r.text))

        sessions[host] = session
        return session

    def getExternalFile(self, url, output_path, headers=None):
        host = urlsplit(url).netloc
        if output_path:
            try:
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
            except requests.exceptions.RequestException as e:
                raise RuntimeError('HTTP Request Exception: %s %s' % (e.errno, e.message))

    def downloadFiles(self, input_manifest):
        logging.info("Retrieving file(s)...")
        try:
            with open(input_manifest, "r") as in_file:
                for line in in_file:
                    entry = json.loads(line)
                    if not (entry.get('url') and entry.get('length')):
                        logging.warning(
                            "Missing required attributes (url, length) in download manifest line %s" % line)
                    url = entry['url']
                    length = int(entry['length'])
                    if not entry.get('filename'):
                        subdir = self.sub_path % entry
                        filename = os.path.basename(url)
                    else:
                        subdir = self.sub_path
                        filename = entry['filename']
                    file_path = os.path.abspath(os.path.join(
                        self.base_path, 'data' if self.is_bag else '', subdir, filename))
                    logging.debug("Retrieving %s as %s" % (url, file_path))
                    output_dir = os.path.dirname(file_path)
                    self.makeDirs(output_dir)
                    if url.startswith("/hatrac/") and self.store:
                        self.store.get_obj(url, self.HEADERS, file_path)
                    else:
                        self.getExternalFile(url, file_path, self.HEADERS)
                        file_bytes = os.path.getsize(file_path)
                        if length != file_bytes:
                            raise RuntimeError(
                                "File size of %s does not match expected size of %s for file %s" %
                                (length, file_bytes, filename))
                    output_path = ''.join([subdir, "/", filename])
                    if self.ro_manifest:
                        ro.add_file_metadata(self.ro_manifest,
                                             source_url=url,
                                             local_path=output_path,
                                             retrieved_on=ro.make_retrieved_on(),
                                             retrieved_by=ro.make_retrieved_by(
                                                 self.ro_author_name, orcid=self.ro_author_orcid),
                                             bundled_as=ro.make_bundled_as(uri="urn:uuid:%s" % str(uuid.uuid4())))
        finally:
            os.remove(input_manifest)
