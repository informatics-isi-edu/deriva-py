import os
import json
import uuid
import logging
import requests
from bdbag import bdbag_ro as ro
from deriva.core import format_exception
from deriva.core.utils.hash_utils import decodeBase64toHex
from deriva.core.utils.mime_utils import parse_content_disposition
from deriva.transfer.download.processors.query.base_query_processor import BaseQueryProcessor, LOCAL_PATH_KEY
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError


class BagFetchQueryProcessor(BaseQueryProcessor):
    def __init__(self, envars=None, **kwargs):
        super(BagFetchQueryProcessor, self).__init__(envars, **kwargs)
        self.content_type = "application/x-json-stream"
        filename = ''.join(['fetch-manifest_', str(uuid.uuid4()), ".json"])
        self.output_relpath, self.output_abspath = self.create_paths(self.base_path, filename=filename)
        self.ro_file_provenance = False

    def process(self):
        super(BagFetchQueryProcessor, self).process()
        rfm_relpath, rfm_abspath = self.createRemoteFileManifest()
        if rfm_relpath and rfm_abspath:
            self.outputs.update({rfm_relpath: {LOCAL_PATH_KEY: rfm_abspath}} if not self.is_bag else {})
        return self.outputs

    def createRemoteFileManifest(self):
        logging.info("Creating remote file manifest from results of query: %s" % self.query)
        input_manifest = self.output_abspath
        remote_file_manifest = self.kwargs.get("remote_file_manifest")

        if not os.path.isfile(input_manifest):
            return None, None

        with open(input_manifest, "r", encoding="utf-8") as in_file, \
                open(remote_file_manifest, "a", encoding="utf-8") as remote_file:
            for line in in_file:
                # get the required bdbag remote file manifest vars from each line of the json-stream input file
                entry = json.loads(line)
                entry = self.createManifestEntry(entry)
                if not entry:
                    continue
                remote_file.write(json.dumps(entry) + "\n")
                if self.ro_manifest:
                    ro.add_file_metadata(self.ro_manifest,
                                         source_url=entry["url"],
                                         media_type=entry.get("content_type"),
                                         bundled_as=ro.make_bundled_as(
                                             folder=os.path.dirname(entry["filename"]),
                                             filename=os.path.basename(entry["filename"])))
        os.remove(input_manifest)
        return os.path.relpath(remote_file_manifest, self.base_path), os.path.abspath(remote_file_manifest)

    def createManifestEntry(self, entry):
        manifest_entry = dict()
        url = entry.get("url")
        if not url:
            logging.warning(
                "Skipping a record due to missing required attribute \"url\" in fetch manifest entry %s" %
                json.dumps(entry))
            return

        ext_url = self.getExternalUrl(url)
        length = entry.get("length")
        md5 = entry.get("md5")
        sha256 = entry.get("sha256")
        filename = entry.get("filename") if not self.output_filename else self.output_filename
        content_type = entry.get("content_type")
        content_disposition = None
        # if any required fields are missing from the query result, attempt to get them from the remote server by
        # issuing a HEAD request against the supplied URL
        if not (length and (md5 or sha256)):
            try:
                headers = self.headForHeaders(url, raise_for_status=True)
            except requests.HTTPError as e:
                raise DerivaDownloadError("Exception during HEAD request: %s" % format_exception(e))
            length = headers.get("Content-Length")
            content_type = headers.get("Content-Type")
            content_disposition = headers.get("Content-Disposition")
            if not md5:
                md5 = headers.get("Content-MD5")
                if md5:
                    md5 = decodeBase64toHex(md5)
            if not sha256:
                sha256 = headers.get("Content-SHA256")
                if sha256:
                    sha256 = decodeBase64toHex(sha256)
        # if content length or both hash values are missing, it is a fatal error
        if length is None:
            raise DerivaDownloadError("Could not determine Content-Length for %s" % ext_url)
        if not (md5 or sha256):
            raise DerivaDownloadError("Could not locate an MD5 or SHA256 hash for %s" % ext_url)
        # if a local filename is not provided, try to construct one using content_disposition, if available
        if not filename:
            filename = os.path.basename(url).split(":")[0] if not content_disposition else \
                parse_content_disposition(content_disposition)
        env = self.envars.copy()
        env.update(entry)
        output_path, _ = self.create_paths(self.base_path,
                                           sub_path=self.sub_path,
                                           filename=filename,
                                           is_bag=self.is_bag,
                                           envars=env)
        manifest_entry['url'] = ext_url
        manifest_entry['length'] = int(length)
        manifest_entry['filename'] = output_path
        if md5:
            manifest_entry['md5'] = md5
        if sha256:
            manifest_entry['sha256'] = sha256
        if content_type:
            manifest_entry["content_type"] = content_type
        return manifest_entry

