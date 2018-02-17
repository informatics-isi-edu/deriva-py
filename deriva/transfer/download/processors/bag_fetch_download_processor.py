import os
import json
import logging
from bdbag import bdbag_ro as ro
from deriva.core.utils.hash_utils import decodeBase64toHex
from deriva.core.utils.mime_utils import parse_content_disposition
from deriva.transfer.download.processors import BaseDownloadProcessor


class BagFetchDownloadProcessor(BaseDownloadProcessor):
    def __init__(self, envars=None, **kwargs):
        super(BagFetchDownloadProcessor, self).__init__(envars, **kwargs)
        self.content_type = "application/x-json-stream"
        self.output_relpath, self.output_abspath = self.createPaths(self.base_path, "fetch-manifest.json")
        self.ro_file_provenance = False

    def process(self):
        super(BagFetchDownloadProcessor, self).process()
        self.createRemoteFileManifest()

    def createRemoteFileManifest(self):
        logging.info("Creating remote file manifest")
        input_manifest = self.output_abspath
        remote_file_manifest = self.args.get("remote_file_manifest")
        with open(input_manifest, "r") as in_file, open(remote_file_manifest, "w") as remote_file:
            for line in in_file:
                # get the required bdbag remote file manifest vars from each line of the json-stream input file
                entry = json.loads(line)
                entry = self.createManifestEntry(entry)
                remote_file.write(json.dumps(entry) + "\n")
                if self.ro_manifest:
                    ro.add_file_metadata(self.ro_manifest,
                                         source_url=entry["url"],
                                         media_type=entry.get("content_type"),
                                         bundled_as=ro.make_bundled_as(
                                             folder=os.path.dirname(entry["filename"]),
                                             filename=os.path.basename(entry["filename"])))
        os.remove(input_manifest)

    def createManifestEntry(self, entry):
        manifest_entry = dict()
        sub_path = self.sub_path
        url = entry.get("url")
        if not url:
            raise RuntimeError("Missing required attribute \"url\" in download manifest entry %s" % json.dumps(entry))

        length = entry.get("length")
        md5 = entry.get("md5")
        sha256 = entry.get("sha256")
        filename = entry.get("filename")
        content_type = entry.get("content_type")
        content_disposition = None
        # if any required fields are missing from the query result, attempt to get them from the remote server by
        # issuing a HEAD request against the supplied URL
        if not (length and (md5 or sha256)):
            headers = self.headForHeaders(url, raise_for_status=True)
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
        if not length:
            raise RuntimeError("Could not determine Content-Length for %s" % url)
        if not (md5 or sha256):
            raise RuntimeError("Could not locate an MD5 or SHA256 hash for %s" % url)
        # if a local relative path/filename are not provided, try to construct one using content_disposition, if avail
        if not filename:
            envvars = self.envars.copy()
            envvars.update(entry)
            subdir = sub_path % envvars
            filename = os.path.basename(url).split(":")[0] if not content_disposition else \
                parse_content_disposition(content_disposition)
            output_path = ''.join([subdir, "/", filename]) if subdir else filename
        else:
            output_path = filename

        manifest_entry['url'] = self.getExternalUrl(url)
        manifest_entry['length'] = int(length)
        manifest_entry['filename'] = output_path
        if md5:
            manifest_entry['md5'] = md5
        if sha256:
            manifest_entry['sha256'] = sha256
        if content_type:
            manifest_entry["content_type"] = content_type
        return manifest_entry

