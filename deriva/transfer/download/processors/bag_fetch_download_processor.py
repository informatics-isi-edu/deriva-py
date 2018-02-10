import os
import json
import logging
from bdbag import bdbag_ro as ro
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
        sub_path = self.sub_path
        remote_file_manifest = self.args.get("remote_file_manifest")
        with open(input_manifest, "r") as in_file, open(remote_file_manifest, "w") as remote_file:
            for line in in_file:
                entry = json.loads(line)
                if not entry.get('url'):
                    raise RuntimeError("Missing required attribute \"url\" in download manifest line %s" % line)

                url = entry['url']
                length = int(entry['length'])
                if self.store and url.startswith("/hatrac/"):
                    path = url
                    url = ''.join([self.store.get_server_uri(), path])
                    entry['url'] = url
                    if not length:
                        r = self.store.head(path)
                        r.raise_for_status()
                        if r.ok:
                            length = r.headers.get('Content-Length')
                            if length:
                                entry['length'] = length
                if not length:
                    raise RuntimeError("Could not determine Content-Length for %s" % url)
                if not entry.get('filename'):
                    subdir = sub_path % entry
                    filename = os.path.basename(url)
                    output_path = ''.join([subdir, "/", filename])
                    entry['filename'] = output_path
                else:
                    output_path = entry['filename']
                remote_file.write(json.dumps(entry) + "\n")
                if self.ro_manifest:
                    ro.add_file_metadata(self.ro_manifest,
                                         source_url=url,
                                         bundled_as=ro.make_bundled_as(
                                             folder=os.path.dirname(output_path),
                                             filename=os.path.basename(output_path)))
        os.remove(input_manifest)
