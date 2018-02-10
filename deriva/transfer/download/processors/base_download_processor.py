import os
import errno
import uuid
from bdbag import bdbag_ro as ro


class BaseDownloadProcessor(object):
    """
    Base class for DownloadProcessor classes
    """
    HEADERS = {'Connection': 'keep-alive'}

    def __init__(self, envars=None, **kwargs):
        self.args = kwargs
        self.envars = envars if envars else dict()
        self.catalog = kwargs["catalog"]
        self.store = kwargs["store"]
        self.query = kwargs["query"]
        if self.envars:
            self.query = self.query % self.envars
        self.base_path = kwargs["base_path"]
        self.is_bag = kwargs.get("bag", False)
        self.sub_path = kwargs.get("sub_path", "")
        self.sessions = kwargs.get("sessions", dict())
        self.format_args = kwargs.get("format_args", dict())
        self.content_type = "application/octet-stream"
        self.url = ''.join([self.catalog.get_server_uri(), self.query])
        self.ro_file_provenance = True
        self.ro_manifest = self.args.get("ro_manifest")
        self.ro_author_name = self.args.get("ro_author_name")
        self.ro_author_orcid = self.args.get("ro_author_orcid")
        self.output_relpath = None
        self.output_abspath = None

    def process(self):
        headers = self.HEADERS
        headers.update({'accept': self.content_type})
        resp = self.catalogQuery(headers)

        if self.ro_manifest and self.ro_file_provenance:
            ro.add_file_metadata(self.ro_manifest,
                                 source_url=self.url,
                                 local_path=self.output_relpath,
                                 media_type=self.content_type,
                                 retrieved_on=ro.make_retrieved_on(),
                                 retrieved_by=ro.make_retrieved_by(self.ro_author_name, orcid=self.ro_author_orcid),
                                 bundled_as=ro.make_bundled_as(uri="urn:uuid:%s" % str(uuid.uuid4())))

    def catalogQuery(self, headers=HEADERS):
        output_dir = os.path.dirname(self.output_abspath)
        self.makeDirs(output_dir)
        r = self.catalog.getAsFile(self.query, self.output_abspath, headers=headers)

        return r

    @staticmethod
    def createPaths(base_path, sub_path=None, ext='', is_bag=False, envars=None):
        relpath = sub_path if sub_path else ''
        if not os.path.splitext(sub_path)[1][1:]:
            relpath += ext
        if isinstance(envars, dict):
            relpath = relpath % envars

        abspath = os.path.abspath(
            os.path.join(base_path, 'data' if is_bag else '', relpath))

        return relpath, abspath

    @staticmethod
    def makeDirs(path):
        if not os.path.isdir(path):
            try:
                os.makedirs(path)
            except OSError as error:
                if error.errno != errno.EEXIST:
                    raise


class CSVDownloadProcessor(BaseDownloadProcessor):
    def __init__(self, envars=None, **kwargs):
        super(CSVDownloadProcessor, self).__init__(envars, **kwargs)
        self.ext = ".csv"
        self.content_type = "text/csv"
        self.output_relpath, self.output_abspath = self.createPaths(
            self.base_path, self.sub_path, ext=self.ext, is_bag=self.is_bag, envars=envars)


class JSONDownloadProcessor(BaseDownloadProcessor):
    def __init__(self, envars=None, **kwargs):
        super(JSONDownloadProcessor, self).__init__(envars, **kwargs)
        self.ext = ".json"
        self.content_type = "application/json"
        self.output_relpath, self.output_abspath = self.createPaths(
            self.base_path, self.sub_path, ext=self.ext, is_bag=self.is_bag, envars=envars)


class JSONStreamDownloadProcessor(BaseDownloadProcessor):
    def __init__(self, envars=None, **kwargs):
        super(JSONStreamDownloadProcessor, self).__init__(envars, **kwargs)
        self.ext = ".json"
        self.content_type = "application/x-json-stream"
        self.output_relpath, self.output_abspath = self.createPaths(
            self.base_path, self.sub_path, ext=self.ext, is_bag=self.is_bag, envars=envars)
