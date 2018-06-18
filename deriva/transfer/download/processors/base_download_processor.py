import os
import errno
import certifi
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from deriva.core import urlsplit, format_exception, DEFAULT_SESSION_CONFIG
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
            self.query = self.query.format(**self.envars)
        self.base_path = kwargs["base_path"]
        self.store_base = kwargs.get("store_base", "/hatrac/")
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
                                 bundled_as=ro.make_bundled_as())
        return [self.output_relpath]

    def catalogQuery(self, headers=HEADERS):
        output_dir = os.path.dirname(self.output_abspath)
        self.makeDirs(output_dir)
        try:
            return self.catalog.getAsFile(self.query, self.output_abspath, headers=headers)
        except requests.HTTPError as e:
            raise RuntimeError("Unable to execute catalog query: %s" % format_exception(e))

    def headForHeaders(self, url, raise_for_status=False):
        store = self.getHatracStore(url)
        if store:
            r = store.head(url, headers=self.HEADERS)
            if raise_for_status:
                r.raise_for_status()
            headers = r.headers
        else:
            session = self.getExternalSession(urlsplit(url).hostname)
            r = session.head(url, headers=self.HEADERS)
            if raise_for_status:
                r.raise_for_status()
            headers = r.headers

        return headers

    def getHatracStore(self, url):
        urlparts = urlsplit(url)
        if not urlparts.path.startswith(self.store_base):
            return None
        if url.startswith(self.store_base):
            return self.store
        else:
            serverURI = urlparts.scheme + "://" + urlparts.netloc
            if serverURI == self.store.get_server_uri():
                return self.store
            else:
                # do we need to deal with the possibility of a fully qualified URL referencing a different hatrac host?
                raise RuntimeError(
                    "Got a reference to a Hatrac server [%s] that is different from the expected Hatrac server: %s" % (
                        serverURI, self.store.get_server_uri))

    def getExternalUrl(self, url):
        urlparts = urlsplit(url)
        if urlparts.path.startswith(self.store_base):
            path_only = url.startswith(self.store_base)
            serverURI = urlparts.scheme + "://" + urlparts.netloc
            if serverURI == self.store.get_server_uri() or path_only:
                url = ''.join([self.store.get_server_uri(), url]) if path_only else url
        else:
            if not (urlparts.scheme and urlparts.netloc):
                url = ''.join([self.catalog.get_server_uri(), url])

        return url

    def getExternalSession(self, host):
        sessions = self.sessions
        auth_params = self.args.get("auth_params", dict())
        cookies = auth_params.get("cookies")
        auth_url = auth_params.get("auth_url")
        login_params = auth_params.get("login_params")
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

    @staticmethod
    def createPaths(base_path, sub_path=None, ext='', is_bag=False, envars=None):
        relpath = sub_path if sub_path else ''
        if not os.path.splitext(sub_path)[1][1:]:
            relpath += ext
        if isinstance(envars, dict):
            relpath = relpath.format(**envars)

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
