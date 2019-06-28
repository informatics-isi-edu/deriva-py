import os
import errno
import certifi
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from deriva.core import urlsplit, get_new_requests_session, stob, make_dirs, DEFAULT_SESSION_CONFIG
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError, \
    DerivaDownloadAuthenticationError, DerivaDownloadAuthorizationError
from deriva.transfer.download.processors.base_processor import BaseProcessor, LOCAL_PATH_KEY, SOURCE_URL_KEY
from bdbag import bdbag_ro as ro


class BaseQueryProcessor(BaseProcessor):
    """
    Base class for QueryProcessor classes
    """
    HEADERS = {'Connection': 'keep-alive'}

    def __init__(self, envars=None, **kwargs):
        super(BaseQueryProcessor, self).__init__(envars, **kwargs)
        self.catalog = kwargs["catalog"]
        self.store = kwargs["store"]
        self.base_path = kwargs["base_path"]
        self.query = self.parameters["query_path"]
        if self.envars:
            self.query = self.query.format(**self.envars)
        self.sub_path = self.parameters.get("output_path", "")
        self.store_base = kwargs.get("store_base", "/hatrac/")
        self.is_bag = kwargs.get("bag", False)
        self.sessions = kwargs.get("sessions", dict())
        self.content_type = "application/octet-stream"
        self.url = ''.join([self.catalog.get_server_uri(), self.query])
        self.ro_file_provenance = stob(self.parameters.get("ro_file_provenance", False if not self.is_bag else True))
        self.ro_manifest = self.kwargs.get("ro_manifest")
        self.ro_author_name = self.kwargs.get("ro_author_name")
        self.ro_author_orcid = self.kwargs.get("ro_author_orcid")
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

        self.outputs.update({self.output_relpath: {LOCAL_PATH_KEY: self.output_abspath, SOURCE_URL_KEY: self.url}})
        return self.outputs

    def catalogQuery(self, headers=HEADERS, as_file=True):
        if as_file:
            output_dir = os.path.dirname(self.output_abspath)
            make_dirs(output_dir)
        try:
            if as_file:
                return self.catalog.getAsFile(self.query, self.output_abspath, headers=headers, delete_if_empty=True)
            else:
                return self.catalog.get(self.query, headers=headers).json()
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                raise DerivaDownloadAuthenticationError(e)
            if e.response.status_code == 403:
                raise DerivaDownloadAuthorizationError(e)
            os.remove(self.output_abspath)
            raise DerivaDownloadError("Error executing catalog query: %s" % e)
        except Exception:
            os.remove(self.output_abspath)
            raise

    def headForHeaders(self, url, raise_for_status=False):
        store = self.getHatracStore(url)
        if store:
            r = store.head(url, headers=self.HEADERS)
            if raise_for_status:
                r.raise_for_status()
            headers = r.headers
        else:
            url = self.getExternalUrl(url)
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
                raise DerivaDownloadConfigurationError(
                    "Got a reference to a Hatrac server [%s] that is different from the expected Hatrac server: %s" % (
                        serverURI, self.store.get_server_uri))

    def getExternalUrl(self, url):
        urlparts = urlsplit(url)
        if urlparts.path.startswith(self.store_base):
            path_only = url.startswith(self.store_base)
            server_uri = urlparts.scheme + "://" + urlparts.netloc
            if server_uri == self.store.get_server_uri() or path_only:
                url = ''.join([self.store.get_server_uri(), url]) if path_only else url
        else:
            if not (urlparts.scheme and urlparts.netloc):
                urlparts = urlsplit(self.catalog.get_server_uri())
                server_uri = urlparts.scheme + "://" + urlparts.netloc
                url = ''.join([server_uri, url])

        return url

    def getExternalSession(self, host):
        sessions = self.sessions
        auth_params = self.kwargs.get("auth_params", dict())
        cookies = auth_params.get("cookies")
        auth_url = auth_params.get("auth_url")
        login_params = auth_params.get("login_params")
        session_config = self.kwargs.get("session_config")

        session = sessions.get(host)
        if session is not None:
            return session

        if not session_config:
            session_config = DEFAULT_SESSION_CONFIG
        session = get_new_requests_session(session_config=session_config)

        if cookies:
            session.cookies.update(cookies)
        if login_params and auth_url:
            r = session.post(auth_url, data=login_params, verify=certifi.where())
            if r.status_code > 203:
                raise DerivaDownloadError(
                    'GetExternalSession Failed with Status Code: %s\n%s\n' % (r.status_code, r.text))

        sessions[host] = session
        return session


class CSVQueryProcessor(BaseQueryProcessor):
    def __init__(self, envars=None, **kwargs):
        super(CSVQueryProcessor, self).__init__(envars, **kwargs)
        self.ext = ".csv"
        self.content_type = "text/csv"
        self.output_relpath, self.output_abspath = self.create_paths(
            self.base_path, self.sub_path, ext=self.ext, is_bag=self.is_bag, envars=envars)


class JSONQueryProcessor(BaseQueryProcessor):
    def __init__(self, envars=None, **kwargs):
        super(JSONQueryProcessor, self).__init__(envars, **kwargs)
        self.ext = ".json"
        self.content_type = "application/json"
        self.output_relpath, self.output_abspath = self.create_paths(
            self.base_path, self.sub_path, ext=self.ext, is_bag=self.is_bag, envars=envars)


class JSONStreamQueryProcessor(BaseQueryProcessor):
    def __init__(self, envars=None, **kwargs):
        super(JSONStreamQueryProcessor, self).__init__(envars, **kwargs)
        self.ext = ".json"
        self.content_type = "application/x-json-stream"
        self.output_relpath, self.output_abspath = self.create_paths(
            self.base_path, self.sub_path, ext=self.ext, is_bag=self.is_bag, envars=envars)


class JSONEnvUpdateProcessor(BaseQueryProcessor):
    def __init__(self, envars=None, **kwargs):
        super(JSONEnvUpdateProcessor, self).__init__(envars, **kwargs)

    def process(self):
        headers = self.HEADERS
        headers.update({'accept': "application/json"})
        resp = self.catalogQuery(headers, as_file=False)
        if resp:
            self.envars.update(resp[0])
            self._urlencode_envars()
        return {}
