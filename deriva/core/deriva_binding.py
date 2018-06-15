import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from multiprocessing import Queue
from . import ConcurrentUpdate, NotModified, DEFAULT_HEADERS, DEFAULT_SESSION_CONFIG


class DerivaPathError (ValueError):
    pass


def _response_raise_for_status(self):
    """Raises requests.HTTPError if status code indicates an error.

    This unbound method can be monkey-patched onto a requests.Response
    instance or manually invoked on one.

    """
    if 400 <= self.status_code < 600:
        raise requests.HTTPError(
            u'%s %s Error: %s for url: %s details: %s' % (
                self.status_code,
                'Client' if self.status_code < 500 else 'Server',
                self.reason,
                self.url,
                self.content,
            ),
            response=self
        )

class DerivaBinding (object):
    """This is a base-class for implementation purposes. Not useful for clients."""

    def __init__(self, scheme, server, credentials=None, caching=True, session_config=None):
        """Create HTTP(S) server binding.

           Arguments:
             scheme: 'http' or 'https'
             server: server FQDN string
             credentials: credential secrets, e.g. cookie
             caching: whether to retain a GET response cache

        """
        self._base_server_uri = "%s://%s" % (
            scheme,
            server
        )
        self._server_uri = self._base_server_uri

        if not session_config:
            session_config = DEFAULT_SESSION_CONFIG
        self._get_new_session(session_config)

        self.set_credentials(credentials, server)

        self._caching = caching
        self._cache = {}

        self._response_raise_for_status = _response_raise_for_status

    def get_server_uri(self):
        return self._server_uri

    def _get_new_session(self, session_config):
        self._session = requests.session()
        retries = Retry(connect=session_config['retry_connect'],
                        read=session_config['retry_read'],
                        backoff_factor=session_config['retry_backoff_factor'],
                        status_forcelist=session_config['retry_status_forcelist'])

        self._session.mount(self._server_uri + '/',
                            HTTPAdapter(max_retries=retries))

    def _pre_get(self, path, headers):
        self.check_path(path)
        url = self._server_uri + path
        headers = headers.copy()
        prev_response = self._cache.get(url)
        if prev_response and 'etag' in prev_response.headers \
           and not ('if-none-match' in headers or 'if-match' in headers):
            headers['if-none-match'] = prev_response.headers['etag']
        else:
            prev_response = None
        return url, headers, prev_response

    def _pre_mutate(self, path, headers, guard_response=None):
        self.check_path(path)
        url = self._server_uri + path
        headers = headers.copy()
        if guard_response and 'etag' in guard_response.headers:
            headers['If-Match'] = guard_response.headers['etag']
        return url, headers

    @staticmethod
    def check_path(path):
        if not path:
            raise DerivaPathError("Path not specified")

        if not path.startswith("/"):
            raise DerivaPathError("Malformed path error (not rooted with \"/\"): %s" % path)

    @staticmethod
    def _raise_for_status_304(r, p, raise_not_modified):
        if r.status_code == 304:
            if raise_not_modified:
                raise NotModified(p or r)
            else:
                return p or r

        _response_raise_for_status(r)
        setattr(r, 'raise_for_status', _response_raise_for_status.__get__(r))
        return r

    @staticmethod
    def _raise_for_status_412(r):
        if r.status_code == 412:
            raise ConcurrentUpdate(r)
        _response_raise_for_status(r)
        setattr(r, 'raise_for_status', _response_raise_for_status.__get__(r))
        return r

    def set_credentials(self, credentials, server):
        assert self._session is not None
        if credentials and ('cookie' in credentials):
            cname, cval = credentials['cookie'].split('=', 1)
            self._session.cookies.set(cname, cval, domain=server, path='/')

    def get_authn_session(self):
        r = self._session.get(self._base_server_uri + "/authn/session")
        _response_raise_for_status(r)
        return r

    def post_authn_session(self, credentials):
        r = self._session.post(self._base_server_uri + "/authn/session", data=credentials)
        _response_raise_for_status(r)
        return r

    def head(self, path, headers=DEFAULT_HEADERS, raise_not_modified=False):
        """Perform HEAD request, returning response object.

           Arguments:
             path: the path within this bound server
             headers: headers to set in request
             raise_not_modified: raise HTTPError for 304 response
               status when true.

           May consult built-in cache and apply 'if-none-match'
           request header unless input headers already include
           'if-none-match' or 'if-match'. On cache hit, returns cached
           response unless raise_not_modified=true. Cached response
           may include content retrieved by GET on the same resource.

        """
        url, headers, prev_response = self._pre_get(path, headers)
        return self._raise_for_status_304(
            self._session.head(url, headers=headers),
            prev_response,
            raise_not_modified
        )
        
    def get(self, path, headers=DEFAULT_HEADERS, raise_not_modified=False, stream=False):
        """Perform GET request, returning response object.

           Arguments:
             path: the path within this bound server
             headers: headers to set in request
             raise_not_modified: raise HTTPError for 304 response
               status when true.
             stream: whether to defer content retrieval to 
               streaming access mode on response object.

           May consult built-in cache and apply 'if-none-match'
           request header unless input headers already include
           'if-none-match' or 'if-match'. On cache hit, returns cached
           response unless raise_not_modified=true.

           Caching of new results is disabled when stream=True.

        """
        if headers is None:
            headers = {}
        url, headers, prev_response = self._pre_get(path, headers)
        r = self._raise_for_status_304(
            self._session.get(url, headers=headers),
            prev_response,
            raise_not_modified
        )
        if self._caching and not stream:
            self._cache[url] = r
        return r

    def post(self, path, data=None, json=None, headers=DEFAULT_HEADERS):
        """Perform POST request, returning response object.

           Arguments:
             path: the path within this bound server
             data: a buffer or file-like content value
             json: data to serialize as JSON content
             headers: headers to set in request

           Raises ConcurrentUpdate for 412 status.

        """
        url, headers = self._pre_mutate(path, headers)
        r = self._session.post(url, data=data, json=json, headers=headers)
        return self._raise_for_status_412(r)

    def put(self, path, data=None, json=None, headers=DEFAULT_HEADERS, guard_response=None):
        """Perform PUT request, returning response object.

           Arguments:
             path: the path within this bound server
             data: a buffer or file-like content value
             json: data to serialize as JSON content
             headers: headers to set in request
             guard_response: expected current resource state
               as previously seen response object.

           Uses guard_response to build appropriate 'if-match' header
           to assure change is only applied to expected state.

           Raises ConcurrentUpdate for 412 status.

        """ 
        url, headers = self._pre_mutate(path, headers, guard_response)
        r = self._session.put(url, data=data, json=json, headers=headers)
        return self._raise_for_status_412(r)
   
    def delete(self, path, headers=DEFAULT_HEADERS, guard_response=None):
        """Perform DELETE request, returning response object.

           Arguments:
             path: the path within this bound server
             headers: headers to set in request
             guard_response: expected current resource state
               as previously seen response object.

           Uses guard_response to build appropriate 'if-match' header
           to assure change is only applied to expected state.

           Raises ConcurrentUpdate for 412 status.

        """
        url, headers = self._pre_mutate(path, headers, guard_response)
        r = self._session.delete(url, headers=headers)
        return self._raise_for_status_412(r)

