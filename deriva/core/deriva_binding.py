
import uuid
import sys
import os
import json
import requests
from multiprocessing import Queue
from . import get_new_requests_session, urlquote_dcctx, ConcurrentUpdate, NotModified, DEFAULT_HEADERS, DEFAULT_SESSION_CONFIG


class DerivaClientContext (dict):
    """Represent Deriva-Client-Context header content.

    Well-known keys (originally defined for Chaise):

    - `cid`: client application ID i.e. program name

    - `wid`: window ID i.e. request stream ID

    - `pid`: page ID i.e. sub-stream ID

    - `action`: UX action embodied by request(s)

    - `table`: table upon which application is focused (if any)

    - `uinit`: True if request initiated by user action

    Default values to use process-wide:

    - `cid`:code:: os.path.basename(sys.argv[0]) if available

    - `wid`:code:: a random UUID

    The process-wide defaults MAY be customized by mutating
    `DerivaClientContext.defaults`:code: prior to constructing instances.

    """
    defaults = {
        'cid': os.path.basename(sys.argv[0]) if len(sys.argv) > 0 and sys.argv[0] else None,
        'wid': uuid.uuid4().hex,
    }

    def __init__(self, *args, **kwargs):
        """Initialize DerivaClientContext from keywords or dict-like.

           Class-wide defaults are used for fields not set explicitly during construction.
        """
        super(DerivaClientContext, self).__init__(*args, **kwargs)
        self.set_defaults()
        self.prune()

    def set_defaults(self, defaults=None):
        """Set default key-values in self if key not already set."""
        if defaults is None:
            defaults = self.defaults
        for k, v in defaults.items():
            if v is not None and (k not in self or self[k] is None):
                self[k] = v

    def prune(self):
        """Prune redundant keys to shorten context representation.

           Keys with None value or uinit=False are unnecessary as these
           are implicitly assumed when absent.

        """
        for k, v in self.items():
            if v is None:
                del self[k]
        if 'uinit' in self and not self['uinit']:
            del self['uinit']

    def encoded(self):
        """Encode self as string suitable for Deriva-Client-Context HTTP header."""
        self.set_defaults()
        self.prune()
        x = urlquote_dcctx(json.dumps(self, indent=None, separators=(',', ':')))
        return x

    def merged(self, overrides):
        c = DerivaClientContext(self)
        c.update(overrides)
        return c

class DerivaPathError (ValueError):
    pass

def _response_raise_for_status(self):
    """Raises requests.HTTPError if status code indicates an error.

    This unbound method can be monkey-patched onto a requests.Response
    instance or manually invoked on one.

    """
    if 400 <= self.status_code < 600:
        details = " Details: %s"
        raise requests.HTTPError(
            u'%s %s Error: %s for url: [%s]%s' % (
                self.status_code,
                'Client' if self.status_code < 500 else 'Server',
                self.reason,
                self.url,
                details % self.content if self.content else "",
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

           Deriva Client Context: You MAY mutate self.dcctx to
           customize the context for this service endpoint prior to
           invoking web requests.  E.g.:

             self.dcctx['cid'] = 'my application name'

           You MAY also supply custom per-request context by passing a
           headers dict to web request methods, e.g. 

             self.get(..., headers={'deriva-client-context': {'action': 'myapp/function1'}})

           This custom header will be merged as override values with
           the default context in self.dcctx in order to form the
           complete context for the request.

        """
        self._base_server_uri = "%s://%s" % (
            scheme,
            server
        )
        self._server_uri = self._base_server_uri

        self.session_config = DEFAULT_SESSION_CONFIG if not session_config else session_config
        self._session = None
        self._get_new_session(self.session_config)

        self._caching = caching
        self._cache = {}

        self._response_raise_for_status = _response_raise_for_status

        self.dcctx = DerivaClientContext()

        self.set_credentials(credentials, server)

    def get_server_uri(self):
        return self._server_uri

    def _get_new_session(self, session_config=None):
        self._close_session()
        self._session = get_new_requests_session(self._server_uri + '/',
                                                 session_config if session_config else self.session_config)
        # allow loopback requests to bypass SSL cert verification
        if "https://localhost" in self._server_uri:
            self._session.verify = False

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
        headers['deriva-client-context'] = self.dcctx.merged(headers.get('deriva-client-context', {})).encoded()
        return url, headers, prev_response

    def _pre_mutate(self, path, headers, guard_response=None):
        self.check_path(path)
        url = self._server_uri + path
        headers = headers.copy()
        if guard_response and 'etag' in guard_response.headers:
            headers['If-Match'] = guard_response.headers['etag']
        headers['deriva-client-context'] = self.dcctx.merged(headers.get('deriva-client-context', {})).encoded()
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
        if not credentials:
            return
        assert self._session is not None
        if 'bearer-token' in credentials:
            self._session.headers.update({'Authorization': 'Bearer {token}'.format(token=credentials['bearer-token'])})
        elif 'cookie' in credentials:
            cname, cval = credentials['cookie'].split('=', 1)
            self._session.cookies.set(cname, cval, domain=server, path='/')
        elif 'username' in credentials and 'password' in credentials:
            self.post_authn_session(credentials)

    def get_authn_session(self):
        headers = { 'deriva-client-context': self.dcctx.encoded() }
        r = self._session.get(self._base_server_uri + "/authn/session", headers=headers)
        _response_raise_for_status(r)
        return r

    def post_authn_session(self, credentials):
        headers = { 'deriva-client-context': self.dcctx.encoded() }
        r = self._session.post(self._base_server_uri + "/authn/session", data=credentials, headers=headers)
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

    def _close_session(self):
        if self._session is not None:
            self._session.close()
            self._session = None

    def __del__(self):
        self._close_session()
