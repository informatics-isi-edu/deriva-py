import io
import os
import sys
import shutil
import errno
import json
import math
import platform
import logging
import requests
import portalocker
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import OrderedDict
from distutils import util as du_util
from urllib.parse import quote as _urlquote, unquote as urlunquote
from urllib.parse import urlparse, urlsplit, urlunsplit, urljoin
from http.cookiejar import MozillaCookieJar


Kilobyte = 1024
Megabyte = Kilobyte ** 2

# Set the default chunk size above the minimum 5MB chunk size for AWS S3 multipart uploads, but also try to find a sweet
# spot between a minimal number of chunks and payloads big enough to be efficient but not incur retries for slow clients
# or situations where network connectivity is unreliable for whatever reason.
DEFAULT_CHUNK_SIZE = Megabyte * 25
# 5GB is the max chunk limit imposed by AWS S3, also a (generous) hard limit.
HARD_MAX_CHUNK_SIZE = Megabyte * 1000 * 5
# Max number of "parts" for AWS S3.
DEFAULT_MAX_CHUNK_LIMIT = 10000
# A practical default limit for single request body payload size, similar to AWS S3 recommendation for payload sizes.
DEFAULT_MAX_REQUEST_SIZE = Megabyte * 100

DEFAULT_HEADERS = {}
DEFAULT_CONFIG_PATH = os.path.join(os.path.expanduser('~'), '.deriva')
DEFAULT_CREDENTIAL_FILE = os.path.join(DEFAULT_CONFIG_PATH, 'credential.json')
DEFAULT_GLOBUS_CREDENTIAL_FILE = os.path.join(DEFAULT_CONFIG_PATH, 'globus-credential.json')
DEFAULT_CONFIG_FILE = os.path.join(DEFAULT_CONFIG_PATH, 'config.json')
DEFAULT_COOKIE_JAR_FILE = os.path.join(DEFAULT_CONFIG_PATH, 'cookies.txt')
DEFAULT_REQUESTS_TIMEOUT = (6, 63)  # (connect, read), integer in seconds
DEFAULT_SESSION_CONFIG = {
    "timeout": DEFAULT_REQUESTS_TIMEOUT,
    "retry_connect": 2,
    "retry_read": 4,
    "retry_backoff_factor": 1.0,
    "retry_status_forcelist": [500, 502, 503, 504],
    "allow_retry_on_all_methods": False,
    "cookie_jar": DEFAULT_COOKIE_JAR_FILE,
    "max_request_size": DEFAULT_MAX_REQUEST_SIZE,
    "max_chunk_limit": DEFAULT_MAX_CHUNK_LIMIT
}
OAUTH2_SCOPES_KEY = "oauth2_scopes"
DEFAULT_CONFIG = {
    "server":
    {
        "protocol": "https",
        "host": 'localhost',
        "catalog_id": 1
    },
    "session": DEFAULT_SESSION_CONFIG,
    "download_processor_whitelist": [],
    OAUTH2_SCOPES_KEY: {}
}
DEFAULT_CREDENTIAL = {}
DEFAULT_LOGGER_OVERRIDES = {
    "globus_sdk": logging.WARNING,
    "boto3": logging.WARNING,
    "botocore": logging.WARNING,
}


class NotModified (ValueError):
    pass


class ConcurrentUpdate (ValueError):
    pass


def urlquote(s, safe=''):
    """Quote all reserved characters according to RFC3986 unless told otherwise.

       The urllib.urlquote has a weird default which excludes '/' from
       quoting even though it is a reserved character.  We would never
       want this when encoding elements in Deriva REST API URLs, so
       this wrapper changes the default to have no declared safe
       characters.

    """
    return _urlquote(s.encode('utf-8'), safe=safe)


def urlquote_dcctx(s, safe='~{}",:'):
    """Quote for use with Deriva-Client-Context or other HTTP headers.

       Defaults to allow additional safe characters for less
       aggressive encoding of JSON content for use in an HTTP header
       value.

    """
    return urlquote(s, safe=safe)


def stob(string):
    return bool(du_util.strtobool(str(string)))


def format_exception(e):
    if not isinstance(e, Exception):
        return str(e)
    exc = "".join(("[", type(e).__name__, "] "))
    if isinstance(e, requests.HTTPError):
        resp = " - Server responded: %s" % e.response.text.strip().replace('\n', ': ') if e.response.text else ""
        return "".join((exc, str(e), resp))
    return "".join((exc, str(e)))


def add_logging_level(level_name, level_num, method_name=None):
    if not method_name:
        method_name = level_name.lower()

    if hasattr(logging, level_name):
        logging.warning('{} already defined in logging module'.format(level_name))
        return
    if hasattr(logging, method_name):
        logging.warning('{} already defined in logging module'.format(method_name))
        return
    if hasattr(logging.getLoggerClass(), method_name):
        logging.warning('{} already defined in logger class'.format(method_name))
        return

    def log_for_level(self, message, *args, **kwargs):
        if self.isEnabledFor(level_num):
            self._log(level_num, message, args, **kwargs)

    def log_to_root(message, *args, **kwargs):
        logging.log(level_num, message, *args, **kwargs)

    logging.addLevelName(level_num, level_name)
    setattr(logging, level_name, level_num)
    setattr(logging.getLoggerClass(), method_name, log_for_level)
    setattr(logging, method_name, log_to_root)


def init_logging(level=logging.INFO,
                 log_format=None,
                 file_path=None,
                 file_mode='w',
                 capture_warnings=True,
                 logger_config=DEFAULT_LOGGER_OVERRIDES):
    add_logging_level("TRACE", logging.DEBUG-5)
    logging.captureWarnings(capture_warnings)
    if log_format is None:
        log_format = "[%(asctime)s - %(levelname)s - %(name)s:%(filename)s:%(lineno)s:%(funcName)s()] %(message)s" \
            if level <= logging.DEBUG else "%(asctime)s - %(levelname)s - %(message)s"
    # allow for reconfiguration of module-specific logging levels
    [logging.getLogger(name).setLevel(level) for name, level in logger_config.items()]
    if file_path:
        logging.basicConfig(filename=file_path, filemode=file_mode, level=level, format=log_format)
    else:
        logging.basicConfig(level=level, format=log_format)


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_REQUESTS_TIMEOUT
        if "timeout" in kwargs:
            timeout = kwargs["timeout"]
            self.timeout = tuple(timeout) if isinstance(timeout, list) else timeout
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


def get_new_requests_session(url=None, session_config=DEFAULT_SESSION_CONFIG):
    session = requests.session()
    retries = Retry(connect=session_config['retry_connect'],
                    read=session_config['retry_read'],
                    backoff_factor=session_config['retry_backoff_factor'],
                    status_forcelist=session_config['retry_status_forcelist'],
                    allowed_methods=Retry.DEFAULT_ALLOWED_METHODS if
                    # Passing False to method_whitelist means allow all methods
                    not session_config.get("allow_retry_on_all_methods", False) else False,
                    raise_on_status=True)
    adapter = TimeoutHTTPAdapter(timeout=session_config.get("timeout", DEFAULT_REQUESTS_TIMEOUT), max_retries=retries)
    if url:
        session.mount(url, adapter)
    else:
        session.mount('http://', adapter)
        session.mount('https://', adapter)

    return session


def make_dirs(path, mode=0o777):
    if not os.path.isdir(path):
        try:
            os.makedirs(path, mode=mode)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise


def copy_config(src, dst):
    config_dir = os.path.dirname(dst)
    make_dirs(config_dir, mode=0o750)
    shutil.copy2(src, dst)


def write_config(config_file=DEFAULT_CONFIG_FILE, config=DEFAULT_CONFIG):
    config_dir = os.path.dirname(config_file)
    make_dirs(config_dir, mode=0o750)
    with io.open(config_file, 'w', newline='\n', encoding='utf-8') as cf:
        config_data = json.dumps(config, ensure_ascii=False, indent=2)
        cf.write(config_data)
        cf.close()


def read_config(config_file=DEFAULT_CONFIG_FILE, create_default=False, default=DEFAULT_CONFIG):
    if not config_file:
        config_file = DEFAULT_CONFIG_FILE
    config = None
    if not os.path.isfile(config_file) and create_default:
        logging.info("No default configuration file found, attempting to create one at: %s" % config_file)
        try:
            write_config(config_file, default)
        except Exception as e:
            logging.warning("Unable to create configuration file %s. Using internal defaults. %s" %
                            (config_file, format_exception(e)))
            config = json.dumps(default, ensure_ascii=False)

    if not config:
        with open(config_file, encoding='utf-8') as cf:
            config = cf.read()

    return json.loads(config, object_pairs_hook=OrderedDict)


def lock_file(file_path, mode, exclusive=True, timeout=60):
    return portalocker.Lock(file_path, mode=mode, timeout=timeout, fail_when_locked=True,
                            flags=(portalocker.LOCK_EX | portalocker.LOCK_NB) if exclusive else
                            (portalocker.LOCK_SH | portalocker.LOCK_NB))


def write_credential(credential_file=DEFAULT_CREDENTIAL_FILE, credential=DEFAULT_CREDENTIAL):
    credential_dir = os.path.dirname(credential_file)
    make_dirs(credential_dir, mode=0o750)
    with lock_file(credential_file, mode='w', exclusive=True) as cf:
        os.chmod(credential_file, 0o600)
        credential_data = json.dumps(credential, ensure_ascii=False, indent=2)
        cf.write(credential_data)
        cf.flush()
        os.fsync(cf.fileno())


def read_credential(credential_file=DEFAULT_CREDENTIAL_FILE, create_default=False, default=DEFAULT_CREDENTIAL):
    if not credential_file:
        credential_file = DEFAULT_CREDENTIAL_FILE
    credential = None
    if not os.path.isfile(credential_file) and create_default:
        logging.info("No default credential file found, attempting to create one at: %s" % credential_file)
        try:
            write_credential(credential_file, default)
        except Exception as e:
            logging.warning("Unable to create credential file %s. Using internal defaults. %s" %
                            (credential_file, format_exception(e)))
            credential = json.dumps(default, ensure_ascii=False)

    if not credential:
        with lock_file(credential_file, mode='r', exclusive=False) as cf:
            credential = cf.read()

    return json.loads(credential, object_pairs_hook=OrderedDict)


def get_oauth_scopes_for_host(host,
                              config_file=DEFAULT_CONFIG_FILE,
                              force_refresh=False,
                              warn_on_discovery_failure=False):
    config = read_config(config_file or DEFAULT_CONFIG_FILE, create_default=True)
    required_scopes = config.get(OAUTH2_SCOPES_KEY)
    result = dict()
    upr = urlparse(host)
    if upr.scheme and upr.netloc:
        if upr.scheme not in ("http", "https"):
            return result
        url = urljoin(host, "/authn/discovery")
        host = upr.hostname
    else:
        url = "https://%s/authn/discovery" % host
    # determine the scope to use based on host-to-scope(s) mappings in the config file
    if required_scopes:
        for hostname, scopes in required_scopes.items():
            if host.lower() == hostname.lower():
                result = scopes
                break
    if not result or force_refresh:
        session = get_new_requests_session(session_config=DEFAULT_SESSION_CONFIG)
        try:
            r = session.get(url, headers=DEFAULT_HEADERS)
            r.raise_for_status()
            result = r.json().get(OAUTH2_SCOPES_KEY)
            if result:
                if not config.get(OAUTH2_SCOPES_KEY):
                    config[OAUTH2_SCOPES_KEY] = {}
                config[OAUTH2_SCOPES_KEY].update({host: result})
                write_config(config_file or DEFAULT_CONFIG_FILE, config=config)
        except Exception as e:
            msg = "Unable to discover and/or update the \"%s\" mappings from [%s]. Requests to this host that " \
                  "use OAuth2 scope-specific bearer tokens may fail or provide only limited access. %s" % \
                  (OAUTH2_SCOPES_KEY, url, format_exception(e))
            if warn_on_discovery_failure:
                logging.warning(msg)
            else:
                logging.debug(msg)
        finally:
            session.close()
    return result


def format_credential(token=None, oauth2_token=None, username=None, password=None):
    if username and password:
        return {"username": username, "password": password}
    credential = dict()
    if token:
        credential.update({"cookie": "webauthn=%s" % token})
    if oauth2_token:
        credential.update({"bearer-token": "%s" % oauth2_token})
    if not credential:
        raise ValueError(
            "Missing required argument(s): a supported authentication token or a username/password must be provided.")

    return credential


def bootstrap(logging_level=logging.INFO):
    init_logging(level=logging_level)
    read_config(create_default=True)
    read_credential(create_default=True)


def load_cookies_from_file(cookie_file=None):
    if not cookie_file:
        cookie_file = DEFAULT_SESSION_CONFIG["cookie_jar"]
    cookies = MozillaCookieJar()
    # Load and return saved cookies if existing
    if os.path.isfile(cookie_file):
        try:
            cookies.load(cookie_file, ignore_discard=True, ignore_expires=True)
            return cookies
        except Exception as e:
            logging.warning(format_exception(e))

    # Create new empty cookie file otherwise
    cookies.save(cookie_file, ignore_discard=True, ignore_expires=True)
    os.chmod(cookie_file, 0o600)

    return cookies


def resource_path(relative_path, default=os.path.abspath(".")):
    if default is None:
        return relative_path
    return os.path.join(default, relative_path)


def get_transfer_summary(total_bytes, elapsed_time):
    total_secs = elapsed_time.total_seconds()
    transferred = \
        float(total_bytes) / float(Kilobyte) if total_bytes < Megabyte else float(total_bytes) / float(Megabyte)
    throughput = str(" at %.2f MB/second" % (transferred / total_secs)) if (total_secs >= 1) else ""
    elapsed = str("Elapsed time: %s." % elapsed_time) if (total_secs > 0) else ""
    summary = "%.2f %s transferred%s. %s" % \
              (transferred, "KB" if total_bytes < Megabyte else "MB", throughput, elapsed)
    return summary


def calculate_optimal_transfer_shape(size,
                                     chunk_limit=DEFAULT_MAX_CHUNK_LIMIT,
                                     requested_chunk_size=DEFAULT_CHUNK_SIZE,
                                     byte_align=Kilobyte * 64):
    if size == 0:
        return DEFAULT_CHUNK_SIZE, 1, 0
    if size < 0:
        raise ValueError('Parameter "size" cannot be negative')
    if chunk_limit <= 0:
        raise ValueError('Parameter "chunk_limit" must be greater than zero')
    if byte_align <= 0:
        raise ValueError('Parameter "byte_align" must be greater than zero')

    if size > (HARD_MAX_CHUNK_SIZE * chunk_limit):
        raise ValueError("Size %d exceeds limit for max chunk size %d and chunk count %d" %
                         (size, HARD_MAX_CHUNK_SIZE, chunk_limit))
    calculated_chunk_size = math.ceil(math.ceil(size / chunk_limit) / byte_align) * byte_align
    chosen_chunk_size = min(HARD_MAX_CHUNK_SIZE, max(requested_chunk_size, calculated_chunk_size))
    remainder = size % chosen_chunk_size
    chunk_count = size // chosen_chunk_size
    if remainder:
        chunk_count += 1
    return chosen_chunk_size, chunk_count, remainder


def json_item_handler(input_file, callback):
    with io.open(input_file, "r", encoding='utf-8') as infile:
        line = infile.readline().lstrip()
        infile.seek(0)
        is_json_stream = False
        if line.startswith('{') and line.endswith('}\n'):
            input_json = infile
            is_json_stream = True
        else:
            input_json = json.load(infile, object_pairs_hook=OrderedDict)
        try:
            for item in input_json:
                if is_json_stream:
                    item = json.loads(item, object_pairs_hook=OrderedDict)
                if callback:
                    callback(item)
        finally:
            infile.close()


def topo_sorted(depmap):
    """Return list of items topologically sorted.

       depmap: { item: [required_item, ...], ... }

    Raises ValueError if a required_item cannot be satisfied in any order.

    The per-item required_item iterables must allow revisiting on
    multiple iterations.

    """
    ordered = [ item for item, requires in depmap.items() if not requires ]
    depmap = { item: set(requires) for item, requires in depmap.items() if requires }
    satisfied = set(ordered)
    while depmap:
        additions = []
        for item, requires in list(depmap.items()):
            if requires.issubset(satisfied):
                additions.append(item)
                satisfied.add(item)
                del depmap[item]
        if not additions:
            raise ValueError(("unsatisfiable", depmap))
        ordered.extend(additions)
        additions = []
    return ordered


class AttrDict (dict):
    """Dictionary with optional attribute-based lookup.

       For keys that are valid attributes, self.key is equivalent to
       self[key].
    """
    def __getattr__(self, a):
        try:
            return self[a]
        except KeyError as e:
            raise AttributeError(str(e))

    def __setattr__(self, a, v):
        self[a] = v

    def update(self, d):
        dict.update(self, d)


# convenient enumeration of common annotation tags
tag = AttrDict({
    'display':            'tag:misd.isi.edu,2015:display',
    'table_alternatives': 'tag:isrd.isi.edu,2016:table-alternatives',
    'column_display':     'tag:isrd.isi.edu,2016:column-display',
    'key_display':        'tag:isrd.isi.edu,2017:key-display',
    'foreign_key':        'tag:isrd.isi.edu,2016:foreign-key',
    'generated':          'tag:isrd.isi.edu,2016:generated',
    'immutable':          'tag:isrd.isi.edu,2016:immutable',
    'non_deletable':      'tag:isrd.isi.edu,2016:non-deletable',
    'app_links':          'tag:isrd.isi.edu,2016:app-links',
    'table_display':      'tag:isrd.isi.edu,2016:table-display',
    'visible_columns':    'tag:isrd.isi.edu,2016:visible-columns',
    'visible_foreign_keys': 'tag:isrd.isi.edu,2016:visible-foreign-keys',
    'export':             'tag:isrd.isi.edu,2016:export',
    'export_2019':        'tag:isrd.isi.edu,2019:export',
    'export_fragment_definitions': 'tag:isrd.isi.edu,2021:export-fragment-definitions',    
    'asset':              'tag:isrd.isi.edu,2017:asset',
    'citation':           'tag:isrd.isi.edu,2018:citation',
    'required':           'tag:isrd.isi.edu,2018:required',
    'indexing_preferences': 'tag:isrd.isi.edu,2018:indexing-preferences',
    'bulk_upload':        'tag:isrd.isi.edu,2017:bulk-upload',
    'chaise_config':      'tag:isrd.isi.edu,2019:chaise-config',
    'source_definitions': 'tag:isrd.isi.edu,2019:source-definitions',
    'indexing_preferences': 'tag:isrd.isi.edu,2018:indexing-preferences',
    'google_dataset':     'tag:isrd.isi.edu,2021:google-dataset',
    'column_defaults':    'tag:isrd.isi.edu,2023:column-defaults',
    'viz_3d_display':     'tag:isrd.isi.edu,2021:viz-3d-display',
})
