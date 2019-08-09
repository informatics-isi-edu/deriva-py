import io
import os
import sys
import shutil
import errno
import json
import platform
import logging
import requests
import portalocker
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.packages.urllib3.exceptions import MaxRetryError
from collections import OrderedDict
from distutils import util as du_util

IS_PY2 = (sys.version_info[0] == 2)
IS_PY3 = (sys.version_info[0] == 3)

if IS_PY3:
    from urllib.parse import quote as _urlquote, unquote as urlunquote
    from urllib.parse import urlparse, urlsplit, urlunsplit
    from http.cookiejar import MozillaCookieJar
else:
    from urllib import quote as _urlquote, unquote as urlunquote
    from urlparse import urlparse, urlsplit, urlunsplit
    from cookielib import MozillaCookieJar

Kilobyte = 1024
Megabyte = Kilobyte ** 2
DEFAULT_CHUNK_SIZE = Megabyte * 10  # above the minimum 5MB chunk size for AWS S3 multipart uploads
MAX_CHUNK_SIZE = Megabyte * 100

DEFAULT_HEADERS = {}
DEFAULT_CONFIG_PATH = os.path.join(os.path.expanduser('~'), '.deriva')
DEFAULT_CREDENTIAL_FILE = os.path.join(DEFAULT_CONFIG_PATH, 'credential.json')
DEFAULT_GLOBUS_CREDENTIAL_FILE = os.path.join(DEFAULT_CONFIG_PATH, 'globus-credential.json')
DEFAULT_CONFIG_FILE = os.path.join(DEFAULT_CONFIG_PATH, 'config.json')
DEFAULT_COOKIE_JAR_FILE = os.path.join(DEFAULT_CONFIG_PATH, 'cookies.txt')
DEFAULT_SESSION_CONFIG = {
    "retry_connect": 5,
    "retry_read": 5,
    "retry_backoff_factor": 1.0,
    "retry_status_forcelist": [500, 503, 504],
    "cookie_jar": DEFAULT_COOKIE_JAR_FILE
}
OAUTH2_SCOPES_KEY = "oauth2_scopes"
DEFAULT_CONFIG = {
    "server":
    {
        "protocol": "https",
        "host": platform.uname()[1],
        "catalog_id": 1
    },
    "session": DEFAULT_SESSION_CONFIG,
    "download_processor_whitelist": [],
    OAUTH2_SCOPES_KEY: {}
}
DEFAULT_CREDENTIAL = {}


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
    exc = "".join(("[", type(e).__name__, "] "))
    if isinstance(e, requests.HTTPError):
        resp = " - Server responded: %s" % e.response.text.strip().replace('\n', ': ')
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
                 capture_warnings=True):
    add_logging_level("TRACE", logging.DEBUG-5)
    logging.captureWarnings(capture_warnings)
    if log_format is None:
        log_format = "[%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s:%(funcName)s()] %(message)s" \
            if level <= logging.DEBUG else "%(asctime)s - %(levelname)s - %(message)s"
    if file_path:
        logging.basicConfig(filename=file_path, filemode=file_mode, level=level, format=log_format)
    else:
        logging.basicConfig(level=level, format=log_format)


def get_new_requests_session(url=None, session_config=DEFAULT_SESSION_CONFIG):
    session = requests.session()
    retries = Retry(connect=session_config['retry_connect'],
                    read=session_config['retry_read'],
                    backoff_factor=session_config['retry_backoff_factor'],
                    status_forcelist=session_config['retry_status_forcelist'],
                    method_whitelist=False,
                    raise_on_status=True)
    if url:
        session.mount(url, HTTPAdapter(max_retries=retries))
    else:
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))

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
        if IS_PY2 and isinstance(config_data, str):
            config_data = unicode(config_data, 'utf-8')
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
        with open(config_file) as cf:
            config = cf.read()

    return json.loads(config, object_pairs_hook=OrderedDict)


def lock_file(file_path, mode, exclusive=True):
    return portalocker.Lock(file_path, mode, timeout=60,
                            flags=portalocker.LOCK_EX if exclusive else portalocker.LOCK_SH)


def write_credential(credential_file=DEFAULT_CREDENTIAL_FILE, credential=DEFAULT_CREDENTIAL):
    credential_dir = os.path.dirname(credential_file)
    make_dirs(credential_dir, mode=0o750)
    with lock_file(credential_file, mode='w', exclusive=True) as cf:
        os.chmod(credential_file, 0o600)
        credential_data = json.dumps(credential, ensure_ascii=False, indent=2)
        if IS_PY2 and isinstance(credential_data, str):
            credential_data = unicode(credential_data, 'utf-8')
        cf.write(credential_data)
        cf.flush()
        cf.close()


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
        with lock_file(credential_file, mode='r') as cf:
            credential = cf.read()

    return json.loads(credential, object_pairs_hook=OrderedDict)


def get_credential(host,
                   credential_file=DEFAULT_CREDENTIAL_FILE,
                   globus_credential_file=DEFAULT_GLOBUS_CREDENTIAL_FILE,
                   config_file=DEFAULT_CONFIG_FILE,
                   requested_scope=None,
                   force_scope_lookup=False,
                   match_scope_tag="deriva-all"):
    # load webauthn credentials first
    credentials = read_credential(credential_file or DEFAULT_CREDENTIAL_FILE, create_default=True)
    creds = credentials.get(host, credentials.get(host.lower(), dict()))

    # if present, load globus credentials and merge
    globus_credentials = read_credential(globus_credential_file or DEFAULT_GLOBUS_CREDENTIAL_FILE, create_default=True)
    if globus_credentials:
        scopes = get_oauth_scopes_for_host(host, config_file, force_refresh=force_scope_lookup)
        for resource, g_creds in globus_credentials.items():
            # 1. look for the explicitly requested scope in the token store, if specified
            if requested_scope is not None and g_creds["scope"] == requested_scope:
                creds["bearer-token"] = g_creds["access_token"]
                break

            # 2. try to determine the scope to use based on host-to-scope(s) mappings
            if scopes:
                for k, v in scopes.items():
                    if v == g_creds["scope"]:
                        if match_scope_tag is not None and match_scope_tag != k:
                            continue
                        creds["bearer-token"] = g_creds["access_token"]
                        break
                if creds.get("bearer-token"):
                    break
            else:
                # 3. if we did not find any host-to-scope(s) mappings in the config file, just fall back and
                # try to find a token by matching the hostname to the resource server name
                if host.lower() == resource.lower():
                    creds["bearer-token"] = g_creds["access_token"]
                    break

    return creds or None


def get_oauth_scopes_for_host(host, config_file=DEFAULT_CONFIG_FILE, scheme="https", force_refresh=False):
    config = read_config(config_file or DEFAULT_CONFIG_FILE, create_default=True)
    required_scopes = config.get(OAUTH2_SCOPES_KEY)
    result = dict()
    # determine the scope to use based on host-to-scope(s) mappings in the config file
    if required_scopes:
        for hostname, scopes in required_scopes.items():
            if host.lower() == hostname.lower():
                result = scopes
                break
    if not result or force_refresh:
        session = get_new_requests_session(session_config=DEFAULT_SESSION_CONFIG)
        url = "%s://%s/authn/discovery" % (scheme or "https", host)
        try:
            r = session.get(url, headers=DEFAULT_HEADERS)
            r.raise_for_status()
            result = r.json().get(OAUTH2_SCOPES_KEY)
            if result:
                config[OAUTH2_SCOPES_KEY][host] = result
                write_config(config_file or DEFAULT_CONFIG_FILE, config=config)
        except Exception as e:
            logging.debug("Unable to discover and/or update the \"%s\" mappings from [%s]. As a result, access to "
                          "this host may be limited even though you may already be in possession of a valid login "
                          "token for the relevant scopes. %s" % (OAUTH2_SCOPES_KEY, url, format_exception(e)))
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


def bootstrap():
    init_logging()
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
    # required to find bundled data at runtime in Pyinstaller single-file exe mode
    if getattr(sys, 'frozen', False):
        return os.path.join(getattr(sys, '_MEIPASS', '.'), relative_path)
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
