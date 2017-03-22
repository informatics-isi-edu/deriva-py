import os
import sys
import errno
import json
import platform
import logging
import requests
from collections import OrderedDict

if sys.version_info > (3,):
    from urllib.parse import quote as urlquote
else:
    from urllib import quote as urlquote

DEFAULT_HEADERS = {}

DEFAULT_CHUNK_SIZE = 1024 ** 2


class NotModified (ValueError):
    pass


class ConcurrentUpdate (ValueError):
    pass


def format_exception(e):
    exc = "".join(("[", type(e).__name__, "] "))
    if isinstance(e, requests.HTTPError):
        resp = " - Server responded: %s" % e.response.text.strip().replace('\n', ': ')
        return "".join((exc, str(e), resp))
    return "".join((exc, str(e)))


def frozendict(d):
    items = list(d.items())
    for i in range(len(items)):
        k, v = items[i]
        if isinstance(v, dict):
            v = frozendict(v)
            items[i] = (k, v)
    return frozenset(set(items))


def init_logging(level=logging.INFO,
                 log_format="%(asctime)s - %(levelname)s - %(message)s",
                 file_path=None,
                 captureWarnings=True):
    logging.captureWarnings(captureWarnings)
    # this will suppress potentially numerous INFO-level "Resetting dropped connection" messages from requests
    logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)
    if file_path:
        logging.basicConfig(filename=file_path, level=level, format=log_format)
    else:
        logging.basicConfig(level=level, format=log_format)


DEFAULT_CONFIG_PATH = os.path.join(os.path.expanduser('~'), '.deriva')
DEFAULT_CREDENTIAL_FILE = os.path.join(DEFAULT_CONFIG_PATH, 'credential.json')
DEFAULT_CONFIG_FILE = os.path.join(DEFAULT_CONFIG_PATH, 'config.json')
DEFAULT_SESSION_CONFIG = {
    "retry_connect": 5,
    "retry_read": 5,
    "retry_backoff_factor": 1.0,
    "retry_status_forcelist": [502, 503, 504]
}
DEFAULT_CONFIG = {
    "server":
    {
        "protocol": "https",
        "host": platform.uname()[1],
        "catalog_id": 1
    },
    "session": DEFAULT_SESSION_CONFIG
}


def create_default_config(config=DEFAULT_CONFIG):
    if not os.path.isdir(DEFAULT_CONFIG_PATH):
        try:
            os.makedirs(DEFAULT_CONFIG_PATH)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
    with open(DEFAULT_CONFIG_FILE, 'w') as cf:
        cf.write(json.dumps(config, sort_keys=True, indent=4, separators=(',', ': ')))
        cf.close()


def read_config(config_file, create_default=True, default=DEFAULT_CONFIG):
    config = json.dumps(DEFAULT_CONFIG)
    if config_file == DEFAULT_CONFIG_FILE and not os.path.isfile(config_file) and create_default:
        logging.debug("No default configuration file found, attempting to create one.")
        try:
            create_default_config(default)
        except Exception as e:
            logging.debug("Unable to create default configuration file %s. Using internal defaults. %s" %
                          (DEFAULT_CONFIG_FILE, format_exception(e)))
    if os.path.isfile(config_file):
        with open(config_file) as cf:
            config = cf.read()

    return json.loads(config, object_pairs_hook=OrderedDict)


def read_credentials(credential_file=DEFAULT_CREDENTIAL_FILE):
    with open(credential_file) as cred:
        credentials = json.load(cred)
        return credentials

from .hatrac_store import HatracStore
from .ermrest_catalog import ErmrestCatalog
from .polling_ermrest_catalog import PollingErmrestCatalog
