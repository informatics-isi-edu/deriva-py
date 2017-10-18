import io
import os
import sys
import shutil
import errno
import json
import platform
import logging
import requests
from collections import OrderedDict
from distutils.util import strtobool
from portalocker import Lock, LOCK_EX, LOCK_SH

__version__ = "0.3.0"

if sys.version_info > (3,):
    from urllib.parse import quote as urlquote
else:
    from urllib import quote as urlquote

DEFAULT_HEADERS = {}

DEFAULT_CHUNK_SIZE = 2048 ** 2


class NotModified (ValueError):
    pass


class ConcurrentUpdate (ValueError):
    pass


def stob(string):
    return bool(strtobool(str(string)))


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
    "retry_status_forcelist": [500, 502, 503, 504]
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

DEFAULT_CREDENTIAL = {}


def copy_config(src, dst):
    config_dir = os.path.dirname(dst)
    if not os.path.isdir(config_dir):
        try:
            os.makedirs(config_dir)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
    shutil.copy2(src, dst)


def write_config(config_file=DEFAULT_CONFIG_FILE, config=DEFAULT_CONFIG):
    config_dir = os.path.dirname(config_file)
    if not os.path.isdir(config_dir):
        try:
            os.makedirs(config_dir)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
    with io.open(config_file, 'w', newline='\n') as cf:
        cf.write(json.dumps(config, indent=2))
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
            config = default

    if not config:
        with open(config_file) as cf:
            config = cf.read()

    return json.loads(config, object_pairs_hook=OrderedDict)


def write_credential(credential_file=DEFAULT_CREDENTIAL_FILE, credential=DEFAULT_CREDENTIAL):
    credential_dir = os.path.dirname(credential_file)
    if not os.path.isdir(credential_dir):
        try:
            os.makedirs(credential_dir)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
    with Lock(credential_file, 'w', timeout=60, flags=LOCK_EX) as cf:
        os.chmod(credential_file, 0o600)
        cf.write(json.dumps(credential, indent=2))
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
            logging.warning("Unable to create configuration file %s. Using internal defaults. %s" %
                            (credential_file, format_exception(e)))
            credential = default

    if not credential:
        with Lock(credential_file, 'r', timeout=60, flags=LOCK_EX) as cf:
            credential = cf.read()

    return json.loads(credential, object_pairs_hook=OrderedDict)


def get_credential(host, credential_file=DEFAULT_CREDENTIAL_FILE):
    credentials = read_credential(credential_file)
    return credentials.get(host)


def resource_path(relative_path, default=os.path.abspath(".")):
    """ required to find bundled data at runtime in Pyinstaller single-file exe mode """
    if getattr(sys, 'frozen', False):
        return os.path.join(getattr(sys, '_MEIPASS'), relative_path)
    if default is None:
        return relative_path
    return os.path.join(default, relative_path)

from deriva.core import datapath
from deriva.core.base_cli import BaseCLI
from deriva.core.deriva_binding import DerivaBinding
from deriva.core.ermrest_catalog import ErmrestCatalog
from deriva.core.ermrest_config import AttrDict, CatalogConfig
from deriva.core.polling_ermrest_catalog import PollingErmrestCatalog
from deriva.core.hatrac_store import HatracStore, HatracHashMismatch, HatracJobPaused, HatracJobAborted