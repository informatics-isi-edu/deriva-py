import os
import sys
import json
import traceback
import requests
import argparse
import logging
import datetime
from collections.abc import Mapping, Iterable
from requests.exceptions import HTTPError, ConnectionError, Timeout
from deriva.core.deriva_binding import DerivaClientContext
from deriva.core.utils.mime_utils import parse_content_disposition
from deriva.core import BaseCLI, KeyValuePairArgs, get_new_requests_session, get_transfer_summary, get_credential, \
                         format_credential, format_exception, urlsplit, DEFAULT_SESSION_CONFIG, DEFAULT_CHUNK_SIZE
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError, \
    DerivaDownloadAuthenticationError, DerivaDownloadAuthorizationError, DerivaDownloadTimeoutError

logger = logging.getLogger(__name__)

EXPORT_SERVICE_PATH = "/deriva/export/%s"

"""
Client tool for interacting with DERIVA Export service.

:param host (str): The host server for the export operation.
:param config_file (str): Path to an export configuration file.
:param credential (dict): Authentication credential (returned from get_credential()) for the export process. Optional.
:param envars (dict): A dictionary of variables used for template substitution. Optional.
:param output_dir (str): The directory where exported data will be stored (default: "."). Optional.
:param defer_download (bool): Whether to defer the actual data download. Optional.
:param timeout (tuple) OR (float): Timeout value as a tuple of floats in seconds for (connect,read) export operations. 
        If a single float value is passed, it will apply to both connect and read operations. Optional.
:param export_type (str): The type of export to perform (default: "bdbag"). Optional.

:return: The full path to the downloaded file. If "defer_download" is True, the URL(s) where the export can be downloaded.
"""
class DerivaExport:
    def __init__(self, **kwargs):
        self.host = kwargs.get("host")
        self.config_file = kwargs.get("config_file")
        self.envars = kwargs.get("envars", dict())
        self.credential = kwargs.get("credential")
        self.output_dir = kwargs.get("output_dir", ".")
        self.defer_download = kwargs.get("defer_download")
        self.timeout = kwargs.get("timeout")
        self.export_type = kwargs.get("export_type", "bdbag")
        self.base_server_uri = "https://" + self.host
        self.service_url = self.base_server_uri + EXPORT_SERVICE_PATH % self.export_type
        self.session_config = DEFAULT_SESSION_CONFIG.copy()
        if isinstance(self.timeout, tuple):
            if len(self.timeout) == 2:
                self.session_config["timeout"] = self.timeout
            else:
                self.session_config["timeout"] = float(self.timeout[0])
        elif self.timeout is not None:
            try:
                self.session_config["timeout"] = float(self.timeout)
            except ValueError:
                logger.warning("Unparseable timeout value: %r. Defaults will be used: %r." %
                               (self.timeout, self.session_config["timeout"]))
        self.session = get_new_requests_session(self.service_url, self.session_config)
        self.dcctx = DerivaClientContext()
        self.dcctx['cid'] = kwargs.get("dcctx_cid", "api/" + self.__class__.__name__)
        self.session.headers.update({'deriva-client-context': self.dcctx.encoded()})

        # credential initialization
        if self.credential is None:
            token = kwargs.get("token")
            oauth2_token = kwargs.get("oauth2_token")
            credential_file = kwargs.get("credential_file")
            if token or oauth2_token:
                self.credential = format_credential(token=token, oauth2_token=oauth2_token)
            else:
                self.credential = get_credential(self.host, credential_file)

        if self.credential is None:
            raise DerivaDownloadAuthenticationError(
                "The requested service requires authentication and a valid login credential could "
                "not be found (or was not provided) for the specified host.")
        if 'bearer-token' in self.credential:
            self.session.headers.update(
                {'Authorization': 'Bearer {token}'.format(token=self.credential['bearer-token'])})
        elif 'cookie' in self.credential:
            cname, cval = self.credential['cookie'].split('=', 1)
            self.session.cookies.set(cname, cval, domain=self.host, path='/')

    def validate_authn_session(self):
        url = self.base_server_uri + "/authn/session"
        r = self.session.get(url)
        if r.status_code == requests.codes.not_found or r.status_code == requests.codes.unauthorized:
            logger.warning("Unable to authenticate. Check for missing or expired credentials.")
        r.raise_for_status()
        return r.json()

    def recursive_format(self, d, **kwargs):
        """
        Recursively apply str.format to all string-based values in a dictionary.
        Supports nested dictionaries and lists.

        :param d: Dictionary or iterable containing values to be formatted
        :param kwargs: Formatting arguments
        :return: New dictionary or iterable with formatted strings
        """
        if isinstance(d, Mapping):
            return {k: self.recursive_format(v, **kwargs) for k, v in d.items()}
        elif isinstance(d, str):
            return d.format(**kwargs)
        elif isinstance(d, Iterable) and not isinstance(d, (str, bytes)):
            return type(d)(self.recursive_format(v, **kwargs) for v in d)
        else:
            return d


    def retrieve_file(self, url):
        content_disposition = None
        try:
            head = self.session.head(url)
            if head.ok:
                content_disposition = head.headers.get("Content-Disposition") if head.ok else None
            if not content_disposition:
                raise DerivaDownloadError("HEAD response missing Content-Disposition header.")
        except requests.HTTPError as e:
            raise DerivaDownloadError("HEAD request for [%s] failed: %s" % (url, e))

        filename = parse_content_disposition(content_disposition)
        output_path = os.path.abspath(os.path.join(self.output_dir, filename))
        with self.session.get(url, stream=True) as r:
            if r.status_code != 200:
                file_error = "File [%s] transfer failed." % output_path
                url_error = 'HTTP GET Failed for url: %s' % url
                host_error = "Host %s responded:\n\n%s" % (urlsplit(url).netloc, r.text)
                raise DerivaDownloadError('%s\n\n%s\n%s' % (file_error, url_error, host_error))
            else:
                total = 0
                start = datetime.datetime.now()
                logging.debug("Transferring file %s to %s" % (url, output_path))
                with open(output_path, 'wb') as data_file:
                    for chunk in r.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                        data_file.write(chunk)
                        total += len(chunk)
                elapsed = datetime.datetime.now() - start
                summary = get_transfer_summary(total, elapsed)
                logging.info("File [%s] transfer successful. %s" % (output_path, summary))
                return output_path

    def export(self):
        try:
            auth = self.validate_authn_session()
            logger.debug("Authenticated session established. Session attributes: %s" % auth)

            try:
                logger.info("Processing export config file: %s" % self.config_file)
                with open(self.config_file, encoding='utf-8') as cf:
                    config = json.loads(cf.read())
                    env = config.get("env", {})
                    env = self.recursive_format(env, **self.envars)
                    config.update({"env": env})
            except Exception as e:
                raise DerivaDownloadConfigurationError("Error processing export config file: %s" % format_exception(e))

            logger.info("Requesting %s export at: %s" % (self.export_type, self.service_url))
            response = self.session.post(self.service_url, json=config)
            response.raise_for_status()
            result_urls = response.text.split('\n')
            logger.info("Export successful. Service responded with URL list: %s" % result_urls)
            if not self.defer_download:
                if self.export_type == "bdbag":
                    result_url = result_urls[1] if len(result_urls) > 1 else result_urls[0]
                    logger.info("Downloading exported bag content from %s to directory: %s" %
                                (result_url, os.path.abspath(self.output_dir)))
                    return self.retrieve_file(result_url)
                elif self.export_type == "file":
                    for result_url in result_urls:
                        self.retrieve_file(result_url)
                        logger.info("Downloading exported file content from %s to directory: %s" %
                                    (result_url, os.path.abspath(self.output_dir)))
                else:
                    pass
            else:
                return result_urls
        except ConnectionError as e:
            raise DerivaDownloadError("Connection error occurred. %s" % format_exception(e))
        except Timeout as e:
            raise DerivaDownloadTimeoutError("Connection timeout occurred. %s" % format_exception(e))
        except HTTPError as e:
            if e.response.status_code == requests.codes.unauthorized:
                raise DerivaDownloadAuthenticationError(
                    "The requested service requires authentication and a valid login session could "
                    "not be found for the specified host. Server responded: %s" % format_exception(e))
            elif e.response.status_code == requests.codes.forbidden:
                raise DerivaDownloadAuthorizationError(
                    "A requested operation was forbidden. Server responded: %s" % format_exception(e))
            else:
                raise DerivaDownloadError(format_exception(e))


class DerivaExportCLI(BaseCLI):
    def __init__(self, description, epilog, **kwargs):

        BaseCLI.__init__(self, description, epilog, **kwargs)
        self.parser.add_argument("--defer-download", action="store_true",
                                 help="Do not download exported file(s). Default: False")
        self.parser.add_argument("--timeout", metavar="<connect,read>", type=BaseCLI.parse_tuple,
                                 help="Timeout value(s) in seconds (int or float) for connect and read operations. "
                                      "Separate using commas. If a single value is provided it will be used for both "
                                      "connect and read timeouts.")
        self.parser.add_argument("--export-type", choices=["bdbag", "file"], default="bdbag",
                                 help="Export type: {bdbag|file}. Default is bdbag.",)
        self.parser.add_argument("--output-dir", metavar="<output dir>", default=".",
                                 help="Path to an output directory. Default is current directory.")
        self.parser.add_argument("envars", metavar="[key=value key=value ...]",
                                 nargs=argparse.REMAINDER, action=KeyValuePairArgs, default={},
                                 help="Variable length of whitespace-delimited key=value pair arguments used for "
                                      "string interpolation in specific parts of the configuration file. "
                                      "For example: key1=value1 key2=value2")

    def main(self):
        try:
            args = self.parse_cli()
        except ValueError as e:
            sys.stderr.write(str(e))
            return 2
        if not args.quiet:
            sys.stderr.write("\n")

        try:
            exporter = DerivaExport(**vars(args), dcctx_cid="cli/" + self.__class__.__name__)
            exporter.export()
        except (DerivaDownloadError, DerivaDownloadConfigurationError, DerivaDownloadAuthenticationError,
                DerivaDownloadAuthorizationError, DerivaDownloadTimeoutError) as e:
            sys.stderr.write(("\n" if not args.quiet else "") + format_exception(e))
            if args.debug:
                traceback.print_exc()
            return 1
        except:
            sys.stderr.write("An unexpected error occurred.")
            traceback.print_exc()
            return 1
        finally:
            if not args.quiet:
                sys.stderr.write("\n\n")
        return 0

DESC = "Deriva Export Service Download Utility - CLI"
INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-py"

def main():
    cli = DerivaExportCLI(DESC, INFO, hostname_required=True, config_file_required=True)
    return cli.main()


if __name__ == '__main__':
    sys.exit(main())