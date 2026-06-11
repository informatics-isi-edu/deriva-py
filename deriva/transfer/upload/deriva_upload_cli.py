import os
import sys
import json
import traceback
from deriva.transfer import DerivaUpload, DerivaUploadError, DerivaUploadConfigurationError, \
    DerivaUploadCatalogCreateError, DerivaUploadCatalogUpdateError, DerivaUploadAuthenticationError
from deriva.core import BaseCLI, write_config, format_credential, format_exception, urlparse, \
    DEFAULT_REQUESTS_TIMEOUT


class DerivaUploadCLI(BaseCLI):
    def __init__(self, uploader, description, epilog):
        if not issubclass(uploader, DerivaUpload):
            raise TypeError("DerivaUpload subclass required")

        BaseCLI.__init__(self, description, epilog, uploader.getVersion(), hostname_required=True)
        self.parser.add_argument('--no-config-update', action="store_true",
                                 help="Do not check for (and download) an updated configuration from the server.")
        self.parser.add_argument('--purge-state', action="store_true",
                                 help="Purge (delete) any existing transfer state files found in the directory "
                                      "hierarchy of the input path.")
        self.parser.add_argument('--dry-run', action="store_true",
                                 help="Scan the input directory for matching files to upload, "
                                      "but do not transfer any files.")
        self.parser.add_argument('--output-file', metavar='<file>',
                                 help="Optional path where a JSON-formatted output file will be written, "
                                      "containing file upload status and associated metadata.")
        self.parser.add_argument("--catalog", default=1, metavar="<1>", help="Catalog number. Default: 1")
        self.parser.add_argument("--connect-timeout", type=float, metavar="<seconds>",
                                 help="Override the connection timeout, in seconds (default: %d). For uploads this "
                                      "also bounds the time allowed to write each request body / chunk, so increase "
                                      "it for slow or high-latency networks." % DEFAULT_REQUESTS_TIMEOUT[0])
        self.parser.add_argument("--read-timeout", type=float, metavar="<seconds>",
                                 help="Override the read (response) timeout, in seconds (default: %d)."
                                      % DEFAULT_REQUESTS_TIMEOUT[1])
        self.parser.add_argument("--chunk-size", type=int, metavar="<bytes>",
                                 help="Override the chunked-upload chunk size, in bytes. Smaller chunks reduce the "
                                      "amount of data resent on retry over unreliable networks.")
        self.parser.add_argument("path", metavar="<input dir>", help="Path to an input directory.")
        self.uploader = uploader

    @staticmethod
    def upload(uploader,
               data_path,
               hostname,
               catalog=1,
               token=None,
               config_file=None,
               credential_file=None,
               no_update=False,
               purge=False,
               dry_run=False,
               output_file=None,
               connect_timeout=None,
               read_timeout=None,
               chunk_size=None):

        if not issubclass(uploader, DerivaUpload):
            raise TypeError("DerivaUpload subclass required")

        assert hostname
        server = dict()
        server["catalog_id"] = catalog
        if hostname.startswith("http"):
            url = urlparse(hostname)
            server["protocol"] = url.scheme
            server["host"] = url.netloc
        else:
            server["protocol"] = "https"
            server["host"] = hostname

        # Build session config overrides from any supplied timeout values, falling back to the library defaults for the
        # component(s) not specified. These are applied per-invocation and do not modify the server/deployed config.
        session_config_overrides = None
        if connect_timeout is not None or read_timeout is not None:
            default_connect, default_read = DEFAULT_REQUESTS_TIMEOUT
            session_config_overrides = {
                "timeout": [connect_timeout if connect_timeout is not None else default_connect,
                            read_timeout if read_timeout is not None else default_read]
            }

        deriva_uploader = uploader(config_file, credential_file, server, dcctx_cid="cli/" + DerivaUploadCLI.__name__,
                                   session_config_overrides=session_config_overrides, chunk_size=chunk_size)
        if token:
            deriva_uploader.setCredentials(format_credential(token))
        if not config_file and not no_update:
            config = deriva_uploader.getUpdatedConfig()
            if config:
                write_config(deriva_uploader.getDeployedConfigFilePath(), config)
        if not deriva_uploader.isVersionCompatible():
            raise RuntimeError("Version incompatibility detected", "Current version: [%s], required version(s): %s." % (
                deriva_uploader.getVersion(), deriva_uploader.getVersionCompatibility()))
        deriva_uploader.scanDirectory(data_path, abort_on_invalid_input=False, purge_state=purge)
        if not dry_run:
            results = deriva_uploader.uploadFiles()
            if output_file:
                with open(output_file, "w") as output:
                    json.dump(results, output)
        deriva_uploader.cleanup()

    def main(self):
        args = self.parse_cli()
        if args.path is None:
            sys.stderr.write("\nError: Input directory not specified.\n")
            self.parser.print_usage()
            return 2
        if not args.quiet:
            sys.stderr.write("\n")

        try:
            DerivaUploadCLI.upload(self.uploader,
                                   os.path.abspath(args.path),
                                   args.host,
                                   args.catalog,
                                   args.token,
                                   args.config_file,
                                   args.credential_file,
                                   args.no_config_update,
                                   args.purge_state,
                                   args.dry_run,
                                   args.output_file,
                                   args.connect_timeout,
                                   args.read_timeout,
                                   args.chunk_size)
        except (RuntimeError, FileNotFoundError, DerivaUploadError, DerivaUploadConfigurationError,
                DerivaUploadCatalogCreateError, DerivaUploadCatalogUpdateError, DerivaUploadAuthenticationError) as e:
            sys.stderr.write(("\n" if not args.quiet else "") + format_exception(e))
            if args.debug:
                traceback.print_exc()
            return 1
        except:
            traceback.print_exc()
            return 1
        finally:
            if not args.quiet:
                sys.stderr.write("\n\n")
        return 0
