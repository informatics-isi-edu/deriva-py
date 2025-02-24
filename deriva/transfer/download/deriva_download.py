import os
import time
import datetime
import uuid
import logging
import platform
import requests
from requests.exceptions import HTTPError
from bdbag import bdbag_api as bdb, bdbag_ro as ro, BAG_PROFILE_TAG, BDBAG_RO_PROFILE_ID
from bdbag.bdbagit import BagValidationError
from deriva.core import DerivaServer, ErmrestCatalog, HatracStore, format_exception, get_credential, \
                         format_credential, read_config, stob, Megabyte, __version__ as VERSION
from deriva.core.utils.version_utils import get_installed_version
from deriva.transfer.download.processors import find_query_processor, find_transform_processor, find_post_processor
from deriva.transfer.download.processors.base_processor import LOCAL_PATH_KEY, REMOTE_PATHS_KEY, SERVICE_URL_KEY, \
    FILE_SIZE_KEY
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError, \
    DerivaDownloadAuthenticationError, DerivaDownloadAuthorizationError, DerivaDownloadTimeoutError, \
    DerivaDownloadBaggingError


logger = logging.getLogger(__name__)


class DerivaDownload(object):
    """

    """
    def __init__(self, server, **kwargs):
        self.server = server
        self.hostname = None
        self.catalog = None
        self.store = None
        self.cancelled = False
        self.output_dir = os.path.abspath(kwargs.get("output_dir", "."))
        self.envars = kwargs.get("envars", dict())
        self.config = kwargs.get("config")
        self.credentials = kwargs.get("credentials", dict())
        config_file = kwargs.get("config_file")
        credential_file = kwargs.get("credential_file")
        self.metadata = dict()
        self.sessions = dict()
        self.allow_anonymous = kwargs.get("allow_anonymous", True)
        self.max_payload_size_mb = int(kwargs.get("max_payload_size_mb", 0) or 0)
        self.timeout_secs = int(kwargs.get("timeout", 0) or 0)
        self.timeout = None

        info = "%s v%s [Python %s, %s]" % (
            self.__class__.__name__, get_installed_version(VERSION),
            platform.python_version(), platform.platform(aliased=True))
        logger.info("Initializing downloader: %s" % info)

        if not self.server:
            raise DerivaDownloadConfigurationError("Server not specified!")

        # server variable initialization
        self.hostname = self.server.get('host', '')
        if not self.hostname:
            raise DerivaDownloadConfigurationError("Host not specified!")
        protocol = self.server.get('protocol', 'https')
        self.server_url = protocol + "://" + self.hostname
        catalog_id = self.server.get("catalog_id", "1")
        session_config = self.server.get('session')

        # credential initialization
        token = kwargs.get("token")
        oauth2_token = kwargs.get("oauth2_token")
        username = kwargs.get("username")
        password = kwargs.get("password")
        if credential_file:
            self.credentials = get_credential(self.hostname, credential_file)
        elif token or oauth2_token or (username and password):
            self.credentials = format_credential(token=token,
                                                 oauth2_token=oauth2_token,
                                                 username=username,
                                                 password=password)

        # catalog and file store initialization
        server = DerivaServer(protocol, self.hostname, credentials=self.credentials, session_config=session_config)
        self.catalog = server.connect_ermrest(catalog_id)
        self.store = HatracStore(protocol, self.hostname, self.credentials, session_config=session_config)

        # init dcctx cid
        self.set_dcctx_cid(kwargs.get("dcctx_cid", "api/" + self.__class__.__name__))

        # process config file
        if config_file:
            try:
                self.config = read_config(config_file)
            except Exception as e:
                raise DerivaDownloadConfigurationError(e)

    def set_dcctx_cid(self, cid):
        assert cid, "A dcctx cid is required"
        if self.catalog:
            self.catalog.dcctx['cid'] = cid
        if self.store:
            self.store.dcctx['cid'] = cid

    def set_config(self, config):
        self.config = config

    def set_credentials(self, credentials):
        self.catalog.set_credentials(credentials, self.hostname)
        self.store.set_credentials(credentials, self.hostname)
        self.credentials = credentials

    def check_payload_size(self, outputs):
        if self.max_payload_size_mb < 1:
            return

        # This is not very well optimized for the way it is invoked but until it becomes an issue, it is good enough.
        max_bytes = self.max_payload_size_mb * Megabyte
        total_bytes = 0
        for v in outputs.values():
            total_bytes += v.get(FILE_SIZE_KEY, 0)
            if total_bytes >= max_bytes:
                raise DerivaDownloadError("Maximum payload size of %d megabytes exceeded." %
                                          self.max_payload_size_mb)

    def download(self, **kwargs):

        if not self.config:
            raise DerivaDownloadConfigurationError("No configuration specified!")

        if self.config.get("catalog") is None:
            raise DerivaDownloadConfigurationError("Catalog configuration error!")

        if self.timeout_secs > 0:
            self.timeout = datetime.datetime.now() + datetime.timedelta(0, self.timeout_secs)

        ro_manifest = None
        ro_author_name = None
        ro_author_orcid = None
        remote_file_manifest = os.path.abspath(
            ''.join([os.path.join(self.output_dir, 'remote-file-manifest_'), str(uuid.uuid4()), ".json"]))

        catalog_config = self.config['catalog']
        self.envars.update(self.config.get('env', dict()))
        self.envars.update({"hostname": self.hostname})

        # 1. If we don't have a client identity, we need to authenticate
        identity = kwargs.get("identity")
        if not identity:
            try:
                if not self.credentials:
                    self.set_credentials(get_credential(self.hostname))
                logger.info("Validating credentials for host: %s" % self.hostname)
                attributes = self.catalog.get_authn_session().json()
                identity = attributes["client"]
            except HTTPError as he:
                if he.response.status_code == 404:
                    logger.info("No existing login session found for host: %s" % self.hostname)
            except Exception as e:
                raise DerivaDownloadAuthenticationError("Unable to validate credentials: %s" % format_exception(e))
        wallet = kwargs.get("wallet", {})

        # 2. Check for bagging config and initialize bag related variables
        bag_path = None
        bag_archiver = None
        bag_algorithms = None
        bag_idempotent = False
        bag_strict = True
        bag_config = self.config.get('bag')
        create_bag = True if bag_config else False
        if create_bag:
            bag_name = bag_config.get(
                'bag_name', ''.join(["deriva_bag", '_', time.strftime("%Y-%m-%d_%H.%M.%S")])).format(**self.envars)
            bag_path = os.path.abspath(os.path.join(self.output_dir, bag_name))
            bag_archiver = bag_config.get('bag_archiver')
            bag_algorithms = bag_config.get('bag_algorithms', ['sha256'])
            bag_idempotent = stob(bag_config.get('bag_idempotent', False))
            bag_metadata = bag_config.get('bag_metadata', {"Internal-Sender-Identifier":
                                                           "deriva@%s" % self.server_url})
            bag_ro = create_bag and not bag_idempotent and stob(bag_config.get('bag_ro', True))
            bag_strict = stob(bag_config.get('bag_strict', True))
            if create_bag:
                bdb.ensure_bag_path_exists(bag_path)
                bag = bdb.make_bag(bag_path, algs=bag_algorithms, metadata=bag_metadata, idempotent=bag_idempotent)
                if bag_ro:
                    ro_author_name = bag.info.get("Contact-Name",
                                                  None if not identity else
                                                  identity.get('full_name',
                                                               identity.get('display_name',
                                                                            identity.get('id', None))))
                    ro_author_orcid = bag.info.get("Contact-Orcid")
                    ro_manifest = ro.init_ro_manifest(author_name=ro_author_name, author_orcid=ro_author_orcid)
                    bag_metadata.update({BAG_PROFILE_TAG: BDBAG_RO_PROFILE_ID})

        # 3. Process the set of queries by locating, instantiating, and invoking the specified processor(s)
        outputs = dict()
        base_path = bag_path if bag_path else self.output_dir
        for processor in catalog_config['query_processors']:
            processor_name = processor["processor"]
            processor_type = processor.get('processor_type')
            processor_params = processor.get('processor_params')

            try:
                query_processor = find_query_processor(processor_name, processor_type)
                processor = query_processor(self.envars,
                                            inputs=outputs,
                                            bag=create_bag,
                                            catalog=self.catalog,
                                            store=self.store,
                                            base_path=base_path,
                                            processor_params=processor_params,
                                            remote_file_manifest=remote_file_manifest,
                                            ro_manifest=ro_manifest,
                                            ro_author_name=ro_author_name,
                                            ro_author_orcid=ro_author_orcid,
                                            identity=identity,
                                            wallet=wallet,
                                            allow_anonymous=self.allow_anonymous,
                                            timeout=self.timeout)
                outputs = processor.process()
                assert outputs is not None
                if processor.should_abort():
                    raise DerivaDownloadTimeoutError("Timeout (%s seconds) waiting for processor [%s] to complete." %
                                                     (self.timeout_secs, processor_name))
                self.check_payload_size(outputs)
            except Exception as e:
                logger.error(format_exception(e))
                if create_bag:
                    bdb.cleanup_bag(bag_path)
                    if remote_file_manifest and os.path.isfile(remote_file_manifest):
                        os.remove(remote_file_manifest)
                raise

        # 4. Execute anything in the transform processing pipeline, if configured
        transform_processors = self.config.get('transform_processors', [])
        if transform_processors:
            for processor in transform_processors:
                processor_name = processor["processor"]
                processor_type = processor.get('processor_type')
                processor_params = processor.get('processor_params')
                try:
                    transform_processor = find_transform_processor(processor_name, processor_type)
                    processor = transform_processor(
                        self.envars,
                        inputs=outputs,
                        processor_params=processor_params,
                        base_path=base_path,
                        bag=create_bag,
                        ro_manifest=ro_manifest,
                        ro_author_name=ro_author_name,
                        ro_author_orcid=ro_author_orcid,
                        identity=identity,
                        wallet=wallet,
                        allow_anonymous=self.allow_anonymous,
                        timeout=self.timeout)
                    outputs = processor.process()
                    if processor.should_abort():
                        raise DerivaDownloadTimeoutError(
                            "Timeout (%s seconds) waiting for processor [%s] to complete." %
                            (self.timeout_secs, processor_name))
                    self.check_payload_size(outputs)
                except Exception as e:
                    if create_bag:
                        bdb.cleanup_bag(bag_path)
                        if remote_file_manifest and os.path.isfile(remote_file_manifest):
                            os.remove(remote_file_manifest)
                    raise

        # 5. Create the bag, and archive (serialize) if necessary
        if create_bag:
            try:
                if ro_manifest:
                    ro.write_bag_ro_metadata(ro_manifest, bag_path)
                if not os.path.isfile(remote_file_manifest):
                    remote_file_manifest = None
                bdb.make_bag(bag_path,
                             algs=bag_algorithms,
                             remote_file_manifest=remote_file_manifest
                             if (remote_file_manifest and os.path.getsize(remote_file_manifest) > 0) else None,
                             update=True,
                             idempotent=bag_idempotent,
                             strict=bag_strict)
            except BagValidationError as bve:
                msg = "Unable to validate bag.%s Error: %s" % (
                    "" if not bag_strict else
                    " Strict checking has been enabled, which most likely means that this bag "
                    "is empty (has no payload files or fetch references) and therefore invalid.",
                    format_exception(bve))
                logger.error(msg)
                bdb.cleanup_bag(bag_path)
                raise DerivaDownloadBaggingError(msg)
            except Exception as e:
                msg = "Unhandled exception while updating bag manifests: %s" % format_exception(e)
                logger.error(msg)
                bdb.cleanup_bag(bag_path)
                raise DerivaDownloadBaggingError(msg)
            finally:
                if remote_file_manifest and os.path.isfile(remote_file_manifest):
                    os.remove(remote_file_manifest)

            logger.info('Created bag: %s' % bag_path)

            if bag_archiver is not None:
                try:
                    archive = bdb.archive_bag(bag_path,
                                              bag_archiver.lower(),
                                              idempotent=bag_idempotent)
                    bdb.cleanup_bag(bag_path)
                    outputs = {os.path.basename(archive): {LOCAL_PATH_KEY: archive}}
                except Exception as e:
                    msg = "Exception while creating data bag archive: %s" % format_exception(e)
                    logger.error(msg)
                    raise DerivaDownloadBaggingError(msg)
            else:
                outputs = {os.path.basename(bag_path): {LOCAL_PATH_KEY: bag_path}}

        # 6. Execute anything in the post processing pipeline, if configured
        post_processors = self.config.get('post_processors', [])
        if post_processors:
            for processor in post_processors:
                processor_name = processor["processor"]
                processor_type = processor.get('processor_type')
                processor_params = processor.get('processor_params')
                try:
                    post_processor = find_post_processor(processor_name, processor_type)
                    processor = post_processor(
                        self.envars,
                        inputs=outputs,
                        processor_params=processor_params,
                        identity=identity,
                        wallet=wallet,
                        allow_anonymous=self.allow_anonymous,
                        timeout=self.timeout)
                    outputs = processor.process()
                    if processor.should_abort():
                        raise DerivaDownloadTimeoutError(
                            "Timeout (%s seconds) waiting for processor [%s] to complete." %
                            (self.timeout_secs, processor_name))
                    self.check_payload_size(outputs)
                except Exception as e:
                    logger.error(format_exception(e))
                    raise

        return outputs

    def __del__(self):
        for session in self.sessions.values():
            session.close()


class GenericDownloader(DerivaDownload):
    LOCAL_PATH_KEY = LOCAL_PATH_KEY
    REMOTE_PATHS_KEY = REMOTE_PATHS_KEY
    SERVICE_URL_KEY = SERVICE_URL_KEY

    def __init__(self, *args, **kwargs):
        DerivaDownload.__init__(self, *args, **kwargs)
