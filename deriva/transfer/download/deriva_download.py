import os
import time
import uuid
import logging
import platform
import requests
from requests.exceptions import HTTPError
from bdbag import bdbag_api as bdb, bdbag_ro as ro, BAG_PROFILE_TAG, BDBAG_RO_PROFILE_ID
from deriva.core import ErmrestCatalog, HatracStore, format_exception, get_credential, read_config, stob, \
    __version__ as VERSION
from deriva.transfer.download.processors import find_query_processor, find_pre_processor, find_post_processor
from deriva.transfer.download.processors.base_processor import LOCAL_PATH_KEY
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError, \
    DerivaDownloadAuthenticationError


class DerivaDownload(object):
    """

    """
    def __init__(self, server,
                 output_dir=None, kwargs=None, config=None, config_file=None, credentials=None, credential_file=None):
        self.server = server
        self.hostname = None
        self.output_dir = output_dir if output_dir else "."
        self.envars = kwargs if kwargs else dict()
        self.catalog = None
        self.store = None
        self.config = config
        self.cancelled = False
        self.credentials = credentials if credentials else dict()
        self.metadata = dict()
        self.sessions = dict()

        info = "%s v%s [Python %s, %s]" % (
            self.__class__.__name__, VERSION, platform.python_version(), platform.platform(aliased=True))
        logging.info("Initializing downloader: %s" % info)

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
        if credential_file:
            self.credentials = get_credential(self.hostname, credential_file)

        # catalog and file store initialization
        if self.catalog:
            del self.catalog
        self.catalog = ErmrestCatalog(
            protocol, self.hostname, catalog_id, self.credentials, session_config=session_config)
        if self.store:
            del self.store
        self.store = HatracStore(
            protocol, self.hostname, self.credentials, session_config=session_config)

        # process config file
        if config_file and os.path.isfile(config_file):
            self.config = read_config(config_file)

    def setConfig(self, config):
        self.config = config

    def setCredentials(self, credentials):
        self.catalog.set_credentials(credentials, self.hostname)
        self.store.set_credentials(credentials, self.hostname)
        self.credentials = credentials

    def download(self, **kwargs):

        if not self.config:
            raise DerivaDownloadConfigurationError("No configuration specified!")

        if self.config.get("catalog") is None:
            raise DerivaDownloadConfigurationError("Catalog configuration error!")

        ro_manifest = None
        ro_author_name = None
        ro_author_orcid = None
        remote_file_manifest = os.path.abspath(
            ''.join([os.path.join(self.output_dir, 'remote-file-manifest_'), str(uuid.uuid4()), ".json"]))

        catalog_config = self.config['catalog']
        self.envars.update(self.config.get('env', dict()))

        # 1. If we don't have a client identity, we need to authenticate
        identity = kwargs.get("identity")
        if not identity:
            try:
                if not self.credentials:
                    self.setCredentials(get_credential(self.hostname))
                logging.info("Validating credentials for host: %s" % self.hostname)
                attributes = self.catalog.get_authn_session().json()
                identity = attributes["client"]
            except Exception as e:
                raise DerivaDownloadAuthenticationError("Unable to validate credentials: %s" % format_exception(e))

        # 2. Check for bagging config and initialize bag related variables
        bag_path = None
        bag_archiver = None
        bag_algorithms = None
        bag_config = self.config.get('bag')
        create_bag = True if bag_config else False
        if create_bag:
            bag_name = bag_config.get('bag_name', ''.join(["deriva_bag", '_', time.strftime("%Y-%m-%d_%H.%M.%S")]))
            bag_path = os.path.abspath(os.path.join(self.output_dir, bag_name))
            bag_archiver = bag_config.get('bag_archiver')
            bag_algorithms = bag_config.get('bag_algorithms', ['sha256'])
            bag_metadata = bag_config.get('bag_metadata', {"Internal-Sender-Identifier":
                                                           "deriva@%s" % self.server_url})
            bag_ro = create_bag and stob(bag_config.get('bag_ro', "True"))
            if create_bag:
                bdb.ensure_bag_path_exists(bag_path)
                bag = bdb.make_bag(bag_path, algs=bag_algorithms, metadata=bag_metadata)
                if bag_ro:
                    ro_author_name = bag.info.get("Contact-Name",
                                                  identity.get('full_name',
                                                               identity.get('display_name',
                                                                            identity.get('id', None))))
                    ro_author_orcid = bag.info.get("Contact-Orcid")
                    ro_manifest = ro.init_ro_manifest(author_name=ro_author_name, author_orcid=ro_author_orcid)
                    bag_metadata.update({BAG_PROFILE_TAG: BDBAG_RO_PROFILE_ID})

        # 3. Process the set of queries by locating, instantiating, and invoking the specified download processor
        outputs = dict()
        base_path = bag_path if bag_path else self.output_dir
        for query in catalog_config['queries']:
            query_path = query['query_path']
            output_format = query['output_format']
            output_processor = query.get("output_format_processor")
            format_args = query.get('output_format_params', None)
            output_path = query.get('output_path', '')

            try:
                query_processor = find_query_processor(output_format, output_processor)
                processor = query_processor(self.envars,
                                            bag=create_bag,
                                            catalog=self.catalog,
                                            store=self.store,
                                            query=query_path,
                                            base_path=base_path,
                                            sub_path=output_path,
                                            format_args=format_args,
                                            remote_file_manifest=remote_file_manifest,
                                            ro_manifest=ro_manifest,
                                            ro_author_name=ro_author_name,
                                            ro_author_orcid=ro_author_orcid)
                outputs.update(processor.process())
            except Exception as e:
                logging.error(format_exception(e))
                if create_bag:
                    bdb.cleanup_bag(bag_path)
                raise

        # 4. Execute anything in the pre-processing pipeline, if configured
        preprocessors = self.config.get('preprocessors', [])
        if preprocessors:
            for processor in preprocessors:
                processor_name = processor["processor"]
                processor_type = processor.get('processor_type', None)
                processor_parameters = processor.get('processor_parameters', None)

                try:
                    preprocessor = find_pre_processor(processor_name, processor_type)
                    processor = preprocessor(
                        self.envars, input_files=outputs, processor_parameters=processor_parameters)
                    outputs = processor.process()
                except Exception as e:
                    logging.error(format_exception(e))
                    raise

        # 5. Create the bag, and archive (serialize) if necessary
        if create_bag:
            try:
                if ro_manifest:
                    ro.write_bag_ro_metadata(ro_manifest, bag_path)
                if not os.path.isfile(remote_file_manifest):
                    remote_file_manifest = None
                bdb.make_bag(bag_path, algs=bag_algorithms, remote_file_manifest=remote_file_manifest, update=True)
            except Exception as e:
                logging.fatal("Exception while updating bag manifests: %s", format_exception(e))
                bdb.cleanup_bag(bag_path)
                raise
            finally:
                if remote_file_manifest and os.path.isfile(remote_file_manifest):
                    os.remove(remote_file_manifest)

            logging.info('Created bag: %s' % bag_path)

            if bag_archiver is not None:
                try:
                    archive = bdb.archive_bag(bag_path, bag_archiver.lower())
                    bdb.cleanup_bag(bag_path)
                    outputs = {os.path.basename(archive): {LOCAL_PATH_KEY: archive}}
                except Exception as e:
                    logging.error("Exception while creating data bag archive:", format_exception(e))
                    raise
            else:
                outputs = {os.path.basename(bag_path): {LOCAL_PATH_KEY: bag_path}}

        # 6. Execute anything in the post processing pipeline, if configured
        postprocessors = self.config.get('postprocessors', [])
        if postprocessors:
            for processor in postprocessors:
                processor_name = processor["processor"]
                processor_type = processor.get('processor_type', None)
                processor_parameters = processor.get('processor_parameters', None)

                try:
                    postprocessor = find_post_processor(processor_name, processor_type)
                    processor = postprocessor(
                        self.envars, input_files=outputs, processor_parameters=processor_parameters)
                    outputs = processor.process()
                except Exception as e:
                    logging.error(format_exception(e))
                    raise

        return outputs


class GenericDownloader(DerivaDownload):

    def __init__(self, server, **kwargs):
        DerivaDownload.__init__(self, server, **kwargs)
