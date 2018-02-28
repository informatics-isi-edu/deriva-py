import os
import time
import uuid
import logging
import platform
from bdbag import bdbag_api as bdb, bdbag_ro as ro, BAG_PROFILE_TAG, BDBAG_RO_PROFILE_ID
from deriva.core import ErmrestCatalog, HatracStore, format_exception, get_credential, read_config, stob, \
    __version__ as VERSION
from deriva.transfer.download.processors import findProcessor


class DerivaDownload(object):
    """

    """
    def __init__(self, server, output_dir=None, kwargs=None, config_file=None, credential_file=None):
        self.server = server
        self.output_dir = output_dir if output_dir else "."
        self.envars = kwargs if kwargs else dict()
        self.catalog = None
        self.store = None
        self.config = None
        self.cancelled = False
        self.metadata = dict()
        self.sessions = dict()

        info = "%s v%s [Python %s, %s]" % (
            self.__class__.__name__, VERSION, platform.python_version(), platform.platform())
        logging.info("Initializing downloader: %s" % info)

        if not self.server:
            raise RuntimeError("Server not specified!")

        # server variable initialization
        host = self.server.get('host', '')
        if not host:
            raise RuntimeError("Host not specified!")
        protocol = self.server.get('protocol', 'https')
        self.server_url = protocol + "://" + host
        catalog_id = self.server.get("catalog_id", "1")
        session_config = self.server.get('session')

        # credential initialization
        self.credentials = get_credential(host, credential_file)

        # catalog and file store initialization
        if self.catalog:
            del self.catalog
        self.catalog = ErmrestCatalog(protocol, host, catalog_id, self.credentials, session_config=session_config)
        if self.store:
            del self.store
        self.store = HatracStore(protocol, host, self.credentials, session_config=session_config)

        # process config file
        if config_file and os.path.isfile(config_file):
            self.config = read_config(config_file)

    def setConfig(self, config):
        self.config = config

    def setCredentials(self, credentials):
        host = self.server['host']
        self.credentials = credentials
        self.catalog.set_credentials(self.credentials, host)
        self.store.set_credentials(self.credentials, host)

    def download(self, identity=None):

        if not self.config:
            raise RuntimeError("No configuration specified!")

        if self.config.get("catalog") is None:
            raise RuntimeError("Catalog configuration error!")

        if not identity:
            logging.info("Validating credentials")
            try:
                attributes = self.catalog.get_authn_session().json()
                identity = attributes.get("client", {})
            except Exception as e:
                raise RuntimeError("Unable to validate credentials: %s" % format_exception(e))

        ro_manifest = None
        ro_author_name = None
        ro_author_orcid = None
        remote_file_manifest = os.path.abspath(
            ''.join([os.path.join(self.output_dir, 'remote-file-manifest_'), str(uuid.uuid4()), ".json"]))

        catalog_config = self.config['catalog']

        bag_path = None
        bag_archiver = None
        bag_config = self.config.get('bag')
        create_bag = True if bag_config else False
        if create_bag:
            bag_name = bag_config.get('bag_name', ''.join(["deriva_bag", '_', time.strftime("%Y-%m-%d_%H.%M.%S")]))
            bag_path = os.path.abspath(os.path.join(self.output_dir, bag_name))
            bag_archiver = bag_config.get('bag_archiver')
            bag_metadata = bag_config.get('bag_metadata', {"Internal-Sender-Identifier":
                                                           "deriva@%s" % self.server_url})
            bag_ro = create_bag and stob(bag_config.get('bag_ro', "True"))
            if create_bag:
                bdb.ensure_bag_path_exists(bag_path)
                bag = bdb.make_bag(bag_path, algs=['sha256'], metadata=bag_metadata)
                if bag_ro:
                    ro_author_name = identity.get('full_name', identity.get('display_name', identity.get('id', None)))
                    ro_author_orcid = bag.info.get("Contact-Orcid")
                    ro_manifest = ro.init_ro_manifest(author_name=ro_author_name, author_orcid=ro_author_orcid)
                    bag_metadata.update({BAG_PROFILE_TAG: BDBAG_RO_PROFILE_ID})

        base_path = bag_path if bag_path else self.output_dir
        for query in catalog_config['queries']:
            query_path = query['query_path']
            output_format = query['output_format']
            output_processor = query.get("output_processor")
            format_args = query.get('output_format_params', None)
            output_path = query.get('output_path', '')

            try:
                download_processor = findProcessor(output_format, output_processor)
                processor = download_processor(self.envars,
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
                processor.process()
            except Exception as e:
                logging.error(format_exception(e))
                if create_bag:
                    bdb.cleanup_bag(bag_path)
                raise

        if create_bag:
            try:
                if ro_manifest:
                    ro.write_bag_ro_manifest(ro_manifest, bag_path)
                if not os.path.isfile(remote_file_manifest):
                    remote_file_manifest = None
                bdb.make_bag(bag_path, remote_file_manifest=remote_file_manifest, update=True)
            except Exception as e:
                logging.fatal("Exception while updating bag manifests: %s", format_exception(e))
                bdb.cleanup_bag(bag_path)
                raise e
            finally:
                if remote_file_manifest and os.path.isfile(remote_file_manifest):
                    os.remove(remote_file_manifest)

            logging.info('Created bag: %s' % bag_path)

            if bag_archiver is not None:
                try:
                    archive = bdb.archive_bag(bag_path, bag_archiver.lower())
                    bdb.cleanup_bag(bag_path)
                    return archive
                except Exception as e:
                    logging.error("Exception while creating data bag archive:", format_exception(e))
                    raise e
            else:
                return bag_path
