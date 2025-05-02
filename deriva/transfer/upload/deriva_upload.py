import io
import os
import re
import sys
import datetime
import json
import shutil
import tempfile
import pathlib
import logging
import platform
import signal
from collections import OrderedDict
from deriva.core import ErmrestCatalog, HatracStore, HatracJobAborted, HatracJobPaused, \
    HatracJobTimeout, urlquote, urlparse, stob, format_exception, get_credential, read_config, write_config, \
    copy_config, resource_path, make_dirs, lock_file, DEFAULT_CHUNK_SIZE, __version__ as VERSION
from deriva.core import DEFAULT_SESSION_CONFIG, DEFAULT_CREDENTIAL_FILE
from deriva.core.utils import hash_utils as hu, mime_utils as mu, version_utils as vu
from deriva.transfer.upload import *
from deriva.transfer.upload.processors import find_processor
from deriva.transfer.upload.processors.base_processor import *
from deriva.transfer.upload.processors.archive_processor import ArchiveProcessor

try:
    from os import scandir, walk
except ImportError:
    from scandir import scandir, walk

logger = logging.getLogger(__name__)


class Enum(tuple):
    __getattr__ = tuple.index


UploadState = Enum(["Success", "Failed", "Pending", "Running", "Paused", "Aborted", "Cancelled", "Timeout"])
UploadMetadataReservedKeyNames = [
    "URI", "file_name", "file_ext", "file_size", "base_path", "base_name", "content-disposition", "md5", "sha256",
    "md5_base64", "sha256_base64", "schema", "table", "target_table", "_upload_year_", "_upload_month_", "_upload_day_",
    "_upload_time_", "_identity_id", "_identity_display_name", "_identity_full_name", "_identity_email"]

DefaultConfig = {
  "version_compatibility": [[">=%s" % VERSION]],
  "version_update_url": "https://github.com/informatics-isi-edu/deriva-py/releases",
  "asset_mappings": [
    {
      "asset_type": "table",
      "default_columns": ["RID", "RCB", "RMB", "RCT", "RMT"],
      "file_pattern": "^((?!/assets/).)*/records/(?P<schema>.+?)/(?P<table>.+?)[.]",
      "ext_pattern": "^.*[.](?P<file_ext>json|csv)$"
    }
  ]
}


class FileUploadState:
    def __init__(self, state=UploadState.Pending, status="Pending", result=None):
        self.state = state
        self.status = status
        self.result = result

    def asdict(self):
        return OrderedDict({
            "State": self.state,
            "Status": self.status,
            "Result": self.result
        })


class UploadEntry(object):
    def __init__(self, asset_group, asset_mapping, groupdict, path):
        self.asset_group = asset_group
        self.asset_mapping = asset_mapping
        self.groupdict = groupdict
        self.path = path


class DerivaUpload(object):
    """
    Base class for upload tasks. Encapsulates a catalog instance and a hatrac store instance and provides some common
    and reusable functions.

    This class is not intended to be instantiated directly, but rather extended by a specific implementation.
    """

    DefaultConfigFileName = "config.json"
    DefaultServerListFileName = "servers.json"
    DefaultTransferStateBaseName = ".deriva-upload-state"
    DefaultTransferStateFileName = "%s-%s.json"

    def __init__(self, config_file=None, credential_file=None, server=None, dcctx_cid=None):
        self.server_url = None
        self.catalog = None
        self.catalog_model = None
        self.store = None
        self.config = None
        self.credentials = None
        self.asset_mappings = None
        self.transfer_state = dict()
        self.transfer_state_fh = None
        self.transfer_state_locks = dict()
        self.cancelled = False
        self.metadata = dict()
        self.catalog_metadata = {"table_metadata": {}}
        self.processor_output = dict()
        self.identity = dict()
        self.file_list = OrderedDict()
        self.file_status = OrderedDict()
        self.skipped_files = set()
        self.override_config_file = config_file
        self.override_credential_file = credential_file
        self.server = self.getDefaultServer() if not server else server
        self.dcctx_cid = dcctx_cid if dcctx_cid else self.__class__.__name__
        signal.signal(signal.SIGINT, self.interrupt_handler)
        self.initialize()

    def __del__(self):
        self.cleanupTransferState()

    def interrupt_handler(self, signum, frame):
        logger.info("Caught interrupt signal.")
        self.cancel()

    def initialize(self, cleanup=False):
        info = "%s v%s [Python %s, %s]" % (
            self.__class__.__name__, vu.get_installed_version(VERSION),
            platform.python_version(), platform.platform(aliased=True))
        logger.info("Initializing uploader: %s" % info)

        # cleanup invalidates the current configuration and credentials in addition to clearing internal state
        if cleanup:
            self.cleanup()
        # reset just clears the internal state
        else:
            self.reset()

        if not self.server:
            logger.warning("A server was not specified and an internal default has not been set.")
            return

        # server variable initialization
        protocol = self.server.get('protocol', 'https')
        host = self.server.get('host', '')
        self.server_url = protocol + "://" + host
        catalog_id = self.server.get("catalog_id", "1")
        session_config = self.server.get('session', DEFAULT_SESSION_CONFIG.copy())
        # default credential initialization
        self.credentials = get_credential(host, self.override_credential_file or DEFAULT_CREDENTIAL_FILE)

        # catalog and file store initialization
        if self.catalog:
            del self.catalog
        self.catalog = ErmrestCatalog(protocol, host, catalog_id, self.credentials, session_config=session_config)
        if self.store:
            del self.store
        self.store = HatracStore(protocol, host, self.credentials, session_config=session_config)

        # determine identity
        if self.credentials:
            try:
                attributes = self.catalog.get_authn_session().json()
                self.identity = attributes.get("client", self.identity)
            except Exception as e:
                # not a big deal since the credential token being used could be expired
                logger.debug("Unable to determine user identity from existing credential (may be expired): %s" % e)

        # init dcctx cid to a default
        self.set_dcctx_cid(self.dcctx_cid)

        """
         Configuration initialization - this is a bit complex because we allow for:
             1. Run-time overriding of the config file location.
             2. Sub-classes of this class to bundle their own default configuration files in an arbitrary location.
             3. The updating of already deployed configuration files if bundled internal defaults are newer.             
        """
        if self.override_config_file and not os.path.isfile(self.override_config_file):
            raise DerivaUploadConfigurationError(
                "The configuration file %s could not be found." % self.override_config_file)

        config_file = self.override_config_file if self.override_config_file else None
        # 1. If we don't already have a valid (i.e., overridden) path to a config file...
        if not (config_file and os.path.isfile(config_file)):
            # 2. Get the currently deployed config file path, which could possibly be overridden by subclass
            config_file = self.getDeployedConfigFilePath()
            # 3. If the deployed default path is not valid, OR, it is valid AND is older than the bundled default
            if (not (config_file and os.path.isfile(config_file))
                    or self.isFileNewer(self.getDefaultConfigFilePath(), self.getDeployedConfigFilePath())):
                # 4. If we can locate a bundled default config file,
                if os.path.isfile(self.getDefaultConfigFilePath()):
                    # 4.1 Copy the bundled default config file to the deployment-specific config path
                    copy_config(self.getDefaultConfigFilePath(), config_file)
                else:
                    # 4.2 Otherwise, fallback to writing a failsafe default based on internal hardcoded settings
                    write_config(config_file, DefaultConfig)
        # 5. Finally, read the resolved configuration file into a config object
        self._update_internal_config(read_config(config_file))

    def set_dcctx_cid(self, cid):
        assert cid, "A dcctx cid is required"
        if self.catalog:
            self.catalog.dcctx['cid'] = cid
        if self.store:
            self.store.dcctx['cid'] = cid

    def _update_internal_config(self, config):
        """This updates the internal state of the uploader based on the config.
        """
        self.config = config
        # uploader initialization from configuration
        self.asset_mappings = self.config.get('asset_mappings', [])
        mu.add_types(self.config.get('mime_overrides'))

    def cancel(self):
        self.cancelled = True

    def reset(self):
        self.metadata.clear()
        self.file_list.clear()
        self.file_status.clear()
        self.skipped_files.clear()
        self.cleanupTransferState()
        self.cancelled = False

    def cleanup(self):
        self.reset()
        self.config = None
        self.credentials = None
        self.catalog_model = None

    def setServer(self, server):
        cleanup = self.server != server
        self.server = server
        self.initialize(cleanup)

    def setCredentials(self, credentials):
        host = self.server['host']
        self.credentials = credentials
        self.catalog.set_credentials(self.credentials, host)
        self.store.set_credentials(self.credentials, host)
        try:
            attributes = self.catalog.get_authn_session().json()
            self.identity = attributes.get("client", self.identity)
        except Exception as e:
            logger.warning("Unable to determine user identity: %s" % e)

    def setConfig(self, config_file):
        if not config_file:
            config = self.getUpdatedConfig()
            if config:
                write_config(self.getDeployedConfigFilePath(), config)
        else:
            self._update_internal_config(read_config(config_file))
        if not self.isVersionCompatible():
            raise RuntimeError("Upload version incompatibility detected",
                               "Current version: [%s], required version(s): %s." %
                               (self.getVersion(), self.getVersionCompatibility()))
    @classmethod
    def getDefaultServer(cls):
        servers = cls.getServers()
        for server in servers:
            lower = {k.lower(): v for k, v in server.items()}
            if lower.get("default", False):
                return server
        return servers[0] if len(servers) else {}

    @classmethod
    def getServers(cls):
        """
        This method must be implemented by subclasses.
        """
        raise NotImplementedError("This method must be implemented by a subclass.")

    @classmethod
    def getVersion(cls):
        """
        This method must be implemented by subclasses.
        """
        raise NotImplementedError("This method must be implemented by a subclass.")

    @classmethod
    def getConfigPath(cls):
        """
        This method must be implemented by subclasses.
        """
        raise NotImplementedError("This method must be implemented by a subclass.")

    @classmethod
    def getDeployedConfigPath(cls):
        return os.path.expanduser(os.path.normpath(cls.getConfigPath()))

    def getVersionCompatibility(self):
        return self.config.get("version_compatibility", list())

    def isVersionCompatible(self):
        compatibility = self.getVersionCompatibility()
        if len(compatibility) > 0:
            return vu.is_compatible(self.getVersion(), compatibility)
        else:
            return True

    @classmethod
    def getFileDisplayName(cls, file_path, asset_mapping=None):
        return os.path.basename(file_path)

    @staticmethod
    def isFileNewer(src, dst):
        if not (os.path.isfile(src) and os.path.isfile(dst)):
            return False

        # This comparison won't work with PyInstaller single-file bundles because the bundle is extracted to a temp dir
        # and every timestamp for every file in the bundle is reset to the bundle extraction/creation time.
        if getattr(sys, 'frozen', False):
            prefix = os.path.sep + "_MEI"
            if prefix in src:
                return False

        src_mtime = os.path.getmtime(os.path.abspath(src))
        dst_mtime = os.path.getmtime(os.path.abspath(dst))
        return src_mtime > dst_mtime

    @staticmethod
    def getFileSize(file_path):
        return os.path.getsize(file_path)

    @staticmethod
    def guessContentType(file_path):
        return mu.guess_content_type(file_path)

    @staticmethod
    def getFileHashes(file_path, hashes=frozenset(['md5'])):
        return hu.compute_file_hashes(file_path, hashes)

    @staticmethod
    def getCatalogTable(asset_mapping, metadata_dict=None):
        schema_name, table_name = asset_mapping.get('target_table', [None, None])
        # allow for template substitution in the schema/table name tuple
        if (schema_name and table_name) and metadata_dict is not None:
            schema_name = schema_name.format(**metadata_dict)
            table_name = table_name.format(**metadata_dict)
        if not (schema_name and table_name):
            metadata_dict_lower = {k.lower(): v for k, v in metadata_dict.items()}
            schema_name = metadata_dict_lower.get("schema")
            table_name = metadata_dict_lower.get("table")
        if not (schema_name and table_name):
            raise ValueError("Unable to determine target catalog table for asset type.")
        return '%s:%s' % (urlquote(schema_name), urlquote(table_name))

    @staticmethod
    def interpolateDict(src, dst, allow_none=False, allow_none_column_list=[]):
        if not (isinstance(src, dict) and isinstance(dst, dict)):
            raise ValueError("Invalid input parameter type(s): (src = %s, dst = %s), expected (dict, dict)" % (
                type(src).__name__, type(dst).__name__))

        dst = dst.copy()
        # prune None values from the src, we don't want those to be replaced with the string 'None' in the dest
        empty = [k for k, v in src.items() if v is None]
        for k in empty:
            del src[k]
        # perform the string replacement for the values in the destination dict
        for k, v in dst.items():
            try:
                value = v.format(**src)
            except KeyError:
                value = v
                if value:
                    if value.startswith('{') and value.endswith('}'):
                        value = None
            dst.update({k: value})
        # remove all None valued entries in the dest, if globally disallowed or the column is not explicitly allowed
        empty = [k for k, v in dst.items() if v is None]
        for k in empty:
            if not allow_none or (allow_none and k not in allow_none_column_list):
                del dst[k]

        return dst

    @staticmethod
    def pruneDict(src, dst, stringify=True):
        dst = dst.copy()
        for k in dst.keys():
            value = src.get(k)
            dst[k] = str(value) if (stringify and value is not None) else value
        return dst

    def getCurrentConfigFilePath(self):
        return self.override_config_file if self.override_config_file else self.getDeployedConfigFilePath()

    def getDefaultConfigFilePath(self):
        return os.path.normpath(resource_path(os.path.join("conf", self.DefaultConfigFileName)))

    def getDeployedConfigFilePath(self):
        return os.path.join(
            self.getDeployedConfigPath(), self.server.get('host', ''), self.DefaultConfigFileName)

    def getTransferStateFileName(self):
        return self.DefaultTransferStateFileName % \
               (self.DefaultTransferStateBaseName, self.server.get('host', 'localhost'))

    def getRemoteConfig(self):
        catalog_config = self.catalog.getCatalogModel()
        return catalog_config.bulk_upload

    def getUpdatedConfig(self):
        # if we are using an overridden config file, skip the update check
        if self.override_config_file:
            return

        logger.info("Checking for updated configuration...")
        remote_config = self.getRemoteConfig()
        if not remote_config:
            logger.info("Remote configuration not present, using default local configuration file.")
            return

        deployed_config_file_path = self.getDeployedConfigFilePath()
        if os.path.isfile(deployed_config_file_path):
            current_md5 = hu.compute_file_hashes(deployed_config_file_path, hashes=['md5'])['md5'][0]
        else:
            logger.info("Local config not found.")
            current_md5 = None
        tempdir = tempfile.mkdtemp(prefix="deriva_upload_")
        if os.path.exists(tempdir):
            updated_config_path = os.path.abspath(os.path.join(tempdir, DerivaUpload.DefaultConfigFileName))
            with io.open(updated_config_path, 'w', newline='\n', encoding='utf-8') as config:
                remote_config_data = json.dumps(
                    remote_config, ensure_ascii=False, sort_keys=True, separators=(',', ': '), indent=2)
                config.write(remote_config_data)
            new_md5 = hu.compute_file_hashes(updated_config_path, hashes=['md5'])['md5'][0]
            if current_md5 != new_md5:
                logger.info("Updated configuration found.")
                config = read_config(updated_config_path)
                self._update_internal_config(config)
            else:
                logger.info("Configuration is up-to-date.")
                config = None
            shutil.rmtree(tempdir, ignore_errors=True)

            return config

    def getFileStatusAsArray(self):
        result = list()
        for key in self.file_status.keys():
            item = {"File": key}
            item.update(self.file_status[key])
            result.append(item)
        return result

    @staticmethod
    def archive_preprocessing_enabled(asset_mapping):
        if not asset_mapping:
            return False
        if asset_mapping.get("archive_preprocessing_enabled", False):
            return True
        pre_processors = asset_mapping.get("pre_processors", [])
        for processor_config in pre_processors:
            processor_name = processor_config[PROCESSOR_NAME_KEY]
            processor_type = processor_config.get(PROCESSOR_TYPE_KEY)
            processor_impl = find_processor(processor_name, processor_type, bypass_whitelist=True)
            if issubclass(processor_impl, ArchiveProcessor):
                asset_mapping["archive_preprocessing_enabled"] = True
                return True
        return False

    def validateFile(self, root, path, name):
        if self.config.get("relative_path_validation", False):
            file_path = os.path.normpath(os.path.join(os.path.relpath(path, root), name))
        else:
            file_path = os.path.normpath(os.path.join(path, name))
        asset_group, asset_mapping, groupdict = self.getAssetMapping(file_path)
        if not asset_mapping:
            return None

        if self.archive_preprocessing_enabled(asset_mapping):
            final_path = os.path.abspath(os.path.normpath(groupdict.get("archive_path", path)))
        else:
            final_path = os.path.abspath(os.path.normpath(os.path.join(path, name)))

        return UploadEntry(asset_group, asset_mapping, groupdict, final_path)

    def scanDirectory(self, root, abort_on_invalid_input=False, purge_state=False):
        """

        :param root:
        :param abort_on_invalid_input:
        :param purge_state:
        :return:
        """
        root = os.path.abspath(root)
        if not os.path.isdir(root):
            raise FileNotFoundError("Invalid directory specified: [%s]" % root)
        self.loadTransferState(root, purge=purge_state)

        logger.info("Scanning files in directory [%s]..." % root)
        file_list = OrderedDict()
        for path, dirs, files in walk(root):
            for file_name in files:
                if file_name.startswith(self.DefaultTransferStateBaseName):
                    continue
                file_path = os.path.normpath(os.path.join(path, file_name))
                upload_entry = self.validateFile(root, path, file_name)
                if not upload_entry:
                    logger.info("Skipping file: [%s] -- Invalid file type or directory location." % file_path)
                    self.skipped_files.add(file_path)
                    if abort_on_invalid_input:
                        raise DerivaUploadError("Invalid input detected, aborting.")
                else:
                    asset_group = upload_entry.asset_group
                    group_list = file_list.get(asset_group, {})
                    group_list.update({upload_entry.path: upload_entry})
                    file_list[asset_group] = group_list

        # make sure that file entries in both self.file_list and self.file_status are ordered by the declared order of
        # the asset_mapping for the file
        for group in sorted(file_list.keys()):
            self.file_list[group] = file_list[group]
            for upload_entry in file_list[group].values():
                file_path = upload_entry.path
                logger.info("Including %s: [%s]." %
                            ("directory (for archive)" if self.archive_preprocessing_enabled(
                                upload_entry.asset_mapping) else "file", file_path))
                status = self.getTransferStateStatus(file_path)
                if status:
                    self.file_status[file_path] = FileUploadState(UploadState.Paused, status).asdict()
                else:
                    self.file_status[file_path] = FileUploadState(UploadState.Pending, "Pending").asdict()

    def getAssetMapping(self, file_path):
        """
        :param file_path:
        :return:
        """
        asset_group = -1
        for asset_type in self.asset_mappings:
            asset_group += 1
            groupdict = dict()
            dir_pattern = asset_type.get('dir_pattern', '')
            ext_pattern = asset_type.get('ext_pattern', '')
            file_pattern = asset_type.get('file_pattern', '')
            path = file_path.replace("\\", "/")
            if dir_pattern:
                match = re.search(dir_pattern, path)
                if not match:
                    logger.debug("The dir_pattern \"%s\" failed to match the input path [%s]" % (dir_pattern, path))
                    continue
                groupdict.update(match.groupdict())
            if ext_pattern:
                if self.archive_preprocessing_enabled(asset_type):
                    logger.warning("The 'ext_pattern' parameter is not compatible when archive preprocessing "
                                   "is enabled. Only input directories matching 'dir_pattern' are supported.")
                    continue
                match = re.search(ext_pattern, path, re.IGNORECASE)
                if not match:
                    logger.debug("The ext_pattern \"%s\" failed to match the input path [%s]" % (ext_pattern, path))
                    continue
                groupdict.update(match.groupdict())
            if file_pattern:
                if self.archive_preprocessing_enabled(asset_type):
                    logger.warning("The 'file_pattern' parameter is not compatible when archive preprocessing "
                                   "is enabled. Only input directories matching 'dir_pattern' are supported.")
                    continue
                match = re.search(file_pattern, path)
                if not match:
                    logger.debug("The file_pattern \"%s\" failed to match the input path [%s]" % (file_pattern, path))
                    continue
                groupdict.update(match.groupdict())

            return asset_group, asset_type, groupdict

        return None, None, None

    def uploadFiles(self, status_callback=None, file_callback=None):
        if not self.identity:
            raise DerivaUploadAuthenticationError("Unable to determine user identity for %s. "
                                                  "Please ensure that you are authenticated successfully." %
                                                  self.server_url)
        completed = 0
        for group, assets in self.file_list.items():
            if self.cancelled:
                break
            for entry in assets.values():
                if self.cancelled:
                    self.file_status[entry.path] = FileUploadState(UploadState.Cancelled, "Cancelled by user").asdict()
                    break
                try:
                    self.file_status[entry.path] = FileUploadState(UploadState.Running, "In-progress").asdict()
                    if status_callback:
                        status_callback()
                    result = self.uploadFile(entry.path,
                                             entry.asset_mapping,
                                             entry.groupdict,
                                             file_callback or self.defaultFileCallback)
                    if self.cancelled:
                        self.file_status[entry.path] = FileUploadState(UploadState.Cancelled,
                                                                       "Cancelled by user").asdict()
                        break
                    else:
                        self.file_status[entry.path] = FileUploadState(UploadState.Success, "Complete", result).asdict()
                        completed += 1
                except HatracJobPaused:
                    status = self.getTransferStateStatus(entry.path)
                    if status:
                        self.file_status[entry.path] = FileUploadState(
                            UploadState.Paused, "Paused: %s" % status).asdict()
                    continue
                except HatracJobTimeout:
                    status = self.getTransferStateStatus(entry.path)
                    if status:
                        self.file_status[entry.path] = FileUploadState(UploadState.Timeout, "Timeout").asdict()
                    continue
                except HatracJobAborted:
                    self.file_status[entry.path] = FileUploadState(UploadState.Aborted, "Aborted by user").asdict()
                except:
                    logger.debug("Unexpected exception", exc_info=sys.exc_info())
                    (etype, value, traceback) = sys.exc_info()
                    self.file_status[entry.path] = FileUploadState(UploadState.Failed, format_exception(value)).asdict()
                self.delTransferState(entry.path)
                if status_callback:
                    status_callback()

        failed_uploads = dict()
        try:
            for key, value in self.file_status.items():
                if (value["State"] == UploadState.Failed) or (value["State"] == UploadState.Timeout):
                    failed_uploads[key] = value["Status"]

            if self.skipped_files:
                logger.warning("The following %d file(s) were skipped because they did not satisfy the matching "
                               "criteria of the configuration:\n\n%s\n" %
                               (len(self.skipped_files), '\n'.join(sorted(self.skipped_files))))

            if failed_uploads:
                logger.warning("The following %d file(s) failed to upload due to errors:\n\n%s\n" %
                               (len(failed_uploads), '\n'.join(["%s -- %s" % (key, failed_uploads[key])
                                                                for key in sorted(failed_uploads.keys())])))
                raise RuntimeError("%s file(s) failed to upload due to errors." % len(failed_uploads))
        finally:
            logger.info("File upload processing completed: %s files were uploaded successfully, "
                        "%s files failed to upload due to errors, "
                        "%s files were skipped because they did not satisfy the matching criteria of the configuration."
                        % (completed, len(failed_uploads), len(self.skipped_files)))

        return self.file_status

    def uploadFile(self, file_path, asset_mapping, match_groupdict, callback=None):
        """
        Primary API subclass function.
        :param file_path:
        :param asset_mapping:
        :param match_groupdict:
        :param callback:
        :return:
        """
        logger.info("Processing: [%s]" % file_path)

        if asset_mapping.get("asset_type", "file") == "table":
            return self._uploadTable(file_path, asset_mapping, match_groupdict)
        else:
            return self._uploadAsset(file_path, asset_mapping, match_groupdict, callback)

    def _uploadAsset(self, file_path, asset_mapping, match_groupdict, callback=None):

        # 1. Populate initial file metadata from directory scan pattern matches
        self._initFileMetadata(file_path, asset_mapping, match_groupdict)

        # 2. Execute any configured preprocessors
        self._execute_processors(file_path, asset_mapping, match_groupdict, processor_list=PRE_PROCESSORS_KEY)
        if PROCESSOR_MODIFIED_FILE_PATH_KEY in self.processor_output:
            file_path = self.processor_output[PROCESSOR_MODIFIED_FILE_PATH_KEY]
        self._urlEncodeMetadata(asset_mapping.get("url_encoding_safe_overrides"))
        logger.info("Computed metadata for: [%s]." % file_path)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Current metadata: %s" % self.metadata)

        # 3. Compute checksum(s) for current file and add to metadata
        logger.info("Computing checksums for file: [%s]. Please wait..." % file_path)
        hashes = self.getFileHashes(file_path, asset_mapping.get('checksum_types', ['md5', 'sha256']))
        for alg, checksum in hashes.items():
            alg = alg.lower()
            self.metadata[alg] = checksum[0]
            self.metadata[alg + "_base64"] = checksum[1]
        if self.cancelled:
            return

        # 4. Populate additional metadata by querying the catalog
        self._queryFileMetadata(asset_mapping)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Updated metadata: %s" % self.metadata)

        # 5. If "create_record_before_upload" specified in asset_mapping, check for an existing record, creating a new
        #    one if necessary. Otherwise, delay this logic until after the file upload.
        result = record = None
        if stob(asset_mapping.get("create_record_before_upload", False)):
            record = self._getFileRecord(asset_mapping)

        # 6. Perform the Hatrac upload
        self._getFileHatracMetadata(asset_mapping)
        hatrac_options = asset_mapping.get("hatrac_options", {})
        file_size = self.metadata["file_size"]
        versioned_uri = \
            self._hatracUpload(self.metadata["URI"],
                               file_path,
                               md5=self.metadata.get("md5_base64"),
                               sha256=self.metadata.get("sha256_base64"),
                               content_type=self.guessContentType(file_path),
                               content_disposition=self.metadata.get("content-disposition"),
                               chunked=True if (file_size > DEFAULT_CHUNK_SIZE or file_size == 0) else False,
                               create_parents=stob(hatrac_options.get("create_parents", True)),
                               allow_versioning=stob(hatrac_options.get("allow_versioning", True)),
                               callback=callback)
        logger.debug("Hatrac upload successful. Result object URI: %s" % versioned_uri)
        versioned_uris = True
        if "versioned_uris" in hatrac_options:
            versioned_uris = stob(hatrac_options.get("versioned_uris", True))
        if "versioned_urls" in hatrac_options:
            versioned_uris = stob(hatrac_options.get("versioned_urls", True))
        if versioned_uris:
            self.metadata["URI"] = versioned_uri
        else:
            self.metadata["URI"] = versioned_uri.rsplit(":")[0]
        self.metadata["URI_urlencoded"] = urlquote(self.metadata["URI"])

        # 7. Check for an existing record and create a new one if necessary
        if not record:
            record, result = self._getFileRecord(asset_mapping)

        # 8. Update an existing record, if necessary
        column_map = asset_mapping.get("column_map", {})
        allow_none_col_list = asset_mapping.get("allow_empty_columns_on_update", [])
        allow_none = True if allow_none_col_list else False
        updated_record = self.interpolateDict(self.metadata, column_map, allow_none, allow_none_col_list)
        if updated_record != record:
            record_update_template = asset_mapping.get("record_update_template")
            require_record_update_template = stob(asset_mapping.get("require_record_update_template", False))
            if require_record_update_template and not record_update_template:
                raise DerivaUploadCatalogUpdateError(
                    "A required 'record_update_template' parameter for this asset mapping could not be found in the "
                    "configuration. The record will not be updated.")
            logger.info("Updating catalog for file [%s]" % self.getFileDisplayName(file_path))
            result = self._catalogRecordUpdate(self.metadata['target_table'],
                                               record,
                                               updated_record,
                                               record_update_template)[0]
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Updated catalog for file [%s]: %s" % (self.getFileDisplayName(file_path), result))
            record, result = self._getFileRecord(asset_mapping)

        # 9. Execute any configured post_processors
        self._execute_processors(file_path, asset_mapping, match_groupdict, processor_list=POST_PROCESSORS_KEY)

        return result

    def _uploadTable(self, file_path, asset_mapping, match_groupdict, callback=None):
        if self.cancelled:
            return None

        self._initFileMetadata(file_path, asset_mapping, match_groupdict)
        self._execute_processors(file_path, asset_mapping, match_groupdict, processor_list=PRE_PROCESSORS_KEY)
        try:
            default_columns = asset_mapping.get("default_columns")
            if not default_columns:
                default_columns = self.catalog.getDefaultColumns({}, self.metadata['target_table'])
            default_param = ('?defaults=%s' % ','.join(default_columns)) if len(default_columns) > 0 else ''
            file_ext = self.metadata['file_ext']
            file_ext = file_ext.lower()
            if file_ext == 'csv':
                headers = {'content-type': 'text/csv'}
            elif file_ext == 'json':
                headers = {'content-type': 'application/json'}
            else:
                raise DerivaUploadCatalogCreateError("Unsupported file type for catalog bulk upload: %s" % file_ext)
            with open(file_path, "rb") as fp:
                result = self.catalog.post(
                    '/entity/%s%s' % (self.metadata['target_table'], default_param), fp, headers=headers)
                return result
        except:
            (etype, value, traceback) = sys.exc_info()
            raise DerivaUploadCatalogCreateError(format_exception(value))
        finally:
            self._execute_processors(file_path, asset_mapping, match_groupdict, processor_list=POST_PROCESSORS_KEY)

    def _getFileRecord(self, asset_mapping):
        """
        Helper function that queries the catalog to get a record linked to the asset, or create it if it doesn't exist.
        :return: the file record
        """
        record = None
        column_map = asset_mapping.get("column_map", {})
        rqt = asset_mapping['record_query_template']
        try:
            path = rqt.format(**self.metadata)
        except KeyError as e:
            raise DerivaUploadConfigurationError("Record query template substitution error: %s" % format_exception(e))
        result = self.catalog.get(path).json()
        if result:
            record = result[0]
            self._updateFileMetadata(record, no_overwrite=True)
            return self.pruneDict(record, column_map), record
        else:
            row = self.interpolateDict(self.metadata, column_map)
            result = self._catalogRecordCreate(self.metadata['target_table'], row)
            if result:
                record = result[0]
                self._updateFileMetadata(record)
            return self.interpolateDict(self.metadata, column_map, allow_none=True), record

    def _urlEncodeMetadata(self, safe_overrides=None):
        urlencoded = dict()
        if not safe_overrides:
            safe_overrides = dict()
        for k, v in self.metadata.items():
            if k.endswith("_urlencoded"):
                continue
            urlencoded[k + "_urlencoded"] = urlquote(str(v), safe_overrides.get(k, ""))
        self._updateFileMetadata(urlencoded)

    def _initFileMetadata(self, file_path, asset_mapping, match_groupdict):
        self.metadata.clear()
        self._updateFileMetadata(match_groupdict)
        self.metadata['target_table'] = self.getCatalogTable(asset_mapping, match_groupdict)

        self.metadata["file_name"] = self.getFileDisplayName(file_path)
        self.metadata["file_size"] = self.getFileSize(file_path)
        if "file_ext" not in self.metadata:
            self.metadata["file_ext"] = "".join(pathlib.PurePath(file_path).suffixes)
        self.metadata["base_path"] = os.path.dirname(file_path)
        self.metadata["base_name"] = self.metadata["file_name"].rsplit(
            self.metadata["file_ext"])[0] if self.metadata["file_ext"] else self.metadata["file_name"]

        time = datetime.datetime.now()
        self.metadata["_upload_year_"] = time.year
        self.metadata["_upload_month_"] = time.month
        self.metadata["_upload_day_"] = time.day
        self.metadata["_upload_time_"] = time.timestamp()
        self.metadata["_identity_id"] = self.identity.get("id", "anonymous")
        self.metadata["_identity_display_name"] = self.identity.get("display_name")
        self.metadata["_identity_full_name"] = self.identity.get("full_name")
        self.metadata["_identity_email"] = self.identity.get("email")

        self._urlEncodeMetadata(asset_mapping.get("url_encoding_safe_overrides"))

    def _updateFileMetadata(self, src, strict=False, no_overwrite=False):
        if not (isinstance(src, dict)):
            raise ValueError("Invalid input parameter type(s): (src = %s), expected (dict)" % type(src).__name__)
        dst = src.copy()
        for k in src.keys():
            if strict:
                if k in UploadMetadataReservedKeyNames:
                    logger.warning("Context metadata update specified reserved key name [%s], "
                                   "ignoring value: %s " % (k, src[k]))
                    del dst[k]
                    continue
            # don't overwrite any existing metadata field
            if no_overwrite:
                if k in self.metadata:
                    del dst[k]
        self.metadata.update(dst)

    def _queryFileMetadata(self, asset_mapping):
        """
        Helper function that queries the catalog to get required metadata for a given file/asset
        """
        metadata_queries = asset_mapping.get("metadata_query_templates", [])
        if logger.isEnabledFor(logging.DEBUG) and metadata_queries:
            logger.debug("Querying catalog for additional metadata...")
        for uri in metadata_queries:
            try:
                path = uri.format(**self.metadata)
            except KeyError as e:
                raise RuntimeError("Metadata query template substitution error: %s" % format_exception(e))
            result = self.catalog.get(path).json()
            if result:
                self._updateFileMetadata(result[0], True)
            else:
                raise RuntimeError("Metadata query did not return any results: %s" % path)

        self._getFileExtensionMetadata(self.metadata.get("file_ext"))

        for k, v in asset_mapping.get("column_value_templates", {}).items():
            try:
                self.metadata[k] = v.format(**self.metadata)
            except KeyError as e:
                logger.warning("Column value template substitution error: %s" % format_exception(e))
                continue

        self._urlEncodeMetadata(asset_mapping.get("url_encoding_safe_overrides"))

    def _getFileExtensionMetadata(self, ext):
        ext_map = self.config.get("file_ext_mappings", {})
        entry = ext_map.get(ext)
        if entry:
            self._updateFileMetadata(entry)

    def _getFileHatracMetadata(self, asset_mapping):
        try:
            hatrac_templates = asset_mapping["hatrac_templates"]
            # URI is required
            self.metadata["URI"] = hatrac_templates["hatrac_uri"].format(**self.metadata)
            # overridden content-disposition is optional
            content_disposition = hatrac_templates.get("content-disposition")
            if content_disposition:
                filename = content_disposition.format(**self.metadata)
            else:
                filename = urlparse(self.metadata["URI"]).path.rsplit("/", 1)[-1]

            sanitized_filename, sanitized_content_disp = \
                self._validateHatracFilename(filename, asset_mapping.get("hatrac_options", {}))
            if content_disposition:
                self.metadata["content-disposition"] = sanitized_content_disp
            else:
                self.metadata["URI"] = self.metadata["URI"].replace(filename, sanitized_filename)

            self._urlEncodeMetadata(asset_mapping.get("url_encoding_safe_overrides"))
        except KeyError as e:
            raise DerivaUploadConfigurationError("Hatrac template substitution error: %s" % format_exception(e))

    def _validateHatracFilename(self, filename, hatrac_options):
        if not filename:
            return None

        sanitize = hatrac_options.get("sanitize_filenames", True)
        pattern = hatrac_options.get("sanitize_filenames_pattern")
        is_content_disp = re.match(r"filename\*?=['\"]?(?:UTF-\d['\"]*)?([^;\r\n\"']*)['\"]?;?", filename)
        if is_content_disp:
            filename = is_content_disp.group(1)
        pattern = pattern if pattern else "[^a-zA-Z0-9_.-]"
        sanitized_filename = urlquote(re.sub(pattern, "_", filename)) if sanitize else filename
        if is_content_disp:
            content_disp = is_content_disp.string.replace(filename, sanitized_filename)
        else:
            content_disp = "filename*=UTF-8''" + sanitized_filename

        return sanitized_filename, content_disp

    def _hatracUpload(self,
                      uri,
                      file_path,
                      md5=None,
                      sha256=None,
                      content_type=None,
                      content_disposition=None,
                      chunked=True,
                      create_parents=True,
                      allow_versioning=True,
                      callback=None):

        # check if there is already an in-progress transfer for this file,
        # and if so, that the local file has not been modified since the original upload job was created
        can_resume = False
        transfer_state = self.getTransferState(file_path)
        if transfer_state:
            content_md5 = transfer_state.get("content-md5")
            content_sha256 = transfer_state.get("content-sha256")
            if content_md5 or content_sha256:
                if (md5 == content_md5) or (sha256 == content_sha256):
                    can_resume = True

        if transfer_state and can_resume:
            logger.info("Resuming upload (%s) of file: [%s] to host %s. Please wait..." % (
                self.getTransferStateStatus(file_path), file_path, transfer_state.get("host")))
            path = transfer_state["target"]
            job_id = transfer_state['url'].rsplit("/", 1)[1]
            if not (transfer_state["total"] == transfer_state["completed"]):
                self.store.put_obj_chunked(path,
                                           file_path,
                                           job_id,
                                           callback=callback,
                                           start_chunk=transfer_state["completed"],
                                           cancel_job_on_error=False)
            return self.store.finalize_upload_job(path, job_id)
        else:
            logger.info("Uploading file: [%s] to host %s. Please wait..." % (file_path, self.server_url))
            return self.store.put_loc(uri,
                                      file_path,
                                      md5=md5,
                                      sha256=sha256,
                                      content_type=content_type,
                                      content_disposition=content_disposition,
                                      chunked=chunked,
                                      create_parents=create_parents,
                                      allow_versioning=allow_versioning,
                                      callback=callback,
                                      cancel_job_on_error=False)

    def _get_catalog_table_columns(self, table):
        table_columns = set()
        catalog_table_metadata = self.catalog_metadata["table_metadata"]
        table_metadata = catalog_table_metadata.get(table)
        if table_metadata:
            table_columns = table_metadata.get("table_columns")
        if not table_columns:
            table_columns = self.catalog.getTableColumns(table)
            catalog_table_metadata.update({table: {"table_columns": table_columns}})
        return table_columns

    def _validate_catalog_row_columns(self, row, table):
        return set(row.keys()) - self._get_catalog_table_columns(table)

    def _validate_row_key_constraints(self, catalog_table, row):
        logger.debug("Validating row key constraints for %s: %s" % (catalog_table, row))
        if not self.catalog_model:
            logger.debug("Fetching catalog model...")
            self.catalog_model = self.catalog.getCatalogModel()
        schema_name, table_name = self.catalog.splitQualifiedCatalogName(catalog_table)
        schema = self.catalog_model.schemas.get(schema_name)
        table = schema.tables.get(table_name)
        non_null_correlations = {cname for cname, cval in row.items() if cval is not None}
        for key in table.keys:
            if set(key.unique_columns.elements).issubset(non_null_correlations):
                logger.debug("%s is a subset of non-null correlations %s" %
                             (set(key.unique_columns.elements), non_null_correlations))
                return True  # it is safe
            else:
                logger.debug("%s is not a subset of non-null correlations %s" %
                             (set(key.unique_columns.elements), non_null_correlations))
        return False  # it is not safe

    def _get_catalog_default_columns(self, row, table, exclude=None, quote_url=True):
        columns = self._get_catalog_table_columns(table)
        if isinstance(exclude, list):
            for col in exclude:
                columns.remove(col)

        defaults = []
        supplied_columns = row.keys()
        for col in columns:
            if col not in supplied_columns:
                defaults.append(urlquote(col, safe='') if quote_url else col)

        return defaults

    def _catalogRecordCreate(self, catalog_table, row, default_columns=None):
        """

        :param catalog_table:
        :param row:
        :param default_columns:
        :return:
        """
        if self.cancelled:
            return None

        try:
            missing = self._validate_catalog_row_columns(row, catalog_table)
            if missing:
                raise ValueError(
                    "Unable to update catalog entry because one or more specified columns do not exist in the "
                    "target table: [%s]" % ','.join(missing))
            if not default_columns:
                default_columns = self._get_catalog_default_columns(row, catalog_table)
            default_param = ('?defaults=%s' % ','.join(default_columns)) if len(default_columns) > 0 else ''
            # for default in default_columns:
            #    row[default] = None
            create_uri = '/entity/%s%s' % (catalog_table, default_param)
            logger.debug(
                "Attempting catalog record create [%s] with data: %s" % (create_uri, json.dumps(row)))
            return self.catalog.post(create_uri, json=[row]).json()
        except:
            (etype, value, traceback) = sys.exc_info()
            raise DerivaUploadCatalogCreateError(format_exception(value))

    def _catalogRecordUpdate(self, catalog_table, old_row, new_row, record_update_template=None):
        """

        :param catalog_table:
        :param new_row:
        :param old_row:
        :return:
        """
        if self.cancelled:
            return None

        try:
            keys = sorted(list(new_row.keys()))
            old_keys = sorted(list(old_row.keys()))
            if keys != old_keys:
                raise RuntimeError("Cannot update catalog - "
                                   "new row column list and old row column list do not match: New: %s != Old: %s" %
                                   (keys, old_keys))
            o_keys = ','.join(["o%d:=%s" % (i, urlquote(keys[i])) for i in range(len(keys))])
            n_keys = ','.join(["n%d:=%s" % (i, urlquote(keys[i])) for i in range(len(keys))])
            update_row = {
                'o%d' % i: old_row[keys[i]]
                for i in range(len(keys))
            }
            update_row.update({
                'n%d' % i: new_row[keys[i]]
                for i in range(len(keys))
            })
            if record_update_template:
                update_uri = record_update_template.format(**self.metadata)
                update_row = new_row
            else:
                update_uri = '/attributegroup/%s/%s;%s' % (catalog_table, o_keys, n_keys)
                if self.config.get("strict_update_check", True) and not \
                        self._validate_row_key_constraints(catalog_table, old_row):
                    raise ValueError(
                        "Potential unsafe attributegroup update [%s]: at least one pre-existing, non-null correlation "
                        "key is required. Old values: %s, New values: %s" %
                        (update_uri, json.dumps(old_row), json.dumps(new_row)))
            logger.debug(
                "Attempting catalog record update [%s] with data: %s" % (update_uri, json.dumps(update_row)))
            return self.catalog.put(update_uri, json=[update_row]).json()
        except:
            (etype, value, traceback) = sys.exc_info()
            raise DerivaUploadCatalogUpdateError(format_exception(value))

    def _execute_processors(self, file_path, asset_mapping, match_groupdict,
                            processor_list=PRE_PROCESSORS_KEY, **kwargs):
        processors = asset_mapping.get(processor_list, [])
        if processors:
            for processor_config in processors:
                processor_name = processor_config[PROCESSOR_NAME_KEY]
                processor_type = processor_config.get(PROCESSOR_TYPE_KEY)
                processor_params = processor_config.get(PROCESSOR_PARAMS_KEY)
                try:
                    processor_impl = find_processor(processor_name, processor_type, bypass_whitelist=True)
                    processor = processor_impl(
                        processor_params=processor_params,
                        file_path=file_path,
                        asset_mapping=asset_mapping,
                        match_groupdict=match_groupdict,
                        metadata=self.metadata,
                        processor_output=self.processor_output,
                        **kwargs)
                    proc_class = processor.__class__.__module__
                    proc_name = processor.__class__.__name__
                    if processor_params is not None and processor_params.get(
                            PROCESSOR_REQUIRES_METADATA_QUERY_KEY, False):
                        self._queryFileMetadata(asset_mapping)
                    logger.debug("Attempting to execute upload processor class %s from module: %s" %
                                 (proc_name, proc_class))
                    output = processor.process()
                    if isinstance(output, dict):
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug("%s output context: %s" % (proc_name, output))
                except:
                    (etype, value, traceback) = sys.exc_info()
                    raise DerivaUploadError(format_exception(value))

    def defaultFileCallback(self, **kwargs):
        completed = kwargs.get("completed")
        total = kwargs.get("total")
        file_path = kwargs.get("file_path")
        file_name = os.path.basename(file_path) if file_path else ""
        job_info = kwargs.get("job_info", {})
        job_info.update()
        if completed and total:
            file_name = " [%s]" % file_name
            job_info.update({"completed": completed, "total": total, "host": kwargs.get("host")})
            status = "Uploading file%s: %d%% complete" % (
                file_name, round(((float(completed) / float(total)) % 100) * 100))
            self.setTransferState(file_path, job_info)
        else:
            summary = kwargs.get("summary", "")
            file_name = "Uploaded file: [%s] " % file_name
            status = file_name  # + summary
        if status:
            # logger.debug(status)
            pass
        if self.cancelled:
            return -1

        return True

    @staticmethod
    def find_file_in_dir_hierarchy(filename, path):
        """ Find all instances of a filename in the entire directory hierarchy specified by path.
        """
        file_paths = set()
        # First, descend from the base path looking for filename in all sub dirs
        for root, dirs, files in walk(path, followlinks=True):
            if filename in files:
                # found a file
                found_path = os.path.normcase(os.path.join(root, filename))
                file_paths.add(os.path.normcase(os.path.realpath(found_path)))
                continue

        # Next, ascend from the base path looking for the same filename in all parent dirs
        current = path
        while True:
            parent = os.path.dirname(current)
            if parent == current:
                break
            for entry in scandir(parent):
                if (entry.name == filename) and entry.is_file():
                    file_paths.add(os.path.normcase(os.path.realpath(os.path.normcase(entry.path))))
            current = parent

        return file_paths

    def delete_dependent_locks(self, directory):
        for path in self.find_file_in_dir_hierarchy(self.getTransferStateFileName(), directory):
            logger.info("Attempting to delete an existing transfer state file (dependent lock) at: [%s]" % path)
            try:
                os.remove(path)
            except OSError as e:
                logger.warning("Unable to delete transfer state file [%s]: %s" % (path, format_exception(e)))

    def acquire_dependent_locks(self, directory):
        for path in self.find_file_in_dir_hierarchy(self.getTransferStateFileName(), directory):
            logger.info("Attempting to acquire a dependent lock in [%s]" % os.path.dirname(path))
            try:
                transfer_state_lock = lock_file(path, 'r+')
                transfer_state_fh = transfer_state_lock.acquire(timeout=0, fail_when_locked=True)
                self.transfer_state_locks.update({path: {"lock": transfer_state_lock, "handle": transfer_state_fh}})
            except Exception as e:
                raise DerivaUploadError("Unable to acquire resource lock for directory [%s]. "
                                        "Multiple upload processes cannot operate within the same directory hierarchy. "
                                        "%s" % (os.path.dirname(path), format_exception(e)))

    def loadTransferState(self, directory, purge=False):
        transfer_state_file_path = os.path.normcase(os.path.join(directory, self.getTransferStateFileName()))
        if purge:
            self.delete_dependent_locks(directory)
        self.acquire_dependent_locks(directory)
        try:
            if not os.path.isfile(transfer_state_file_path):
                with lock_file(transfer_state_file_path, "w") as tsfp:
                    json.dump(self.transfer_state, tsfp)

            transfer_state_lock = self.transfer_state_locks.get(transfer_state_file_path)
            if transfer_state_lock:
                self.transfer_state_fh = transfer_state_lock["handle"]
            else:
                transfer_state_lock = lock_file(transfer_state_file_path, 'r+')
                self.transfer_state_fh = transfer_state_lock.acquire(timeout=0, fail_when_locked=True)
                self.transfer_state_locks.update(
                    {directory: {"lock": transfer_state_lock, "handle": self.transfer_state_fh}})
            self.transfer_state = json.load(self.transfer_state_fh, object_pairs_hook=OrderedDict)
        except Exception as e:
            raise DerivaUploadError("Unable to acquire resource lock for directory [%s]. "
                                    "Multiple upload processes cannot operate within the same directory hierarchy. %s"
                                    % (directory, format_exception(e)))

    def getTransferState(self, file_path):
        return self.transfer_state.get(file_path)

    def setTransferState(self, file_path, transfer_state):
        self.transfer_state[file_path] = transfer_state
        self.writeTransferState()

    def delTransferState(self, file_path):
        transfer_state = self.getTransferState(file_path)
        if transfer_state:
            del self.transfer_state[file_path]
        self.writeTransferState()

    def writeTransferState(self):
        if not self.transfer_state_fh:
            return
        try:
            self.transfer_state_fh.seek(0, 0)
            self.transfer_state_fh.truncate()
            json.dump(self.transfer_state, self.transfer_state_fh, indent=2)
            self.transfer_state_fh.flush()
            os.fsync(self.transfer_state_fh.fileno())
        except Exception as e:
            logger.warning("Unable to write transfer state file: %s" % format_exception(e))

    def cleanupTransferState(self):
        if self.transfer_state_fh and not self.transfer_state_fh.closed:
            try:
                self.transfer_state_fh.flush()
                os.fsync(self.transfer_state_fh.fileno())
            except Exception as e:
                logger.warning("Unable to flush/close transfer state file: %s" % format_exception(e))
            finally:
                for entry in self.transfer_state_locks.values():
                    lock = entry.get("lock")
                    if lock and not lock.fh.closed:
                        lock.release()
                self.transfer_state_locks.clear()
                self.transfer_state_fh = None

    def getTransferStateStatus(self, file_path):
        transfer_state = self.getTransferState(file_path)
        if transfer_state:
            return "%d%% complete" % (
                round(((float(transfer_state["completed"]) / float(transfer_state["total"])) % 100) * 100))
        return None


class GenericUploader(DerivaUpload):

    def __init__(self, config_file=None, credential_file=None, server=None, dcctx_cid=None):
        DerivaUpload.__init__(self,
                              config_file=config_file,
                              credential_file=credential_file,
                              server=server,
                              dcctx_cid=dcctx_cid)

    @classmethod
    def getVersion(cls):
        return VERSION

    @classmethod
    def getConfigPath(cls):
        return "~/.deriva/upload/"

    @classmethod
    def getServers(cls):
        return read_config(os.path.join(
            cls.getDeployedConfigPath(), cls.DefaultServerListFileName), create_default=True, default=[])

    @classmethod
    def setServers(cls, servers):
        return write_config(os.path.join(cls.getDeployedConfigPath(), cls.DefaultServerListFileName), servers)
