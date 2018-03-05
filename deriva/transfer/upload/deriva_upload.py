import io
import os
import re
import sys
import errno
import json
import shutil
import tempfile
import logging
import platform
from collections import OrderedDict, namedtuple
from deriva.core import ErmrestCatalog, CatalogConfig, HatracStore, HatracJobAborted, HatracJobPaused, urlquote, stob, \
    format_exception, get_credential, read_config, write_config, copy_config, resource_path, __version__ as VERSION
from deriva.core.utils import hash_utils as hu, mime_utils as mu, version_utils as vu

try:
    from os import scandir, walk
except ImportError:
    from scandir import scandir, walk


class ConfigurationError (Exception):
    pass


class CatalogCreateError (Exception):
    pass


class CatalogUpdateError (Exception):
    pass


class Enum(tuple):
    __getattr__ = tuple.index


UploadState = Enum(["Success", "Failed", "Pending", "Running", "Paused", "Aborted", "Cancelled"])
FileUploadState = namedtuple("FileUploadState", ["State", "Status"])

DefaultConfig = {
  "version_compatibility": [">=%s" % VERSION],
  "version_update_url": "https://github.com/informatics-isi-edu/deriva-py/releases",
  "asset_mappings": [
    {
      "asset_type": "data",
      "default_columns": ["RID", "RCB", "RMB", "RCT", "RMT"],
      "file_pattern": "^((?!/assets/).)*/records/(?P<schema>.+?)/(?P<table>.+?)[.]",
      "ext_pattern": "^.*[.](?P<file_ext>json|csv)$"
    }
  ]
}


class DerivaUpload(object):
    """
    Base class for upload tasks. Encapsulates a catalog instance and a hatrac store instance and provides some common
    and reusable functions.

    This class is not intended to be instantiated directly, but rather extended by a deployment specific implementation.
    """

    DefaultConfigFileName = "config.json"
    DefaultServerListFileName = "servers.json"
    DefaultTransferStateFileName = "transfers.json"

    def __init__(self, config_file=None, credential_file=None, server=None):
        self.server_url = None
        self.catalog = None
        self.store = None
        self.config = None
        self.credentials = None
        self.asset_mappings = None
        self.transfer_state = dict()
        self.transfer_state_fp = None
        self.cancelled = False
        self.metadata = dict()

        self.file_list = OrderedDict()
        self.file_status = OrderedDict()
        self.skipped_files = set()
        self.override_config_file = config_file
        self.override_credential_file = credential_file
        self.server = self.getDefaultServer() if not server else server
        self.initialize()

    def __del__(self):
        self.cleanupTransferState()

    def initialize(self, cleanup=False):
        info = "%s v%s [Python %s, %s]" % (
            self.__class__.__name__, VERSION, platform.python_version(), platform.platform(aliased=True))
        logging.info("Initializing uploader: %s" % info)

        # cleanup invalidates the current configuration and credentials in addition to clearing internal state
        if cleanup:
            self.cleanup()
        # reset just clears the internal state
        else:
            self.reset()

        if not self.server:
            logging.warning("A server was not specified and an internal default has not been set.")
            return

        # server variable initialization
        protocol = self.server.get('protocol', 'https')
        host = self.server.get('host', '')
        self.server_url = protocol + "://" + host
        catalog_id = self.server.get("catalog_id", "1")
        session_config = self.server.get('session')

        # overriden credential initialization
        if self.override_credential_file:
            self.credentials = get_credential(host, self.override_config_file)

        # catalog and file store initialization
        if self.catalog:
            del self.catalog
        self.catalog = ErmrestCatalog(protocol, host, catalog_id, self.credentials, session_config=session_config)
        if self.store:
            del self.store
        self.store = HatracStore(protocol, host, self.credentials, session_config=session_config)

        # transfer state initialization
        self.loadTransferState()

        """
         Configuration initialization - this is a bit complex because we allow for:
             1. Run-time overriding of the config file location.
             2. Sub-classes of this class to bundle their own default configuration files in an arbitrary location.
             3. The updating of already deployed configuration files if bundled internal defaults are newer.             
        """
        config_file = self.override_config_file if self.override_config_file else None
        # 1. If we don't already have a valid (i.e., overridden) path to a config file...
        if not (config_file and os.path.isfile(config_file)):
            # 2. Get the currently deployed config file path, which could possibly be overridden by subclass
            config_file = self.getDeployedConfigFilePath()
            # 3. If the deployed default path is not valid, OR, it is valid AND is older than the bundled default
            if (not(config_file and os.path.isfile(config_file))
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
        self.cancelled = False

    def cleanup(self):
        self.reset()
        self.config = None
        self.credentials = None
        self.cleanupTransferState()

    def setServer(self, server):
        cleanup = self.server != server
        self.server = server
        self.initialize(cleanup)

    def setCredentials(self, credentials):
        host = self.server['host']
        self.credentials = credentials
        self.catalog.set_credentials(self.credentials, host)
        self.store.set_credentials(self.credentials, host)

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

        # This comparison wont work with PyInstaller single-file bundles because the bundle is extracted to a temp dir
        # and every timestamp for every file in the bundle is reset to the bundle extraction/creation time.
        if getattr(sys, 'frozen', False):
            prefix = os.sep + "_MEI"
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
        if not (schema_name and table_name):
            metadata_dict_lower = {k.lower(): v for k, v in metadata_dict.items()}
            schema_name = metadata_dict_lower.get("schema")
            table_name = metadata_dict_lower.get("table")
        if not (schema_name and table_name):
            raise ValueError("Unable to determine target catalog table for asset type.")
        return '%s:%s' % (urlquote(schema_name), urlquote(table_name))

    @staticmethod
    def interpolateDict(src, dst, allowNone=False):
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
        # remove all None valued entries in the dest, if disallowed
        if not allowNone:
            empty = [k for k, v in dst.items() if v is None]
            for k in empty:
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

    def getDeployedTransferStateFilePath(self):
        return os.path.join(
            self.getDeployedConfigPath(), self.server.get('host', ''), self.DefaultTransferStateFileName)

    def getRemoteConfig(self):
        catalog_config = CatalogConfig.fromcatalog(self.catalog)
        return catalog_config.annotation_obj("tag:isrd.isi.edu,2017:bulk-upload")

    def getUpdatedConfig(self):
        # if we are using an overridden config file, skip the update check
        if self.override_config_file:
            return

        logging.info("Checking for updated configuration...")
        remote_config = self.getRemoteConfig()
        if not remote_config:
            logging.info("Remote configuration not present, using default local configuration file.")
            return

        deployed_config_file_path = self.getDeployedConfigFilePath()
        if os.path.isfile(deployed_config_file_path):
            current_md5 = hu.compute_file_hashes(deployed_config_file_path, hashes=['md5'])['md5'][0]
        else:
            logging.info("Local config not found.")
            current_md5 = None
        tempdir = tempfile.mkdtemp(prefix="deriva_upload_")
        if os.path.exists(tempdir):
            updated_config_path = os.path.abspath(os.path.join(tempdir, DerivaUpload.DefaultConfigFileName))
            with io.open(updated_config_path, 'w', newline='\n', encoding='utf-8') as config:
                config.write(json.dumps(
                    remote_config, ensure_ascii=False, sort_keys=True, separators=(',', ': '), indent=2))
            new_md5 = hu.compute_file_hashes(updated_config_path, hashes=['md5'])['md5'][0]
            if current_md5 != new_md5:
                logging.info("Updated configuration found.")
                config = read_config(updated_config_path)
                self._update_internal_config(config)
            else:
                logging.info("Configuration is up-to-date.")
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

    def validateFile(self, root, path, name):
        file_path = os.path.normpath(os.path.join(path, name))
        asset_mapping, groupdict = self.getAssetMapping(file_path)
        if not asset_mapping:
            return None

        return {file_path: (asset_mapping, groupdict)}

    def scanDirectory(self, root, abort_on_invalid_input=False):
        """

        :param root:
        :param abort_on_invalid_input:
        :return:
        """
        root = os.path.abspath(root)
        if not os.path.isdir(root):
            raise ValueError("Invalid directory specified: [%s]" % root)

        logging.info("Scanning files in directory [%s]..." % root)
        for path, dirs, files in walk(root):
            for file_name in files:
                file_path = os.path.normpath(os.path.join(path, file_name))
                file_entry = self.validateFile(root, path, file_name)
                if not file_entry:
                    logging.info("Skipping file: [%s] -- Invalid file type or directory location." % file_path)
                    self.skipped_files.add(file_path)
                    if abort_on_invalid_input:
                        raise ValueError("Invalid input detected, aborting.")
                else:
                    logging.info("Including file: [%s]." % file_path)
                    self.file_list.update(file_entry)
                    status = self.getTransferStateStatus(file_path)
                    if status:
                        self.file_status[file_path] = FileUploadState(UploadState.Paused, status)._asdict()
                    else:
                        self.file_status[file_path] = FileUploadState(UploadState.Pending, "Pending")._asdict()

    def getAssetMapping(self, file_path):
        """
        :param file_path:
        :return:
        """
        for asset_type in self.asset_mappings:
            groupdict = dict()
            dir_pattern = asset_type.get('dir_pattern', '')
            ext_pattern = asset_type.get('ext_pattern', '')
            file_pattern = asset_type.get('file_pattern', '')
            path = file_path.replace("\\", "/")
            if dir_pattern:
                match = re.search(dir_pattern, path)
                if not match:
                    continue
                groupdict.update(match.groupdict())
            if ext_pattern:
                match = re.search(ext_pattern, path, re.IGNORECASE)
                if not match:
                    continue
                groupdict.update(match.groupdict())
            if file_pattern:
                match = re.search(file_pattern, path)
                if not match:
                    continue
                groupdict.update(match.groupdict())

            return asset_type, groupdict

        return None, None

    def uploadFiles(self, status_callback=None, file_callback=None):
        for file_path, (asset_mapping, groupdict) in self.file_list.items():
            if self.cancelled:
                self.file_status[file_path] = FileUploadState(UploadState.Cancelled, "Cancelled by user")._asdict()
                continue
            try:
                self.file_status[file_path] = FileUploadState(UploadState.Running, "In-progress")._asdict()
                if status_callback:
                    status_callback()
                self.uploadFile(file_path, asset_mapping, groupdict, file_callback)
                self.file_status[file_path] = FileUploadState(UploadState.Success, "Complete")._asdict()
            except HatracJobPaused:
                status = self.getTransferStateStatus(file_path)
                if status:
                    self.file_status[file_path] = FileUploadState(UploadState.Paused, "Paused: %s" % status)._asdict()
                continue
            except HatracJobAborted:
                self.file_status[file_path] = FileUploadState(UploadState.Aborted, "Aborted by user")._asdict()
            except:
                (etype, value, traceback) = sys.exc_info()
                self.file_status[file_path] = FileUploadState(UploadState.Failed, format_exception(value))._asdict()
            self.delTransferState(file_path)
            if status_callback:
                status_callback()

        failed_uploads = dict()
        for key, value in self.file_status.items():
            if value["State"] == UploadState.Failed:
                failed_uploads[key] = value["Status"]

        if self.skipped_files:
            logging.warning("The following file(s) were skipped because they did not satisfy the matching criteria "
                            "of the configuration:\n\n%s\n" % '\n'.join(sorted(self.skipped_files)))

        if failed_uploads:
            logging.warning("The following file(s) failed to upload due to errors:\n\n%s\n" %
                            '\n'.join(["%s -- %s" % (key, failed_uploads[key])
                                       for key in sorted(failed_uploads.keys())]))
            raise RuntimeError("One or more file(s) failed to upload due to errors.")

    def uploadFile(self, file_path, asset_mapping, match_groupdict, callback=None):
        """
        Primary API subclass function.
        :param file_path:
        :param asset_mapping:
        :param match_groupdict:
        :param callback:
        :return:
        """
        logging.info("Processing file: [%s]" % file_path)

        if asset_mapping.get("asset_type", "file") == "table":
            self._uploadTable(file_path, asset_mapping, match_groupdict)
        else:
            self._uploadAsset(file_path, asset_mapping, match_groupdict, callback)

    def _uploadAsset(self, file_path, asset_mapping, match_groupdict, callback=None):

        # 1. Populate metadata by querying the catalog
        self._queryFileMetadata(file_path, asset_mapping, match_groupdict)

        # 2. If "create_record_before_upload" specified in asset_mapping, check for an existing record, creating a new
        #    one if necessary. Otherwise delay this logic until after the file upload.
        record = None
        if stob(asset_mapping.get("create_record_before_upload", False)):
            record = self._getFileRecord(asset_mapping)

        # 3. Perform the Hatrac upload
        self._getFileHatracMetadata(asset_mapping)
        hatrac_options = asset_mapping.get("hatrac_options", {})
        versioned_uri = \
            self._hatracUpload(self.metadata["URI"],
                               file_path,
                               md5=self.metadata.get("md5_base64"),
                               sha256=self.metadata.get("sha256_base64"),
                               content_type=self.guessContentType(file_path),
                               content_disposition=self.metadata.get("content-disposition"),
                               chunked=True,
                               create_parents=stob(hatrac_options.get("create_parents", True)),
                               allow_versioning=stob(hatrac_options.get("allow_versioning", True)),
                               callback=callback)
        logging.debug("Hatrac upload successful. Result object URI: %s" % versioned_uri)
        if stob(hatrac_options.get("versioned_uris", True)):
            self.metadata["URI"] = versioned_uri
        else:
            self.metadata["URI"] = versioned_uri.rsplit(":")[0]
        self.metadata["URI_urlencoded"] = urlquote(self.metadata["URI"])

        # 3. Check for an existing record and create a new one if necessary
        if not record:
            record = self._getFileRecord(asset_mapping)

        # 4. Update an existing record, if necessary
        column_map = asset_mapping.get("column_map", {})
        updated_record = self.interpolateDict(self.metadata, column_map)
        if updated_record != record:
            logging.info("Updating catalog for file [%s]" % self.getFileDisplayName(file_path))
            self._catalogRecordUpdate(self.metadata['target_table'], record, updated_record)

    def _uploadTable(self, file_path, asset_mapping, match_groupdict, callback=None):
        if self.cancelled:
            return None

        self._initFileMetadata(file_path, asset_mapping, match_groupdict)
        try:
            default_columns = asset_mapping.get("default_columns")
            if not default_columns:
                default_columns = self.catalog.getDefaultColumns({}, self.metadata['target_table'])
            default_param = ('?defaults=%s' % ','.join(default_columns)) if len(default_columns) > 0 else ''
            file_ext = self.metadata['file_ext']
            if file_ext == 'csv':
                headers = {'content-type': 'text/csv'}
            elif file_ext == 'json':
                headers = {'content-type': 'application/json'}
            else:
                raise CatalogCreateError("Unsupported file type for catalog bulk upload: %s" % file_ext)
            with open(file_path, "rb") as fp:
                result = self.catalog.post(
                    '/entity/%s%s' % (self.metadata['target_table'], default_param), fp, headers=headers)
                return result
        except:
            (etype, value, traceback) = sys.exc_info()
            raise CatalogCreateError(format_exception(value))

    def _getFileRecord(self, asset_mapping):
        """
        Helper function that queries the catalog to get a record linked to the asset, or create it if it doesn't exist.
        :return: the file record
        """
        column_map = asset_mapping.get("column_map", {})
        rqt = asset_mapping['record_query_template']
        try:
            path = rqt.format(**self.metadata)
        except KeyError as e:
            raise ConfigurationError("Record query template substitution error: %s" % format_exception(e))
        result = self.catalog.get(path).json()
        if result:
            self.metadata.update(result[0])
            return self.pruneDict(result[0], column_map)
        else:
            row = self.interpolateDict(self.metadata, column_map)
            result = self._catalogRecordCreate(self.metadata['target_table'], row)
            if result:
                self.metadata.update(result[0])
            return self.interpolateDict(self.metadata, column_map, allowNone=True)

    def _urlEncodeMetadata(self, safe_overrides=None):
        urlencoded = dict()
        if not safe_overrides:
            safe_overrides = dict()
        for k, v in self.metadata.items():
            if k.endswith("_urlencoded"):
                continue
            urlencoded[k + "_urlencoded"] = urlquote(str(v), safe_overrides.get(k, ""))
        self.metadata.update(urlencoded)

    def _initFileMetadata(self, file_path, asset_mapping, match_groupdict):
        self.metadata.clear()
        self.metadata.update(match_groupdict)

        self.metadata['target_table'] = self.getCatalogTable(asset_mapping, match_groupdict)
        self.metadata["file_name"] = self.getFileDisplayName(file_path)
        self.metadata["file_size"] = self.getFileSize(file_path)

        self._urlEncodeMetadata(asset_mapping.get("url_encoding_safe_overrides"))

    def _queryFileMetadata(self, file_path, asset_mapping, match_groupdict):
        """
        Helper function that queries the catalog to get required metadata for a given file/asset
        """
        file_name = self.getFileDisplayName(file_path)
        logging.info("Computing metadata for file: [%s]." % file_name)
        self._initFileMetadata(file_path, asset_mapping, match_groupdict)

        logging.info("Computing checksums for file: [%s]. Please wait..." % file_name)
        hashes = self.getFileHashes(file_path, asset_mapping.get('checksum_types', ['md5', 'sha256']))
        for alg, checksum in hashes.items():
            alg = alg.lower()
            self.metadata[alg] = checksum[0]
            self.metadata[alg + "_base64"] = checksum[1]

        for uri in asset_mapping.get("metadata_query_templates", []):
            try:
                path = uri.format(**self.metadata)
            except KeyError as e:
                raise RuntimeError("Metadata query template substitution error: %s" % format_exception(e))
            result = self.catalog.get(path).json()
            if result:
                self.metadata.update(result[0])
                self._urlEncodeMetadata(asset_mapping.get("url_encoding_safe_overrides"))
            else:
                raise RuntimeError("Metadata query did not return any results: %s" % path)

        self._getFileExtensionMetadata(self.metadata.get("file_ext"))

        for k, v in asset_mapping.get("column_value_templates", {}).items():
            try:
                self.metadata[k] = v.format(**self.metadata)
            except KeyError as e:
                logging.warning("Column value template substitution error: %s" % format_exception(e))
                continue
        self._urlEncodeMetadata(asset_mapping.get("url_encoding_safe_overrides"))

    def _getFileExtensionMetadata(self, ext):
        ext_map = self.config.get("file_ext_mappings", {})
        entry = ext_map.get(ext)
        if entry:
            self.metadata.update(entry)

    def _getFileHatracMetadata(self, asset_mapping):
        try:
            hatrac_templates = asset_mapping["hatrac_templates"]
            # URI is required
            self.metadata["URI"] = hatrac_templates["hatrac_uri"].format(**self.metadata)
            # overridden content-disposition is optional
            content_disposition = hatrac_templates.get("content-disposition")
            self.metadata["content-disposition"] = \
                None if not content_disposition else content_disposition.format(**self.metadata)
            self._urlEncodeMetadata(asset_mapping.get("url_encoding_safe_overrides"))
        except KeyError as e:
            raise ConfigurationError("Hatrac template substitution error: %s" % format_exception(e))

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
            logging.info("Resuming upload (%s) of file: [%s] to host %s. Please wait..." % (
                self.getTransferStateStatus(file_path), file_path, transfer_state.get("host")))
            path = transfer_state["target"]
            job_id = transfer_state['url'].rsplit("/", 1)[1]
            self.store.put_obj_chunked(path,
                                       file_path,
                                       job_id,
                                       callback=callback,
                                       start_chunk=transfer_state["completed"])
            return self.store.finalize_upload_job(path, job_id)
        else:
            logging.info("Uploading file: [%s] to host %s. Please wait..." % (
                self.getFileDisplayName(file_path), self.server_url))
            return self.store.put_loc(uri,
                                      file_path,
                                      md5=md5,
                                      sha256=sha256,
                                      content_type=content_type,
                                      content_disposition=content_disposition,
                                      chunked=chunked,
                                      create_parents=create_parents,
                                      allow_versioning=allow_versioning,
                                      callback=callback)

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
            missing = self.catalog.validateRowColumns(row, catalog_table)
            if missing:
                raise CatalogCreateError(
                    "Unable to update catalog entry because one or more specified columns do not exist in the "
                    "target table: [%s]" % ','.join(missing))
            if not default_columns:
                default_columns = self.catalog.getDefaultColumns(row, catalog_table)
            default_param = ('?defaults=%s' % ','.join(default_columns)) if len(default_columns) > 0 else ''
            # for default in default_columns:
            #    row[default] = None
            return self.catalog.post('/entity/%s%s' % (catalog_table, default_param), json=[row]).json()
        except:
            (etype, value, traceback) = sys.exc_info()
            raise CatalogCreateError(format_exception(value))

    def _catalogRecordUpdate(self, catalog_table, old_row, new_row):
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
            combined_row = {
                'o%d' % i: old_row[keys[i]]
                for i in range(len(keys))
            }
            combined_row.update({
                'n%d' % i: new_row[keys[i]]
                for i in range(len(keys))
            })
            update_uri = '/attributegroup/%s/%s;%s' % (
                    catalog_table,
                    ','.join(["o%d:=%s" % (i, urlquote(keys[i])) for i in range(len(keys))]),
                    ','.join(["n%d:=%s" % (i, urlquote(keys[i])) for i in range(len(keys))])
            )
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(
                    "Attempting catalog record update [%s] with data values: %s" % (update_uri, [combined_row]))
            return self.catalog.put(update_uri, json=[combined_row]).json()
        except:
            (etype, value, traceback) = sys.exc_info()
            raise CatalogUpdateError(format_exception(value))

    def defaultFileCallback(self,  **kwargs):
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
            # logging.debug(status)
            pass
        if self.cancelled:
            return -1

        return True

    def loadTransferState(self):
        transfer_state_file_path = self.getDeployedTransferStateFilePath()
        transfer_state_dir = os.path.dirname(transfer_state_file_path)
        try:
            if not os.path.isdir(transfer_state_dir):
                try:
                    os.makedirs(transfer_state_dir)
                except OSError as error:
                    if error.errno != errno.EEXIST:
                        raise

            if not os.path.isfile(transfer_state_file_path):
                with open(transfer_state_file_path, "w") as tsfp:
                    json.dump(self.transfer_state, tsfp)

            self.transfer_state_fp = \
                open(transfer_state_file_path, 'r+')
            self.transfer_state = json.load(self.transfer_state_fp, object_pairs_hook=OrderedDict)
        except Exception as e:
            logging.warning("Unable to read transfer state file, transfer checkpointing will not be available. "
                            "Error: %s" % format_exception(e))

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
        if not self.transfer_state_fp:
            return
        try:
            self.transfer_state_fp.seek(0, 0)
            self.transfer_state_fp.truncate()
            json.dump(self.transfer_state, self.transfer_state_fp, indent=2)
            self.transfer_state_fp.flush()
        except Exception as e:
            logging.warning("Unable to write transfer state file: %s" % format_exception(e))

    def cleanupTransferState(self):
        if self.transfer_state_fp and not self.transfer_state_fp.closed:
            try:
                self.transfer_state_fp.flush()
                self.transfer_state_fp.close()
            except Exception as e:
                logging.warning("Unable to flush/close transfer state file: %s" % format_exception(e))

    def getTransferStateStatus(self, file_path):
        transfer_state = self.getTransferState(file_path)
        if transfer_state:
            return "%d%% complete" % (
                round(((float(transfer_state["completed"]) / float(transfer_state["total"])) % 100) * 100))
        return None


class GenericUploader(DerivaUpload):

    def __init__(self, config_file=None, credential_file=None, server=None):
        DerivaUpload.__init__(self, config_file, credential_file, server)

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
