import json
import os
import re
import sys
import shutil
import tempfile
import logging
from collections import OrderedDict
from json import JSONDecodeError
from deriva_common import ErmrestCatalog, HatracStore, HatracJobAborted, HatracJobPaused, format_exception, urlquote, \
    read_config, resource_path
from deriva_common.utils import hash_utils as hu, mime_utils as mu

try:
    from os import scandir, walk
except ImportError:
    from scandir import scandir, walk


class CatalogCreateError (Exception):
    pass


class CatalogUpdateError (Exception):
    pass


FileUploadState = {"S": "Success", "F": "Failure", "P": "Pending", "R": "Retry", "C": "Cancelled"}


class DerivaUpload(object):
    """
    Base class for upload tasks. Encapsulates a catalog instance and a hatrac store instance and provides some common
    and reusable functions.

    This class is not intended to be instantiated directly, but rather extended by a deployment specific implementation.
    """
    catalog = None
    store = None
    asset_mappings = None
    remote_config_path = None
    transfer_state = None
    transfer_state_fp = None
    cancelled = False

    DefaultConfigFileName = "config.json"
    DefaultTransferStateFileName = "transfers.json"

    def __init__(self, config=None, credentials=None):
        self.file_list = dict()
        self.file_status = dict()
        self.skipped_files = set()
        self.config = config
        self.credentials = credentials

        self.configure()

    def __del__(self):
        if self.transfer_state_fp:
            self.transfer_state_fp.flush()
            self.transfer_state_fp.close()

    def configure(self):
        self.cleanup()
        server = self.config['server']
        protocol = server.get('protocol', 'https')
        host = server.get('host', '')
        self.remote_config_path = server.get('config_path')
        catalog_id = self.config.get('catalog_id', '1')
        session_config = self.config.get('session')
        self.asset_mappings = self.config.get('asset_mappings', [])
        mu.add_types(self.config.get('mime_overrides'))

        if self.catalog:
            del self.catalog
        self.catalog = ErmrestCatalog(protocol, host, catalog_id, self.credentials, session_config=session_config)
        if self.store:
            del self.store
        self.store = HatracStore(protocol, host, self.credentials, session_config=session_config)

        self.loadTransferState()

    def cancel(self):
        self.cancelled = True

    def cleanup(self):
        self.file_list.clear()
        self.file_status.clear()
        self.skipped_files.clear()
        self.cancelled = False

    def setCredentials(self, credentials):
        server = self.config['server']['host']
        self.credentials = credentials
        self.catalog.set_credentials(self.credentials, server)
        self.store.set_credentials(self.credentials, server)

    @staticmethod
    def getInstance(config=None, credentials=None):
        """
        This method must be implemented by subclasses.
        """
        raise NotImplementedError("This method must be implemented by a subclass.")

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
    def getCatalogTable(asset_mapping):
        schema_name, table_name = asset_mapping['base_record_type']
        return '%s:%s' % (urlquote(schema_name), urlquote(table_name))

    @staticmethod
    def processTemplates(src, dst, allowNone=False):
        dst = dst.copy()
        # prune None values from the src, we don't want those to be replaced with the string 'None' in the dest
        empty = [k for k, v in src.items() if v == None]
        for k in empty:
            del src[k]
        # perform the string replacement for the values in the destination dict
        for k, v in dst.items():
            try:
                value = v % src
            except KeyError:
                value = v
                if value:
                    if value.startswith('%('):
                        value = None
            dst.update({k: value})
        # remove all None valued entries in the dest, if disallowed
        if not allowNone:
            empty = [k for k, v in dst.items() if v == None]
            for k in empty:
                del dst[k]

        return dst

    def getDefaultConfigFilePath(self):
        return os.path.normpath(resource_path(os.path.join("conf", DerivaUpload.DefaultConfigFileName)))

    def getDeployedConfigFilePath(self):
        """
        This method must be implemented by subclasses.
        """
        raise NotImplementedError("This method must be implemented by a subclass.")

    def getDeployedTransferStateFilePath(self):
        """
        This method must be implemented by subclasses.
        """
        raise NotImplementedError("This method must be implemented by a subclass.")

    def getUpdatedConfig(self):
        if not self.remote_config_path:
            logging.debug(
                "Unable to check for updated configuration file -- remote configuration path not specified.")
            return
        logging.info("Checking for updated configuration file...")
        if not self.store.content_equals(self.remote_config_path, self.getDeployedConfigFilePath()):
            logging.info("Retrieving updated configuration file...")
            with tempfile.TemporaryDirectory() as tempdir:
                updated_config_path = os.path.abspath(os.path.join(tempdir, DerivaUpload.DefaultConfigFileName))
                self.store.get_obj(self.remote_config_path,
                                   destfilename=updated_config_path)
                # an extra sanity check here
                if self.store.content_equals(self.remote_config_path, updated_config_path):
                    shutil.copy2(updated_config_path, self.getDeployedConfigFilePath())
                else:
                    logging.error("Downloaded configuration file does not match checksum of current server version. "
                                  "Falling back to old version.")
                    return
            logging.info("Applying updated configuration file...")
            self.config = read_config(self.getDeployedConfigFilePath())
            self.configure()
        else:
            logging.info("Configuration file is up-to-date.")

    def getFileStatusAsArray(self):
        result = list()
        for key in sorted(self.file_status.keys()):
            item = {"File": key}
            item.update(self.file_status[key])
            result.append(item)
        return result

    def getFileDisplayName(self, file_path, asset_mapping=None):
        return os.path.basename(file_path)

    def validateFile(self, root, path, name):
        file_path = os.path.normpath(os.path.join(path, name))
        asset_mapping, groupdict = self.getAssetMapping(file_path)
        if not asset_mapping:
            return None

        return {file_path: (asset_mapping, groupdict)}

    def uploadFile(self, file_path, asset_mapping, match_groupdict, callback=None):
        """
        This method must be implemented by subclasses.
        """
        raise NotImplementedError("This method must be implemented by a subclass.")

    def uploadFiles(self, status_callback=None, file_callback=None):
        for file_path, (asset_mapping, groupdict) in self.file_list.items():
            if self.cancelled:
                self.file_status[file_path] = {"State": FileUploadState["C"], "Status": "Cancelled by user."}
                continue
            try:
                self.uploadFile(file_path, asset_mapping, groupdict, file_callback)
                self.file_status[file_path] = {"State": FileUploadState["S"], "Status": "Transfer complete."}
            except HatracJobPaused:
                self.file_status[file_path] = {"State": FileUploadState["R"], "Status": "Paused by user."}
                continue
            except HatracJobAborted:
                self.file_status[file_path] = {"State": FileUploadState["C"], "Status": "Cancelled by user."}
            except:
                (etype, value, traceback) = sys.exc_info()
                self.file_status[file_path] = {"State": FileUploadState["F"], "Status": format_exception(value)}
            self.delTransferState(file_path)

        failed_uploads = dict()
        for key, value in self.file_status.items():
            if value["State"] == FileUploadState["F"]:
                failed_uploads[key] = value["Status"]

        if self.skipped_files:
            logging.warning("The following file(s) were skipped because they did not satisfy the matching criteria "
                            "of the configuration:\n\n%s\n" % '\n'.join(sorted(self.skipped_files)))

        if failed_uploads:
            logging.warning("The following file(s) failed to upload due to errors:\n\n%s\n" %
                            '\n'.join(["%s -- %s" % (key, failed_uploads[key])
                                       for key in sorted(failed_uploads.keys())]))
            raise RuntimeError("One or more file(s) failed to upload due to errors.")

    def scanDirectory(self, root, abort_on_invalid_input=False):
        """

        :param root:
        :param abort_on_invalid_input:
        :return:
        """
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
                    transfer_state = self.getTransferState(file_path)
                    if transfer_state:
                        status = "%d%% complete" % (
                            round(((transfer_state["completed"] / transfer_state["total"]) % 100) * 100))
                        self.file_status[file_path] = {"State": FileUploadState["R"], "Status": status}
                    else:
                        self.file_status[file_path] = {"State": FileUploadState["P"], "Status": "Transfer Pending"}

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

        server = self.config['server']['protocol'] + "://" + self.config['server']['host']

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
            logging.info("Resuming upload of file: [%s] to host %s. Please wait..." % (
                file_path, transfer_state.get("host")))
            path = transfer_state["target"]
            job_id = transfer_state['url'].rsplit("/", 1)[1]
            self.store.put_obj_chunked(path,
                                       file_path,
                                       job_id,
                                       callback=callback,
                                       start_chunk=transfer_state["completed"])
            self.store.finalize_upload_job(path, job_id)
        else:
            logging.info("Uploading file: [%s] to host %s. Please wait..." % (
                self.getFileDisplayName(file_path), server))
            self.store.put_loc(uri,
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
            assert keys == sorted(list(old_row.keys()))
            combined_row = {
                'o%d' % i: old_row[keys[i]]
                for i in range(len(keys))
            }
            combined_row.update({
                'n%d' % i: new_row[keys[i]]
                for i in range(len(keys))
            })
            return self.catalog.put(
                '/attributegroup/%s/%s;%s' % (
                    catalog_table,
                    ','.join(["o%d:=%s" % (i, urlquote(keys[i])) for i in range(len(keys))]),
                    ','.join(["n%d:=%s" % (i, urlquote(keys[i])) for i in range(len(keys))])
                ),
                json=[combined_row]
            ).json()
        except:
            (etype, value, traceback) = sys.exc_info()
            raise CatalogUpdateError(format_exception(value))

    def loadTransferState(self):
        self.transfer_state_fp = \
            os.fdopen(os.open(self.getDeployedTransferStateFilePath(), os.O_RDWR | os.O_CREAT), 'r+')
        try:
            self.transfer_state = json.load(self.transfer_state_fp, object_pairs_hook=OrderedDict)
        except JSONDecodeError as e:
            logging.debug("Unable to read transfer state: %s" % format_exception(e))
            self.transfer_state = dict()

    def getTransferState(self, file_path):
        return self.transfer_state.get(file_path)

    def setTransferState(self, file_path, transfer_state):
        self.transfer_state[file_path] = transfer_state
        self.transfer_state_fp.seek(0, 0)
        self.transfer_state_fp.truncate()
        json.dump(self.transfer_state, self.transfer_state_fp, indent=2)
        self.transfer_state_fp.flush()

    def delTransferState(self, file_path):
        transfer_state = self.transfer_state.get(file_path)
        if transfer_state:
            del self.transfer_state[file_path]
        self.transfer_state_fp.seek(0, 0)
        self.transfer_state_fp.truncate()
        json.dump(self.transfer_state, self.transfer_state_fp, indent=2)
        self.transfer_state_fp.flush()
