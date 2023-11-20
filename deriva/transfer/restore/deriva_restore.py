import io
import os
import sys
import copy
import json
import time
import logging
import datetime
import platform
from collections import OrderedDict
from bdbag import bdbag_api as bdb
from deriva.core import (get_credential, format_credential, urlquote, format_exception, read_config,
                         DEFAULT_SESSION_CONFIG, __version__ as VERSION)
from deriva.core.utils.version_utils import get_installed_version
from deriva.core.ermrest_model import Model
from deriva.core.deriva_server import DerivaServer
from deriva.core.ermrest_catalog import ErmrestCatalog, _clone_state_url as CLONE_STATE_URL
from deriva.core.hatrac_store import HatracStore
from deriva.transfer import DerivaUpload, DerivaUploadError, DerivaUploadConfigurationError, GenericUploader
from deriva.transfer.restore import DerivaRestoreError, DerivaRestoreConfigurationError, \
    DerivaRestoreAuthenticationError, DerivaRestoreAuthorizationError


class DerivaRestore:
    """
    Restore a DERIVA catalog from a bag archive or directory.
    Core restore logic re-purposed from ErmrestCatalog.clone_catalog().
    """

    RESTORE_STATE_URL = "tag:isrd.isi.edu,2019:restore-status"
    BASE_DATA_INPUT_PATH = os.path.join("records", "{}", "{}.json")
    BASE_ASSETS_INPUT_PATH = "assets"

    def __init__(self, *args, **kwargs):

        self.server_args = args[0]
        self.hostname = None
        self.dst_catalog = None
        self.cancelled = False
        self.input_path = kwargs.get("input_path")
        self.exclude_schemas = kwargs.get("exclude_schemas", list())
        self.exclude_data = kwargs.get("exclude_data", list())
        self.restore_data = not kwargs.get("no_data", False)
        self.data_chunk_size = kwargs.get("data_chunk_size", 10000)
        self.restore_annotations = not kwargs.get("no_annotations", False)
        self.restore_policy = not kwargs.get("no_policy", False)
        self.restore_assets = not kwargs.get("no_assets", False)
        self.strict_bag_validation = not kwargs.get("weak_bag_validation", True)
        self.no_bag_materialize = kwargs.get("no_bag_materialize", False)
        self.upload_config = kwargs.get("asset_config")
        self.truncate_after = True
        self.envars = kwargs.get("envars", dict())
        self.config = kwargs.get("config")
        self.credentials = kwargs.get("credentials", dict())
        config_file = kwargs.get("config_file")
        credential_file = kwargs.get("credential_file")

        info = "%s v%s [Python %s, %s]" % (
            self.__class__.__name__, get_installed_version(VERSION),
            platform.python_version(), platform.platform(aliased=True))
        logging.info("Initializing: %s" % info)

        if not self.server_args:
            raise DerivaRestoreConfigurationError("Target server not specified!")

        # server variable initialization
        self.hostname = self.server_args.get('host', '')
        if not self.hostname:
            raise DerivaRestoreConfigurationError("Host not specified!")
        protocol = self.server_args.get('protocol', 'https')
        self.server_url = protocol + "://" + self.hostname
        self.catalog_id = self.server_args.get("catalog_id",)
        self.session_config = self.server_args.get('session', DEFAULT_SESSION_CONFIG.copy())
        self.session_config["allow_retry_on_all_methods"] = True

        # credential initialization
        token = kwargs.get("token")
        oauth2_token = kwargs.get("oauth2_token")
        username = kwargs.get("username")
        password = kwargs.get("password")
        if token or oauth2_token or (username and password):
            self.credentials = format_credential(token=token,
                                                 oauth2_token=oauth2_token,
                                                 username=username,
                                                 password=password)
        else:
            self.credentials = get_credential(self.hostname, credential_file)

        # destination catalog initialization
        self.server = DerivaServer(protocol,
                                   self.hostname,
                                   self.credentials,
                                   caching=True,
                                   session_config=self.session_config)
        self.server.dcctx["cid"] = kwargs.get("dcctx_cid", "api/" + self.__class__.__name__)
        # process config file
        if config_file:
            try:
                self.config = read_config(config_file)
            except Exception as e:
                raise DerivaRestoreConfigurationError(e)

    def set_config(self, config):
        self.config = config

    def set_credentials(self, credentials):
        self.dst_catalog.set_credentials(credentials, self.hostname)
        self.credentials = credentials

    def prune_parts(self, dest, *extra_victims):
        victims = set(extra_victims)
        if not self.restore_annotations and 'annotations' in dest:
            victims |= {'annotations', }
        if not self.restore_policy:
            victims |= {'acls', 'acl_bindings'}
        for k in victims:
            dest.pop(k, None)
        return dest

    def copy_sdef(self, schema):
        """Copy schema definition structure with conditional parts for cloning."""
        dest = self.prune_parts(schema.prejson(), 'tables')
        return dest

    def copy_tdef_core(self, table):
        """Copy table definition structure with conditional parts excluding fkeys."""
        dest = self.prune_parts(table.prejson(), 'foreign_keys')
        dest['column_definitions'] = [self.prune_parts(column) for column in dest['column_definitions']]
        dest['keys'] = [self.prune_parts(column) for column in dest.get('keys', [])]
        dest.setdefault('annotations', {})[self.RESTORE_STATE_URL] = 1 if self.restore_data else None
        return dest

    def copy_tdef_fkeys(self, table):
        """Copy table fkeys structure."""

        def check(fkdef):
            for fkc in fkdef['referenced_columns']:
                if fkc['schema_name'] == 'public' \
                        and fkc['table_name'] in {'ERMrest_Client', 'ERMrest_Group', 'ERMrest_RID_Lease'} \
                        and fkc['column_name'] == 'RID':
                    raise DerivaRestoreError(
                        "Cannot restore catalog with foreign key reference to "
                        "%(schema_name)s:%(table_name)s:%(column_name)s" % fkc)
            return fkdef

        return [self.prune_parts(check(dest)) for dest in table.prejson().get('foreign_keys', [])]

    def copy_cdef(self, column):
        """Copy column definition with conditional parts."""
        return column.table.schema.name, column.table.name, self.prune_parts(column.prejson())

    @staticmethod
    def check_column_compatibility(src, dst):
        """Check compatibility of source and destination column definitions."""

        def error(fieldname, sv, dv):
            return DerivaRestoreError("Source/dest column %s mismatch %s != %s for %s:%s:%s" % (
                fieldname,
                sv, dv,
                src.table.schema.name, src.table.name, src.name
            ))

        if src.type.typename != dst.type.typename:
            raise error("type", src.type.typename, dst.type.typename)
        if src.nullok != dst.nullok:
            raise error("nullok", src.nullok, dst.nullok)
        if src.default != dst.default:
            raise error("default", src.default, dst.default)

    def copy_kdef(self, key):
        return key.table.schema.name, key.table.name, self.prune_parts(key.prejson())

    def get_table_path(self, sname, tname, is_bag):
        return os.path.abspath(
            os.path.join(self.input_path, "data" if is_bag else "",
                         self.BASE_DATA_INPUT_PATH.format(urlquote(sname), urlquote(tname))))

    def load_json_file(self, file_path):
        with io.open(file_path, 'r', encoding='UTF-8') as file_data:
            return json.load(file_data, object_pairs_hook=OrderedDict)

    def open_json_stream_file(self, table_path):
        """
        Open a JSON-Stream file for reading, caller is responsible for closing.
        """
        table_data = io.open(table_path, 'r', encoding='UTF-8')
        line = table_data.readline().strip()
        table_data.seek(0)
        if line.startswith('{') and line.endswith('}'):
            return table_data
        else:
            table_data.close()
            raise DerivaRestoreError(
                "Input file %s does not appear to be in the required json-stream format." % table_path)

    def get_json_recordset(self, data, chunk_size, after=None, after_column='RID'):
        chunk = list()
        found = False
        for line in data:
            if isinstance(line, dict):
                row = line
            else:
                row = json.loads(line, object_pairs_hook=OrderedDict)
            if after and not found:
                if after == row[after_column]:
                    found = True
                continue
            chunk.append(row)
            if len(chunk) == chunk_size:
                yield chunk
                chunk = list()
        if chunk:
            yield chunk

    def restore(self, **kwargs):
        """
        Perform the catalog restore operation. The restore process is broken up into six phases:

        1. Pre-process the input path.
            - If the input path is a file, it is assumed that it is a compressed archive file that can be extracted
            into an input directory via a supported codec: `tar`,`tgz`,`bz2`, or `zip`.
            - If the input directory is a valid _bag_ directory structure, the bag will be materialized.
        2. The catalog schema will be restored first. The schema is restored from a ERMRest JSON schema document file.
            The schema document file must be named `catalog-schema.json` and must appear at the root of the input
            directory. The restore process can be configured to exclude the restoration of an enumerated set both
            schema and tables.
        3. The catalog table data will be restored, if present. The table date restoration process is resilient to
            interruption and may be restarted. However, if the catalog schema or data is mutated outside of the scope of
            the restore function in-between such restarts, the restored catalog's consistency cannot be guaranteed.
            The restore process can be configured to exclude the restoration of table data for a set of tables.
        4. The catalog foreign keys will be restored.
        5. The catalog assets will be restored, if present.
        6. On success, the restore state marker annotations will be deleted and the catalog history will be truncated.

        :param kwargs:
        :return:
        """
        success = True
        start = datetime.datetime.now()

        # pre-process input
        logging.info("Processing input path: %s" % self.input_path)
        is_file, is_dir, is_uri = bdb.inspect_path(self.input_path)
        if not (is_file or is_dir or is_uri):
            raise DerivaRestoreError("Invalid input path [%s]. If the specified input path refers to a locally mounted "
                                     "file or directory, it does not exist or cannot be accessed. If the specified "
                                     "path is a URI, the scheme component of the URI could not be determined." %
                                     self.input_path)
        if is_file or is_dir:
            self.input_path = os.path.abspath(self.input_path)
        if is_file:
            logging.info("The input path [%s] is a file. Assuming input file is a directory archive and extracting..." %
                         self.input_path)
            self.input_path = bdb.extract_bag(self.input_path)

        try:
            if not self.no_bag_materialize:
                self.input_path = bdb.materialize(self.input_path)
        except bdb.bdbagit.BagValidationError as e:
            if self.strict_bag_validation:
                raise DerivaRestoreError(format_exception(e))
            else:
                logging.warning("Input bag validation failed and strict validation mode is disabled. %s" %
                                format_exception(e))
        is_bag = bdb.is_bag(self.input_path)

        src_schema_file = os.path.abspath(
            os.path.join(self.input_path, "data" if is_bag else "", "catalog-schema.json"))
        # the src_catalog_stub created below will never be "connected" in any kind of network sense,
        # but we need an instance of ErmrestCatalog in order to get a working Model from the schema file.
        src_catalog_stub = ErmrestCatalog("file", src_schema_file, "1")
        src_model = Model.fromfile(src_catalog_stub, src_schema_file)

        # initialize/connect to destination catalog
        if not self.catalog_id:
            self.catalog_id = self.server.create_ermrest_catalog().catalog_id
            self.server_args["catalog_id"] = self.catalog_id
            logging.info("Created new target catalog with ID: %s" % self.catalog_id)
        self.dst_catalog = self.server.connect_ermrest(self.catalog_id)

        # init dcctx cid to a default
        self.dst_catalog.dcctx['cid'] = self.__class__.__name__

        # build up the model content we will copy to destination
        dst_model = self.dst_catalog.getCatalogModel()

        logging.info("Restoring %s to catalog: %s" % (self.input_path, self.dst_catalog.get_server_uri()))
        # set top-level config right away and find fatal usage errors...
        if self.restore_policy:
            logging.info("Restoring top-level catalog ACLs...")
            if not src_model.acls:
                logging.info("Source schema does not contain any ACLs.")
            else:
                src_model.acls.owner.extend(dst_model.acls.owner)
                self.dst_catalog.put('/acl', json=src_model.acls)

        if self.restore_annotations:
            logging.info("Restoring top-level catalog annotations...")
            self.dst_catalog.put('/annotation', json=src_model.annotations)

        # build up the model content we will copy to destination
        dst_model = self.dst_catalog.getCatalogModel()

        new_model = []
        new_columns = []  # ERMrest does not currently allow bulk column creation
        new_keys = []  # ERMrest does not currently allow bulk key creation
        restore_states = {}
        fkeys_deferred = {}
        exclude_schemas = [] if self.exclude_schemas is None else self.exclude_schemas
        exclude_data = [] if self.exclude_data is None else self.exclude_data

        try:
            for sname, schema in src_model.schemas.items():
                if sname in exclude_schemas:
                    continue
                if sname not in dst_model.schemas:
                    new_model.append(self.copy_sdef(schema))

                for tname, table in schema.tables.items():
                    if table.kind != 'table':
                        logging.warning('Skipping restore of %s %s:%s' % (table.kind, sname, tname))
                        continue

                    if 'RID' not in table.column_definitions.elements:
                        raise DerivaRestoreError(
                            "Source table %s.%s lacks system-columns and cannot be restored." % (sname, tname))

                    # make sure the source table is pruned of any existing restore state markers
                    if table.annotations.get(CLONE_STATE_URL) is not None:
                        del table.annotations[CLONE_STATE_URL]
                    if table.annotations.get(self.RESTORE_STATE_URL) is not None:
                        del table.annotations[self.RESTORE_STATE_URL]

                    if sname not in dst_model.schemas or tname not in dst_model.schemas[sname].tables:
                        new_model.append(self.copy_tdef_core(table))
                        restore_states[(sname, tname)] = 1 if self.restore_data else None
                        fkeys_deferred[(sname, tname)] = self.copy_tdef_fkeys(table)
                    else:
                        if dst_model.schemas[sname].tables[tname].foreign_keys:
                            # assume presence of any destination foreign keys means we already loaded deferred_fkeys
                            self.restore_data = False
                        else:
                            fkeys_deferred[(sname, tname)] = self.copy_tdef_fkeys(table)

                        src_columns = {c.name: c for c in table.column_definitions}
                        dst_columns = {c.name: c for c in dst_model.schemas[sname].tables[tname].column_definitions}

                        for cname in src_columns:
                            if cname not in dst_columns:
                                new_columns.append(self.copy_cdef(src_columns[cname]))
                            else:
                                self.check_column_compatibility(src_columns[cname], dst_columns[cname])

                        for cname in dst_columns:
                            if cname not in src_columns:
                                raise DerivaRestoreError(
                                    "Destination column %s.%s.%s does not exist in source catalog." %
                                    (sname, tname, cname))

                        src_keys = {tuple(sorted(c.name for c in key.unique_columns)): key for key in table.keys}
                        dst_keys = {tuple(sorted(c.name for c in key.unique_columns)): key for key in
                                    dst_model.schemas[sname].tables[tname].keys}

                        for utuple in src_keys:
                            if utuple not in dst_keys:
                                new_keys.append(self.copy_kdef(src_keys[utuple]))

                        for utuple in dst_keys:
                            if utuple not in src_keys:
                                raise DerivaRestoreError("Destination key %s.%s(%s) does not exist in source catalog."
                                                         % (sname, tname, ', '.join(utuple)))

                        restore_states[(sname, tname)] = \
                            dst_model.schemas[sname].tables[tname].annotations.get(self.RESTORE_STATE_URL)

            restore_states[('public', 'ERMrest_RID_Lease')] = None  # never try to sync leases

            # apply the stage 1 model to the destination in bulk
            logging.info("Restoring catalog schema...")
            if new_model:
                self.dst_catalog.post("/schema", json=new_model).raise_for_status()

            for sname, tname, cdef in new_columns:
                self.dst_catalog.post("/schema/%s/table/%s/column" % (urlquote(sname), urlquote(tname)),
                                      json=cdef).raise_for_status()

            for sname, tname, kdef in new_keys:
                self.dst_catalog.post("/schema/%s/table/%s/key" % (urlquote(sname), urlquote(tname)),
                                      json=kdef).raise_for_status()

            # copy data in stage 2
            if self.restore_data:
                logging.info("Restoring catalog data...")
                for sname, tname in restore_states.keys():
                    object_name = "%s:%s" % (sname, tname)
                    if object_name in exclude_data:
                        continue
                    tname_uri = "%s:%s" % (urlquote(sname), urlquote(tname))
                    if restore_states[(sname, tname)] == 1:
                        # determine current position in (partial?) copy
                        row = self.dst_catalog.get("/entity/%s@sort(RID::desc::)?limit=1" % tname_uri).json()
                        if row:
                            last = row[0]['RID']
                            logging.info("Existing data detected in table [%s] -- will attempt partial restore of "
                                         "remaining records following last known RID: %s" % (tname_uri, last))
                        else:
                            last = None

                        table_path = self.get_table_path(sname, tname, is_bag)
                        if not os.path.isfile(table_path):
                            logging.warning("Restoration of table data [%s] incomplete. File not found: %s" %
                                            (("%s:%s" % (sname, tname)), table_path))
                            continue
                        table = self.get_json_recordset(self.open_json_stream_file(table_path),
                                                        self.data_chunk_size, after=last)

                        total = 0
                        table_success = True
                        try:
                            for chunk in table:
                                if chunk:
                                    self.dst_catalog.post("/entity/%s?nondefaults=RID,RCT,RCB" % tname_uri, json=chunk)
                                    total += len(chunk)
                                else:
                                    break
                        except Exception as e:
                            table_success = False
                            logging.error(format_exception(e))
                        finally:
                            table.close()
                            if table_success:
                                logging.info("Restoration of table data [%s] successful. %s rows restored." %
                                             (tname_uri, total))
                            else:
                                success = False
                                logging.warning("Restoration of table data [%s] failed. %s rows restored." %
                                                (tname_uri, total))

                        # record our progress on catalog in case we fail part way through
                        self.dst_catalog.put(
                            "/schema/%s/table/%s/annotation/%s" % (
                                urlquote(sname),
                                urlquote(tname),
                                urlquote(self.RESTORE_STATE_URL),
                            ),
                            json=2
                        )
                    elif restore_states[(sname, tname)] is None and (sname, tname) in {
                        ('public', 'ERMrest_Client'),
                        ('public', 'ERMrest_Group'),
                    }:
                        # special sync behavior for magic ermrest tables
                        # HACK: these are assumed small enough to join via local merge of arrays
                        page = self.load_json_file(self.get_table_path(sname, tname, is_bag))
                        self.dst_catalog.post("/entity/%s?onconflict=skip" % tname_uri, json=page)

                        # record our progress on catalog in case we fail part way through
                        self.dst_catalog.put(
                            "/schema/%s/table/%s/annotation/%s" % (
                                urlquote(sname),
                                urlquote(tname),
                                urlquote(self.RESTORE_STATE_URL),
                            ),
                            json=2
                        )

            # apply stage 2 model in bulk only... we won't get here unless preceding succeeded
            logging.info("Restoring foreign keys...")
            new_fkeys = []
            for fkeys in fkeys_deferred.values():
                new_fkeys.extend(fkeys)

            if new_fkeys:
                self.dst_catalog.post("/schema", json=new_fkeys)

            # copy over configuration in stage 3
            # we need to do this after deferred_fkeys to handle acl_bindings projections with joins
            logging.info("Restoring catalog configuration...")
            dst_model = self.dst_catalog.getCatalogModel()

            for sname, src_schema in src_model.schemas.items():
                if sname in exclude_schemas:
                    continue
                dst_schema = dst_model.schemas[sname]

                if self.restore_annotations:
                    dst_schema.annotations.clear()
                    dst_schema.annotations.update(src_schema.annotations)

                if self.restore_policy:
                    dst_schema.acls.clear()
                    dst_schema.acls.update(src_schema.acls)

                for tname, src_table in src_schema.tables.items():
                    dst_table = dst_schema.tables[tname]

                    if self.restore_annotations:
                        merged = dict(src_table.annotations)
                        if self.RESTORE_STATE_URL in dst_table.annotations:
                            merged[self.RESTORE_STATE_URL] = dst_table.annotations[self.RESTORE_STATE_URL]
                        dst_table.annotations.clear()
                        dst_table.annotations.update(merged)

                    if self.restore_policy:
                        dst_table.acls.clear()
                        dst_table.acls.update(src_table.acls)
                        dst_table.acl_bindings.clear()
                        dst_table.acl_bindings.update(src_table.acl_bindings)

                    for cname, src_col in src_table.columns.elements.items():
                        dst_col = dst_table.columns[cname]

                        if self.restore_annotations:
                            dst_col.annotations.clear()
                            dst_col.annotations.update(src_col.annotations)

                        if self.restore_policy:
                            dst_col.acls.clear()
                            dst_col.acls.update(src_col.acls)
                            dst_col.acl_bindings.clear()
                            dst_col.acl_bindings.update(src_col.acl_bindings)

                    for src_key in src_table.keys:
                        dst_key = dst_table.key_by_columns([col.name for col in src_key.unique_columns])

                        if self.restore_annotations:
                            dst_key.annotations.clear()
                            dst_key.annotations.update(src_key.annotations)

                    def xlate_column_map(fkey):
                        dst_from_table = dst_table
                        dst_to_schema = dst_model.schemas[fkey.pk_table.schema.name]
                        dst_to_table = dst_to_schema.tables[fkey.pk_table.name]
                        return {
                            dst_from_table._own_column(from_col.name): dst_to_table._own_column(to_col.name)
                            for from_col, to_col in fkey.column_map.items()
                        }

                    for src_fkey in src_table.foreign_keys:
                        dst_fkey = dst_table.fkey_by_column_map(xlate_column_map(src_fkey))

                        if self.restore_annotations:
                            dst_fkey.annotations.clear()
                            dst_fkey.annotations.update(src_fkey.annotations)

                        if self.restore_policy:
                            dst_fkey.acls.clear()
                            dst_fkey.acls.update(src_fkey.acls)
                            dst_fkey.acl_bindings.clear()
                            dst_fkey.acl_bindings.update(src_fkey.acl_bindings)

            # send all the config changes to the server
            dst_model.apply()

            # restore assets
            if self.restore_assets:
                self.upload_assets()

            # cleanup
            if success:
                self.cleanup_restored_catalog()
        except:
            success = False
            raise
        finally:
            elapsed_time = datetime.datetime.now() - start
            total_secs = elapsed_time.total_seconds()
            elapsed = time.strftime('%H:%M:%S', time.gmtime(total_secs))
            logging.info("Restore of catalog %s %s. %s" % (self.dst_catalog.get_server_uri(),
                                                           "completed successfully" if success else "failed",
                                                           ("Elapsed time: %s" % elapsed) if (total_secs > 0) else ""))

    def cleanup_restored_catalog(self):
        # cleanup restore state markers
        logging.info("Cleaning up restore state...")
        dst_model = self.dst_catalog.getCatalogModel()
        for sname, schema in dst_model.schemas.items():
            for tname, table in schema.tables.items():
                if sname == "public" and tname == "ERMrest_RID_Lease":
                    continue
                annotation_uri = "/schema/%s/table/%s/annotation/%s" % (
                    urlquote(sname),
                    urlquote(tname),
                    urlquote(self.RESTORE_STATE_URL)
                )
                try:
                    self.dst_catalog.delete(annotation_uri)
                except Exception as e:
                    logging.warning("Unable to cleanup restore state marker annotation %s: %s" %
                                    (annotation_uri, format_exception(e)))
                    continue

        # truncate restore history
        if self.truncate_after:
            logging.info("Truncating restore history...")
            snaptime = self.dst_catalog.get("/").json()["snaptime"]
            self.dst_catalog.delete("/history/,%s" % urlquote(snaptime))

    def upload_assets(self):
        asset_dir = os.path.join(self.input_path, self.BASE_ASSETS_INPUT_PATH)
        if not os.path.isdir(asset_dir):
            logging.debug("No asset directory found. Will not attempt to upload file assets.")
            return

        logging.info("Restoring file assets...")
        uploader = GenericUploader(config_file=self.upload_config, server=self.server_args)
        uploader.setCredentials(self.credentials)
        uploader.setConfig(self.upload_config)
        uploader.scanDirectory(asset_dir, abort_on_invalid_input=False, purge_state=False)
        uploader.uploadFiles(file_callback=uploader.defaultFileCallback)
        uploader.cleanup()
