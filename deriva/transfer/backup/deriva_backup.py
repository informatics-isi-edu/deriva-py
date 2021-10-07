import os
import sys
import copy
import time
import logging
import datetime
from deriva.core import get_credential, urlquote
from deriva.transfer import DerivaDownload
from deriva.transfer.backup import DerivaBackupError, DerivaBackupConfigurationError, \
    DerivaBackupAuthenticationError, DerivaBackupAuthorizationError


class DerivaBackup(DerivaDownload):
    BASE_CONFIG = {
        "catalog": {
            "query_processors": []
        }
    }
    BASE_SCHEMA_QUERY_PROC = {
        "processor": "json",
        "processor_params": {
            "query_path": "/schema",
            "output_path": "catalog-schema"
        }
    }
    BASE_DATA_QUERY_PATH = "/entity/{}:{}"
    BASE_DATA_OUTPUT_PATH = "records/{}/{}"
    BASE_ASSET_OUTPUT_PATH = "assets"

    def __init__(self, *args, **kwargs):
        DerivaDownload.__init__(self, *args, **kwargs)

        self.config_file = kwargs.get("config_file")
        self.annotation_config = None

        if not self.config:
            self.config = copy.deepcopy(self.BASE_CONFIG)

            no_schema = kwargs.get("no_schema", False)
            if not no_schema:
                self.config["catalog"]["query_processors"].append(self.BASE_SCHEMA_QUERY_PROC)

            no_bag = kwargs.get("no_bag", False)
            if not no_bag:
                bag = dict()
                bag["bag_name"] = os.path.basename(self.output_dir)
                bag["bag_archiver"] = kwargs.get("bag_archiver", "tgz")
                bag["bag_algorithms"] = ["sha256", "md5"]
                self.config["bag"] = bag

            # if credentials have not been explicitly set yet, try to get them from the default credential store
            if not self.credentials:
                self.set_credentials(get_credential(self.hostname))

            logging.debug("Inspecting catalog model...")
            model = self.catalog.getCatalogModel()
            # if we dont have catalog ownership rights, its a hard error for now
            if not model.acls:
                raise DerivaBackupAuthorizationError("Only catalog owners may perform full catalog dumps.")

            if kwargs.get("no_data", False):
                return

            exclude = kwargs.get("exclude_data", list())
            for sname, schema in model.schemas.items():
                if sname in exclude:
                    logging.info("Excluding data dump from all tables in schema: %s" % sname)
                    continue
                for tname, table in schema.tables.items():
                    fqtname = "%s:%s" % (sname, tname)
                    if table.kind != "table":
                        logging.warning("Skipping data dump of %s: %s" % (table.kind, fqtname))
                        continue
                    if fqtname in exclude:
                        logging.info("Excluding data dump from table: %s" % fqtname)
                        continue
                    if "RID" not in table.column_definitions.elements:
                        logging.warning(
                            "Source table %s.%s lacks system-columns and will not be dumped." % (sname, tname))
                        continue

                    # Configure table data download query processors
                    data_format = "json" if (sname, tname) in {
                        ('public', 'ERMrest_Client'),
                        ('public', 'ERMrest_Group'),
                    } else "json-stream"
                    q_sname = urlquote(sname)
                    q_tname = urlquote(tname)
                    output_path = self.BASE_DATA_OUTPUT_PATH.format(q_sname, q_tname)
                    query_path = self.BASE_DATA_QUERY_PATH.format(q_sname, q_tname)
                    query_proc = dict()
                    query_proc["processor"] = data_format
                    query_proc_params = {"query_path": query_path, "output_path": output_path}
                    if data_format in ("json-stream", "csv"):
                        query_proc_params.update({"paged_query": True, "paged_query_size": 100000})
                    query_proc["processor_params"] = query_proc_params
                    self.config["catalog"]["query_processors"].append(query_proc)

        self.generate_asset_configs()

    def generate_asset_configs(self):
        # TODO: Generate asset data download query processor configuration entries
        pass

    def download(self, **kwargs):
        logging.info("Backing up catalog: %s" % self.catalog.get_server_uri())
        success = True
        start = datetime.datetime.now()
        try:
            return super(DerivaBackup, self).download(**kwargs)
        except:
            success = False
            raise
        finally:
            elapsed_time = datetime.datetime.now() - start
            total_secs = elapsed_time.total_seconds()
            elapsed = time.strftime('%H:%M:%S', time.gmtime(total_secs))
            logging.info("Backup of catalog %s %s. %s" % (self.catalog.get_server_uri(),
                                                          "completed successfully" if success else "failed",
                                                          ("Elapsed time: %s" % elapsed) if (total_secs > 0) else ""))
