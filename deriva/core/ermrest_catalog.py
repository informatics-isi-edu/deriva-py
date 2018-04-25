import logging
import datetime

from . import urlquote, datapath, DEFAULT_HEADERS, DEFAULT_CHUNK_SIZE, Megabyte, get_transfer_summary
from .deriva_binding import DerivaBinding
from .ermrest_config import CatalogConfig
from . import ermrest_model


class ErmrestCatalogMutationError(Exception):
    pass


class ErmrestCatalog(DerivaBinding):
    """Persistent handle for an ERMrest catalog.

       Provides basic REST client for HTTP methods on arbitrary
       paths. Caller has to understand ERMrest APIs and compose
       appropriate paths, headers, and/or content.

       Additional utility methods provided for accessing catalog metadata.
    """
    table_schemas = dict()

    def __init__(self, scheme, server, catalog_id, credentials=None, caching=True, session_config=None):
        """Create ERMrest catalog binding.

           Arguments:
             scheme: 'http' or 'https'
             server: server FQDN string
             catalog_id: e.g. '1'
             credentials: credential secrets, e.g. cookie
             caching: whether to retain a GET response cache

        """
        super(ErmrestCatalog, self).__init__(scheme, server, credentials, caching, session_config)
        self._server_uri = "%s/ermrest/catalog/%s" % (
            self._server_uri,
            catalog_id
        )
        self._scheme, self._server, self._catalog_id, self._credentials, self._caching, self._session_config = \
            scheme, server, catalog_id, credentials, caching, session_config

    def latest_snapshot(self):
        """Gets a handle to this catalog's latest snapshot.
        """
        r = self.get('/')
        r.raise_for_status()
        return ErmrestSnapshot(self._scheme, self._server, self._catalog_id, r.json()['snaptime'],
                               self._credentials, self._caching, self._session_config)

    def getCatalogConfig(self):
        return CatalogConfig.fromcatalog(self)

    def getCatalogModel(self):
        return ermrest_model.Model.fromcatalog(self)

    def applyCatalogConfig(self, config):
        return config.apply(self)

    def getCatalogSchema(self):
        path = '/schema'
        r = self.get(path)
        r.raise_for_status()
        return r.json()

    def getPathBuilder(self):
        """Returns the 'path builder' interface for this catalog."""
        return datapath.from_catalog(self)

    def getTableSchema(self, fq_table_name):
        # first try to get from cache(s)
        s, t = self.splitQualifiedCatalogName(fq_table_name)
        cat = self.getCatalogSchema()
        schema = cat['schemas'][s]['tables'][t] if cat else None
        if schema:
            return schema
        schema = self.table_schemas.get(fq_table_name)
        if schema:
            return schema

        path = '/schema/%s/table/%s' % (s, t)
        r = self.get(path)
        resp = r.json()
        self.table_schemas[fq_table_name] = resp
        r.raise_for_status()

        return resp

    def getTableColumns(self, fq_table_name):
        columns = set()
        schema = self.getTableSchema(fq_table_name)
        for column in schema['column_definitions']:
            columns.add(column['name'])

        return columns

    def validateRowColumns(self, row, fq_tableName):
        columns = self.getTableColumns(fq_tableName)
        return set(row.keys()) - columns

    def getDefaultColumns(self, row, table, exclude=None, quote_url=True):
        columns = self.getTableColumns(table)
        if isinstance(exclude, list):
            for col in exclude:
                columns.remove(col)

        defaults = []
        supplied_columns = row.keys()
        for col in columns:
            if col not in supplied_columns:
                defaults.append(urlquote(col, safe='') if quote_url else col)

        return defaults

    @staticmethod
    def splitQualifiedCatalogName(name):
        entity = name.split(':')
        if len(entity) != 2:
            logging.debug("Unable to tokenize %s into a fully qualified <schema:table> name." % name)
            return None
        return entity[0], entity[1]

    def getAsFile(self, path, destfilename, headers=DEFAULT_HEADERS, callback=None):
        """
           Retrieve catalog data streamed to destination file.
           Caller is responsible to clean up file even on error, when the file may or may not be exist.

        """
        self.check_path(path)

        headers = headers.copy()

        destfile = open(destfilename, 'w+b')

        try:
            r = self._session.get(self._server_uri + path, headers=headers, stream=True)
            r.raise_for_status()

            total = 0
            start = datetime.datetime.now()
            logging.debug("Transferring file %s to %s" % (self._server_uri + path, destfilename))
            for buf in r.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                destfile.write(buf)
                total += len(buf)
                if callback:
                    if not callback(progress="Downloading: %.2f MB transferred" % (float(total) / float(Megabyte))):
                        destfile.close()
                        return None
            elapsed = datetime.datetime.now() - start
            summary = get_transfer_summary(total, elapsed)
            logging.info("File [%s] transfer successful. %s" % (destfilename, summary))
            if callback:
                callback(summary=summary, file_path=destfilename)

            return r
        finally:
            destfile.close()


class ErmrestSnapshot(ErmrestCatalog):
    """Persistent handle for an ERMrest catalog snapshot.

    Inherits from ErmrestCatalog and provides the same interfaces,
    except that the interfaces are now bound to a fixed snapshot
    of the catalog.
    """
    def __init__(self, scheme, server, catalog_id, snaptime, credentials=None, caching=True, session_config=None):
        """Create ERMrest catalog snapshot binding.

           Arguments:
             scheme: 'http' or 'https'
             server: server FQDN string
             catalog_id: e.g., '1'
             snaptime: e.g., '2PM-DGYP-56Z4'
             credentials: credential secrets, e.g. cookie
             caching: whether to retain a GET response cache
        """
        super(ErmrestSnapshot, self).__init__(scheme, server, catalog_id, credentials, caching, session_config)
        self._server_uri = "%s@%s" % (
            self._server_uri,
            snaptime
        )

    def _pre_mutate(self, path, headers, guard_response=None):
        """Override and disable mutation operations.

        When called by the super-class, this method raises an exception.
        """
        raise ErmrestCatalogMutationError('Catalog snapshot is immutable')
