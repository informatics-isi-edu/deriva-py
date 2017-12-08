import logging

from . import urlquote, datapath
from .deriva_binding import DerivaBinding
from .ermrest_config import CatalogConfig
from . import ermrest_model

class ErmrestCatalog(DerivaBinding):
    """Persistent handle for an ERMrest catalog.

       Provides basic REST client for HTTP methods on arbitrary
       paths. Caller has to understand ERMrest APIs and compose
       appropriate paths, headers, and/or content.

       Additional utility methods provided for accessing catalog metadata.
    """
    catalog_schema = None
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
        DerivaBinding.__init__(self, scheme, server, credentials, caching, session_config)
        self._server_uri = "%s/ermrest/catalog/%s" % (
            self._server_uri,
            catalog_id
        )

    def getCatalogConfig(self):
        return CatalogConfig.fromcatalog(self)

    def get_catalog_model(self):
        return ermrest_model.Model.fromcatalog(self)

    def applyCatalogConfig(self, config):
        return config.apply(self)

    def getCatalogSchema(self):
        if self.catalog_schema:
            return self.catalog_schema

        path = '/schema'
        r = self.get(path)
        resp = r.json()
        self.catalog_schema = resp
        r.raise_for_status()

        return resp

    def getPathBuilder(self):
        """Returns the 'path builder' interface for this catalog."""
        return datapath.from_catalog(self)

    def getTableSchema(self, fq_table_name):
        # first try to get from cache(s)
        s, t = self.splitQualifiedCatalogName(fq_table_name)
        cat = self.catalog_schema
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

