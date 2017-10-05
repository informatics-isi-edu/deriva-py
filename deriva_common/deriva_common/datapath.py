from . import urlquote
from datetime import date
import logging
import re

try:
    from collections.abc import Mapping
    _MappingBaseClass = Mapping
except:
    _MappingBaseClass = object

logger = logging.getLogger(__name__)


def from_catalog(catalog):
    """Creates a PathBuilder object from a derivapy ERMrest catalog.
    :param catalog: an ERMrest catalog object
    """
    return PathBuilder(catalog, catalog.getCatalogSchema())


def _isidentifier(a):
    """Tests if string is a valid python identifier.
    This function is intended for internal usage within this module.
    :param a a string
    """
    assert isinstance(a, str)
    if hasattr(a, 'isidentifier'):
        return a.isidentifier()
    else:
        return re.match("[_A-Za-z][_a-zA-Z0-9]*$", a) is not None


class _LazyDict (_MappingBaseClass):
    """A lazy dictionary object that acts like a Mapping object.
    This class is intended for internal usage within this module.
    """
    def __init__(self, new_elem_fn, keys=list()):
        """Initializes the lazy dict.
        :param new_elem_fn: a function that takes an 'item' key and returns a new element
        :param keys: the list of keys expected to be valid
        """
        self._new_elem_fn = new_elem_fn
        self._keys = set(keys)
        self._storage = {}

    def __getitem__(self, item):
        # Uses the 'new_elem_fn' function to create a new element when the item
        # is not already in the storage dictionary.
        if item not in self._storage:
            self._storage[item] = self._new_elem_fn(item)
            self._keys.update([item])
        return self._storage[item]

    def __iter__(self):
        return iter(self._keys)

    def __len__(self):
        return len(self._keys)

    def _ipython_key_completions_(self):
        return self.keys()

    def keys(self):
        return list(self._keys)


class PathBuilder (object):
    """PathBuilder is the main container object and beginning interface for this module.
    """
    def __init__(self, catalog, doc):
        """Initializes the PathBuilder.
        :param catalog: an ermrest catalog instance
        :param doc: the schema document for the catalog
        """
        schemas_doc = doc.get('schemas', {})
        keys = schemas_doc.keys()
        self.schemas = _LazyDict(lambda a: Schema(catalog, a, schemas_doc[a]), keys)
        self._identifiers = dir(PathBuilder) + ['schemas'] + [
            key for key in keys if _isidentifier(key)
        ]

    def __dir__(self):
        return self._identifiers

    def __getattr__(self, a):
        return self.schemas[a]


class Schema (object):
    """Represents a schema.
    """
    def __init__(self, catalog, name, doc):
        """Initializes the Schema.
        :param catalog: the catalog instance
        :param name: the schema's name
        :param doc: the schema document
        """
        self._catalog = catalog
        self._name = name
        tables_doc = doc.get('tables', {})
        self.table_names = tables_doc.keys()
        self.tables = _LazyDict(lambda a: Table(self, a, tables_doc[a]), self.table_names)
        self._identifiers = dir(Schema) + ['tables'] + [
            table_name for table_name in self.table_names if _isidentifier(table_name)
        ]

    def __dir__(self):
        return self._identifiers

    def __getattr__(self, a):
        return self.tables[a]

    def __repr__(self):
        s = "Schema name: '%s'\nList of tables:\n" % self._name
        if len(self.table_names) == 0:
            s += "none"
        else:
            s += "\n".join("  '%s'" % tname for tname in self.table_names)
        return s

    @property
    def name(self):
        """the url encoded name"""
        return urlquote(self._name)

    @property
    def fqname(self):
        """the url encoded fully qualified name"""
        return self.name


class DataPath (object):
    """Represents an arbitrary data path."""
    def __init__(self, path_expression):
        assert isinstance(path_expression, PathNode)
        self._path_expression = path_expression

    @property
    def uri(self):
        catalog = self._path_expression._catalog
        return catalog._server_uri + str(self._path_expression)

    def filter(self, filter_expression):
        """Filters the path based on the specified formula.
        :param filter_expression: should be a valid Predicate object
        """
        assert isinstance(filter_expression, Predicate)
        return DataPath(Filter(self._path_expression, filter_expression))

    def link(self, on, as_=None, join_type=''):
        """Links this path with another table.
        At present, the implementation only supports single column keys.
        :param on: may be a table or an equality comparison between keys and foreign keys
        :param as_: an optional table alias to assign this table instance to
        :param join_type: the join type of this link which may be 'left', 'right', 'full' outer joins or '' for inner join link by default.
        """
        return DataPath(Link(self._path_expression, on, as_, join_type))

    def context_reset(self, alias):
        """Resets path context to aliased table.
        The table alias must have been defined earlier within this path expression in order for it to be valid.
        :param alias: the table alias that this path context will be reset to
        """
        assert isinstance(alias, TableAlias)
        return DataPath(ContextReset(self._path_expression, alias))

    def attributes(self, *args):
        """Projects the columns of the path based on the specified list of columns.
        :param args: a list of Columns.
        """
        return DataPath(Projection(self._path_expression, args))

    def entities(self, limit=None):
        """Returns the entity set computed by this data path.
        :param limit: an optional limit on the size of the entity set.
        """
        assert limit is None or isinstance(limit, int)
        opts = '?limit=%d' % limit if limit else ''
        path = str(self._path_expression) + opts
        catalog = self._path_expression._catalog

        def fetcher():
            logger.debug(path)
            resp = catalog.get(path)
            resp.raise_for_status()
            return resp.json()

        return EntitySet(fetcher)


class EntitySet (object):
    """Represents an entity set.
    Data paths return results as sets of entities.
    """
    def __init__(self, fetcher_fn):
        """Initializes the EntitySet.
        :param fetcher_fn: a function that fetches the data
        """
        assert fetcher_fn is not None
        self._fetcher_fn = fetcher_fn
        self._results_doc = None
        self._dataframe = None

    @property
    def dataframe(self):
        """Pandas DataFrame representation of this path."""
        if self._dataframe is None:
            from pandas import DataFrame
            self._dataframe = DataFrame(self._fetch())
        return self._dataframe

    def __len__(self):
        return len(self._fetch())

    def __getitem__(self, item):
        return self._fetch()[item]

    def __iter__(self):
        return iter(self._fetch())

    def _fetch(self):
        if self._results_doc is None:
            self._results_doc = self._fetcher_fn()
        return self._results_doc


class Table (DataPath):
    """Represents a table.
    """
    def __init__(self, schema, name, doc):
        """Initializes the table.
        :param schema: the schema object to which this table belongs
        :param name: the name of the table
        :param doc: the table definition doc
        """
        super(Table, self).__init__(Root(self))
        assert isinstance(schema, Schema)
        self._schema = schema
        self._name = name
        self._doc = doc
        self.columns = {}
        self._identifiers = dir(Table) + ['columns']
        for cdoc in doc.get('column_definitions', {}):
            column_name = cdoc['name']
            self.columns[column_name] = Column(self, column_name, cdoc)
            if _isidentifier(column_name):
                self._identifiers.append(column_name)

    def __dir__(self):
        return self._identifiers

    def __repr__(self):
        s = "Table name: '%s'\nList of columns:\n" % self._name
        if len(self.columns) == 0:
            s += "none"
        else:
            s += "\n".join("  %s" % repr(col) for col in self.columns.values())
        return s

    @property
    def name(self):
        """the url encoded name"""
        return urlquote(self._name)

    @property
    def fqname(self):
        """the url encoded fully qualified name"""
        return "%s:%s" % (self._schema.name, self.name)

    @property
    def instancename(self):
        return ''

    @property
    def fromname(self):
        return self.fqname

    def __getattr__(self, a):
        return self.columns[a]

    def as_(self, alias):
        """Returns a table alias object.
        :param alias: a string to use as the alias name
        """
        return TableAlias(self, alias)


class TableAlias (Table):
    """Represents a table alias.
    """
    def __init__(self, table, alias):
        """Initializes the table alias.
        :param table: the base table to be given an alias name
        :param alias: the alias name
        """
        assert isinstance(table, Table)
        super(TableAlias, self).__init__(table._schema, table._name, table._doc)
        self._table = table
        self._alias = alias

    @property
    def name(self):
        """the url encoded name"""
        return urlquote(self._alias)

    @property
    def fqname(self):
        """the url encoded fully qualified name"""
        return self._table.fqname

    @property
    def instancename(self):
        return self.name

    @property
    def fromname(self):
        return "%s:=%s" % (self.name, self._table.fqname)


class Column (object):
    """Represents a column in a table.
    """
    def __init__(self, table, name, doc):
        """Initializes a column.
        :param table: the table to which this column belongs
        :param name: the name of the column
        :param doc: the column definition document
        """
        assert isinstance(table, Table)
        self._table = table
        self._name = name
        self._doc = doc

    def __repr__(self):
        return "Column name: '%s'\tType: %s\tComment: '%s'" % \
               (self._name, self._doc['type']['typename'], self._doc['comment'])

    @property
    def name(self):
        """the url encoded name"""
        return urlquote(self._name)

    @property
    def fqname(self):
        """the url encoded fully qualified name"""
        return "%s:%s" % (self._table.fqname, self.name)

    @property
    def instancename(self):
        table_instancename = self._table.instancename
        if len(table_instancename) > 0:
            return "%s:%s" % (table_instancename, self.name)
        else:
            return self.name

    def __str__(self):
        return self._name

    def __eq__(self, other):
        return BinaryPredicate(self, "=", other)

    def __lt__(self, other):
        return BinaryPredicate(self, "::lt::", other)

    def __le__(self, other):
        return BinaryPredicate(self, "::leq::", other)

    def __gt__(self, other):
        return BinaryPredicate(self, "::gt::", other)

    def __ge__(self, other):
        return BinaryPredicate(self, "::geq::", other)


class PathNode (object):
    def __init__(self, r):
        assert isinstance(r, PathNode) or isinstance(r, DataPath)
        if isinstance(r, Projection):
            raise Exception("This path cannot be extended")
        self._r = r

    @property
    def _path(self):
        return self._r._path

    @property
    def _mode(self):
        return self._r._mode

    @property
    def _catalog(self):
        return self._r._catalog

    def __str__(self):
        return "/%s/%s" % (self._mode, self._path)


class Root (PathNode):
    def __init__(self, r):
        super(Root, self).__init__(r)
        assert isinstance(r, Table)
        self._table = r

    @property
    def _path(self):
        return self._table.fromname

    @property
    def _mode(self):
        return 'entity'

    @property
    def _catalog(self):
        return self._table._schema._catalog


class ContextReset (PathNode):
    def __init__(self, r, alias):
        super(ContextReset, self).__init__(r)
        assert isinstance(alias, TableAlias)
        self._alias = alias

    @property
    def _path(self):
        return "%s/$%s" % (self._r._path, self._alias.name)


class Filter(PathNode):
    def __init__(self, r, formula):
        super(Filter, self).__init__(r)
        assert isinstance(formula, BinaryPredicate) or isinstance(formula, JunctionPredicate)
        self._formula = formula

    @property
    def _path(self):
        return "%s/%s" % (self._r._path, str(self._formula))


class Projection (PathNode):
    def __init__(self, r, attrs):
        super(Projection, self).__init__(r)
        assert isinstance(attrs, tuple)
        assert len(attrs) > 0
        self._attrs = []
        if len(attrs) == 1 and isinstance(attrs[0], TableAlias):
            self.__mode = 'entity'
            self._attrs.append("$%s" % attrs[0].name)
        else:
            self.__mode = 'attribute'
            for attr in attrs:
                if isinstance(attr, Table):
                    iname = attr.instancename
                    if len(iname) > 0:
                        self._attrs.append(iname + ':*')
                    else:
                        self._attrs.append('*')
                else:
                    self._attrs.append(attr.instancename)

    @property
    def _path(self):
        return "%s/%s" % (self._r._path, ','.join(attr for attr in self._attrs))

    @property
    def _mode(self):
        return self.__mode


class Link (PathNode):
    def __init__(self, r, on, as_=None, join_type=''):
        super(Link, self).__init__(r)
        assert isinstance(on, BinaryPredicate) or isinstance(on, Table)
        assert as_ is None or isinstance(as_, TableAlias)
        assert join_type == '' or (join_type in ('left', 'right', 'full') and isinstance(on, BinaryPredicate))
        # TODO: we should test that parent path and `on` belong to the same catalog
        self._on = on
        self._as = as_
        self._join_type = join_type

    @property
    def _path(self):
        assign = '' if self._as is None else "%s:=" % self._as.name
        cond = self._on.fqname if isinstance(self._on, Table) else str(self._on)
        return "%s/%s%s%s" % (self._r._path, assign, self._join_type, cond)


class Predicate (object):
    def __init__(self):
        pass


class BinaryPredicate (Predicate):
    def __init__(self, lop, op, rop):
        super(BinaryPredicate, self).__init__()
        assert isinstance(lop, Column)
        assert isinstance(rop, Column) or isinstance(rop, int) or \
            isinstance(rop, float) or isinstance(rop, str) or \
            isinstance(rop, date)
        assert isinstance(op, str)
        self._lop = lop
        self._op = op
        self._rop = rop

    def __str__(self):
        if isinstance(self._rop, Column):
            # The only valid circumstance for a Column rop is in a link 'on' predicate
            # TODO: ultimately, this should be a Column Set equality comparison
            return "(%s)=(%s)" % (self._lop.instancename, self._rop.fqname)
        else:
            return "%s%s%s" % (self._lop.instancename, self._op, urlquote(str(self._rop)))

    def __and__(self, other):
        return JunctionPredicate(self, "&", other)

    def __or__(self, other):
        return JunctionPredicate(self, ";", other)


class JunctionPredicate (Predicate):
    def __init__(self, left, op, right):
        super(JunctionPredicate, self).__init__()
        assert isinstance(left, Predicate)
        assert isinstance(right, Predicate)
        assert isinstance(op, str)
        self._left = left
        self._op = op
        self._right = right

    def __str__(self):
        # TODO: not sure this is 100% correct. Barely thought about it so far.
        return "(%s)%s(%s)" % (self._left, self._op, self._right)

    def __and__(self, other):
        return JunctionPredicate(self, "&", other)

    def __or__(self, other):
        return JunctionPredicate(self, ";", other)
