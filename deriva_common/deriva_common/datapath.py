from . import urlquote
from datetime import date
import logging
import re
from requests import HTTPError

try:
    from collections.abc import Mapping as _MappingBaseClass
except ImportError:
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
        self.tables = _LazyDict(lambda a: Table(catalog, self._name, a, tables_doc[a].get('column_definitions', {})),
                                self.table_names)
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
    def catalog(self):
        return self._catalog

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
    def __init__(self, root):
        assert isinstance(root, TableAlias)
        self._path_expression = Root(root)
        self._root = root
        self.nodes = dict()  # map of alias_name => TableAlias object
        self._context = None
        self._identifiers = dir(DataPath) + ['nodes']
        self._bind_table_instance(root)

    def __dir__(self):
        return self._identifiers

    def __getattr__(self, a):
        return self.nodes[a]

    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, value):
        assert isinstance(value, TableAlias)
        assert value.name in self.nodes
        self._path_expression = ResetContext(self._path_expression, value)
        self._context = value

    @property
    def uri(self):
        return self._root.catalog._server_uri + str(self._path_expression)

    def _bind_table_instance(self, table):
        """Binds a new table into this path.
        """
        assert isinstance(table, TableAlias)
        table_name = table.name
        table.path = self
        self.nodes[table_name] = self._context = table
        if _isidentifier(table_name):
            self._identifiers.append(table_name)

    def filter(self, filter_expression):
        """Filters the path based on the specified formula.
        :param filter_expression: should be a valid Predicate object
        :returns self
        """
        assert isinstance(filter_expression, Predicate)
        self._path_expression = Filter(self._path_expression, filter_expression)
        return self

    def link(self, right, on=None, join_type=''):
        """Links this path with another table.
        At present, the implementation only supports single column keys.
        :param right: the right hand table of the link expression
        :param on: an equality comparison between keys and foreign keys
        :param join_type: the join type of this link which may be 'left', 'right', 'full' outer joins or '' for inner
        join link by default.
        :returns self
        """
        assert isinstance(right, Table)
        assert on is None or isinstance(on, BinaryPredicate)
        assert join_type == '' or on is not None

        if right.catalog != self._root.catalog:
            raise Exception("Cannot link across catalogs.")

        if isinstance(right, TableAlias):
            # Validate that alias has not been used
            if right.name in self.nodes:
                raise Exception("Table instance is already linked. Consider renaming it.")
        else:
            # Generate an unused alias name for the table
            table_name = right.name
            alias = table_name
            counter = 1
            while alias in self.nodes:
                counter += 1
                alias = table_name + str(counter)
            right = right.as_(alias)

        if on is None:
            on = right

        # Extend path expression
        self._path_expression = Link(self._path_expression, on, right, join_type)

        # Bind alias and this data path
        self._bind_table_instance(right)

        return self

    def entities(self, *attributes, **renamed_attributes):
        """Returns the entity set computed by this data path.
        Optionally, caller may specify the attributes to be included in the entity set. The attributes may be from the
        current context of the path or from a linked table instance. Columns may be renamed in the output and will
        take the name of the keyword parameter used. If no attributes are specified, the entity set will contain whole
        entities of the type of the path's context.
        :param attributes: a list of Columns.
        :param renamed_attributes: a list of renamed Columns.
        :returns an entity set
        """
        catalog = self._root.catalog
        if attributes or renamed_attributes:
            base_path = str(Project(self._path_expression, attributes, renamed_attributes))
        else:
            base_path = str(self._path_expression)

        def fetcher(limit=None):
            assert limit is None or isinstance(limit, int)
            opts = '?limit=%d' % limit if limit else ''
            path = base_path + opts
            logger.debug("Fetching " + path)
            try:
                resp = catalog.get(path)
                return resp.json()
            except HTTPError as e:
                logger.error(e.response.text)
                raise e

        return EntitySet(fetcher)


class EntitySet (object):
    """A set of entities.
    The EntitySet is produced by a path. The results may be explicitly fetched. The EntitySet behaves like a
    container. If the EntitySet has not been fetched explicitly, on first use of container operations, it will
    be implicitly fetched from the catalog.
    """
    def __init__(self, fetcher_fn):
        """Initializes the EntitySet.
        :param fetcher_fn: a function that fetches the entities from the catalog.
        """
        assert fetcher_fn is not None
        self._fetcher_fn = fetcher_fn
        self._results_doc = None
        self._dataframe = None

    @property
    def _results(self):
        if self._results_doc is None:
            self.fetch()
        return self._results_doc

    @property
    def dataframe(self):
        """Pandas DataFrame representation of this path."""
        if self._dataframe is None:
            from pandas import DataFrame
            self._dataframe = DataFrame(self._results)
        return self._dataframe

    def __len__(self):
        return len(self._results)

    def __getitem__(self, item):
        return self._results[item]

    def __iter__(self):
        return iter(self._results)

    def fetch(self, limit=None):
        """Fetches the entities from the catalog.
        :param limit: maximum number of entities to fetch from the catalog.
        :returns self
        """
        limit = int(limit) if limit else None
        self._results_doc = self._fetcher_fn(limit)
        self._dataframe = None  # clear potentially cached state
        logger.debug("Fetched %d entities" % len(self._results_doc))
        return self

class Table (object):
    """Represents a table.
    """
    def __init__(self, catalog, schema_name, table_name, columns_doc):
        """Initializes the table.
        :param catalog: the catalog object
        :param schema_name: name of the schema
        :param table_name: name of the table
        :param columns_doc: deserialized json columns document from the table definition
        """
        self._catalog = catalog
        self._schema_name = schema_name
        self._name = table_name
        self._doc = columns_doc

        self.columns = {}
        self._identifiers = dir(Table) + ['columns']
        for cdoc in self._doc:
            column_name = cdoc['name']
            self.columns[column_name] = Column(self, column_name, cdoc)
            if _isidentifier(column_name):
                self._identifiers.append(column_name)

    def __dir__(self):
        return self._identifiers

    def __getattr__(self, a):
        return self.columns[a]

    def __repr__(self):
        s = "Table name: '%s'\nList of columns:\n" % self._name
        if len(self.columns) == 0:
            s += "none"
        else:
            s += "\n".join("  %s" % repr(col) for col in self.columns.values())
        return s

    @property
    def catalog(self):
        return self._catalog

    @property
    def name(self):
        """the url encoded name"""
        return urlquote(self._name)

    @property
    def fqname(self):
        """the url encoded fully qualified name"""
        return "%s:%s" % (self._schema_name, self.name)

    @property
    def instancename(self):
        return ''

    @property
    def fromname(self):
        return self.fqname

    @property
    def path(self):
        """Always a new DataPath instance that is rooted at this table.
        Note that this table will be automatically aliased using its own table name.
        """
        return DataPath(self.as_(self.name))

    @path.setter
    def path(self, value):
        """Not allowed on base tables.
        """
        raise Exception("Path assignment not allowed on base table objects.")

    @property
    def uri(self):
        return self.path.uri

    def as_(self, alias):
        """Returns a table alias object.
        :param alias: a string to use as the alias name
        """
        return TableAlias(self, alias)

    def filter(self, filter_expression):
        return self.path.filter(filter_expression)

    def link(self, right, on=None, join_type=''):
        return self.path.link(right, on, join_type)

    def entities(self, *attributes, **renamed_attributes):
        return self.path.entities(*attributes, **renamed_attributes)


class TableAlias (Table):
    """Represents a table alias.
    """
    def __init__(self, table, alias):
        """Initializes the table alias.
        :param table: the base table to be given an alias name
        :param alias: the alias name
        """
        assert isinstance(table, Table)
        super(TableAlias, self).__init__(table._catalog, table._schema_name, table._name, table._doc)
        self._table = table
        self._alias = alias
        self._parent = None

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

    @property
    def path(self):
        """Returns the parent path for this alias and conditionally resets the context to this table instance.
        """
        if not self._parent:
            self._parent = DataPath(self)
        elif self._parent.context != self:
            self._parent.context = self
        return self._parent

    @path.setter
    def path(self, value):
        if self._parent:
            raise Exception("Cannot bind a table instance that has already been bound.")
        elif not isinstance(value, DataPath):
            raise Exception("value must be a DataPath instance.")
        self._parent = value


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


class PathOperator (object):
    def __init__(self, r):
        assert isinstance(r, PathOperator) or isinstance(r, Table)
        if isinstance(r, Project):
            raise Exception("Cannot extend a path after an attribute projection")
        self._r = r

    @property
    def _path(self):
        assert isinstance(self._r, PathOperator)
        return self._r._path

    @property
    def _mode(self):
        assert isinstance(self._r, PathOperator)
        return self._r._mode

    def __str__(self):
        return "/%s/%s" % (self._mode, self._path)


class Root (PathOperator):
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


class ResetContext (PathOperator):
    def __init__(self, r, alias):
        if isinstance(r, ResetContext):
            r = r._r  # discard the previous context reset operator
        super(ResetContext, self).__init__(r)
        assert isinstance(alias, TableAlias)
        self._alias = alias

    @property
    def _path(self):
        assert isinstance(self._r, PathOperator)
        return "%s/$%s" % (self._r._path, self._alias.name)


class Filter(PathOperator):
    def __init__(self, r, formula):
        super(Filter, self).__init__(r)
        assert isinstance(formula, BinaryPredicate) or isinstance(formula, JunctionPredicate)
        self._formula = formula

    @property
    def _path(self):
        assert isinstance(self._r, PathOperator)
        return "%s/%s" % (self._r._path, str(self._formula))


class Project (PathOperator):
    def __init__(self, r, attributes, renamed_attributes):
        super(Project, self).__init__(r)
        assert len(attributes) > 0 or len(renamed_attributes) > 0
        self._attrs = []

        # Build up the list of project attributes
        for attr in attributes:
            if isinstance(attr, Table):
                iname = attr.instancename
                if len(iname) > 0:
                    self._attrs.append(iname + ':*')
                else:
                    self._attrs.append('*')
            else:
                self._attrs.append(attr.instancename)

        # Extend the list with renamed attributes (i.e., "out alias" named)
        for new_name in renamed_attributes:
            attr = renamed_attributes[new_name]
            self._attrs.append("%s:=%s" % (new_name, attr.instancename))

    @property
    def _path(self):
        assert isinstance(self._r, PathOperator)
        return "%s/%s" % (self._r._path, ','.join(attr for attr in self._attrs))

    @property
    def _mode(self):
        return 'attribute'


class Link (PathOperator):
    def __init__(self, r, on, as_=None, join_type=''):
        super(Link, self).__init__(r)
        assert isinstance(on, BinaryPredicate) or isinstance(on, Table)
        assert as_ is None or isinstance(as_, TableAlias)
        assert join_type == '' or (join_type in ('left', 'right', 'full') and isinstance(on, BinaryPredicate))
        self._on = on
        self._as = as_
        self._join_type = join_type

    @property
    def _path(self):
        assert isinstance(self._r, PathOperator)
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
        return "(%s)%s(%s)" % (self._left, self._op, self._right)

    def __and__(self, other):
        return JunctionPredicate(self, "&", other)

    def __or__(self, other):
        return JunctionPredicate(self, ";", other)
