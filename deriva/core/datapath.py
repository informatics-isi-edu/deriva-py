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


def _kwargs(**kwargs):
    """Helper for extending ermrest_model with sub-types for the whole model tree."""
    kwargs2 = {
        'schema_class': Schema,
        'table_class': Table,
        'column_class': Column
    }
    kwargs2.update(kwargs)
    return kwargs2


def from_catalog(catalog):
    """Creates a Catalog object from a derivapy ERMrest catalog.
    :param catalog: an ERMrest catalog object
    """
    return Catalog(catalog.getCatalogSchema(), **_kwargs(catalog=catalog))


def _isidentifier(a):
    """Tests if string is a valid python identifier.
    This function is intended for internal usage within this module.
    :param a: a string
    """
    assert isinstance(a, str)
    if hasattr(a, 'isidentifier'):
        return a.isidentifier()
    else:
        return re.match("[_A-Za-z][_a-zA-Z0-9]*$", a) is not None


def _http_error_message(e):
    """Returns a formatted error message from the raw HTTPError.
    """
    return '\n'.join(e.response.text.splitlines()[1:]) + '\n' + str(e)


class DataPathException (Exception):
    """DataPath exception
    """
    def __init__(self, message, reason=None):
        super(DataPathException, self).__init__(message, reason)
        assert isinstance(message, str)
        self.message = message
        self.reason = reason

    def __str__(self):
        return self.message


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


class Catalog (object):
    """Handle to a Catalog.
    """
    def __init__(self, model_doc, **kwargs):
        """Creates the Catalog.
        :param model_doc: the schema document for the catalog
        """
        super(Catalog, self).__init__()
        self.schemas = {
            sname: kwargs.get('schema_class', Schema)(sname, sdoc, **kwargs)
            for sname, sdoc in model_doc.get('schemas', {}).items()
        }

    def __dir__(self):
        return list(super(Catalog, self).__dir__()) + [key for key in self.schemas if _isidentifier(key)]

    def __getattr__(self, a):
        return self.schemas[a]


class Schema (object):
    """Represents a Schema.
    """
    def __init__(self, sname, schema_doc, **kwargs):
        """Creates the Schema.
        :param sname: the schema's name
        :param schema_doc: the schema document
        """
        super(Schema, self).__init__()
        self.name = sname
        self.tables = {
            tname: kwargs.get('table_class', Table)(sname, tname, tdoc, **kwargs)
            for tname, tdoc in schema_doc.get('tables', {}).items()
        }

    def __dir__(self):
        return list(super(Schema, self).__dir__()) + [key for key in self.tables if _isidentifier(key)]

    def __getattr__(self, a):
        return self.tables[a]

    def __repr__(self):
        s = "Schema name: '%s'\nList of tables:\n" % self.name
        if len(self.table_names) == 0:
            s += "none"
        else:
            s += "\n".join("  '%s'" % tname for tname in self.table_names)
        return s


class DataPath (object):
    """Represents an arbitrary data path."""
    def __init__(self, root):
        assert isinstance(root, TableAlias)
        self._path_expression = Root(root)
        self._root = root
        self._base_uri = root.catalog._server_uri
        self.table_instances = dict()  # map of alias_name => TableAlias object
        self._context = None
        self._identifiers = []
        self._bind_table_instance(root)

    def __dir__(self):
        return list(super(DataPath, self).__dir__()) + self._identifiers

    def __getattr__(self, a):
        return self.table_instances[a]

    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, value):
        assert isinstance(value, TableAlias)
        assert value.name in self.table_instances
        if self._context != value:
            self._path_expression = ResetContext(self._path_expression, value)
            self._context = value

    @property
    def uri(self):
        return self._base_uri + str(self._path_expression)

    def _contextualized_uri(self, context):
        """Returns a path uri for the specified context.
        :param context: a table instance that is bound to this path
        :return: string representation of the path uri
        """
        assert isinstance(context, TableAlias)
        assert context.name in self.table_instances
        if self._context != context:
            return self._base_uri + str(ResetContext(self._path_expression, context))
        else:
            return self.uri

    def _bind_table_instance(self, alias):
        """Binds a new table instance into this path.
        """
        assert isinstance(alias, TableAlias)
        alias.path = self
        self.table_instances[alias.name] = self._context = alias
        if _isidentifier(alias.name):
            self._identifiers.append(alias.name)

    def delete(self):
        """Deletes the entity set referenced by the data path.
        """
        try:
            path = str(self._path_expression)
            logger.debug("Deleting: {p}".format(p=path))
            self._root.catalog.delete(path)
        except HTTPError as e:
            logger.error(e.response.text)
            if 400 <= e.response.status_code < 500:
                raise DataPathException(_http_error_message(e), e)
            else:
                raise e

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
        assert on is None or isinstance(on, FilterPredicate)
        assert join_type == '' or on is not None

        if right.catalog != self._root.catalog:
            raise Exception("Cannot link across catalogs.")

        if isinstance(right, TableAlias):
            # Validate that alias has not been used
            if right.name in self.table_instances:
                raise Exception("Table instance is already linked. "
                                "Consider aliasing it if you want to link another instance of the base table.")
        else:
            # Generate an unused alias name for the table
            table_name = right.name
            alias_name = table_name
            counter = 1
            while alias_name in self.table_instances:
                counter += 1
                alias_name = table_name + str(counter)
            right = right.alias(alias_name)

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
        return self._entities(attributes, renamed_attributes)

    def _entities(self, attributes, renamed_attributes, context=None):
        """Returns the entity set computed by this data path from the perspective of the given 'context'.
        :param attributes: a list of Columns.
        :param renamed_attributes: a list of renamed Columns.
        :param context: optional context for the entities.
        :returns an entity set
        """
        assert context is None or isinstance(context, TableAlias)
        catalog = self._root.catalog

        expression = self._path_expression
        if context:
            expression = ResetContext(expression, context)
        if attributes or renamed_attributes:
            expression = Project(expression, attributes, renamed_attributes)
        base_path = str(expression)

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
                if 400 <= e.response.status_code < 500:
                    raise DataPathException(_http_error_message(e), e)
                else:
                    raise e

        return EntitySet(self._base_uri + base_path, fetcher)


class EntitySet (object):
    """A set of entities.
    The EntitySet is produced by a path. The results may be explicitly fetched. The EntitySet behaves like a
    container. If the EntitySet has not been fetched explicitly, on first use of container operations, it will
    be implicitly fetched from the catalog.
    """
    def __init__(self, uri, fetcher_fn):
        """Initializes the EntitySet.
        :param uri: the uri for the entity set in the catalog.
        :param fetcher_fn: a function that fetches the entities from the catalog.
        """
        assert fetcher_fn is not None
        self._fetcher_fn = fetcher_fn
        self._results_doc = None
        self._dataframe = None
        self.uri = uri

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
    """Represents a Table.
    """
    def __init__(self, sname, tname, table_doc, **kwargs):
        """Creates a Table object.
        :param sname: name of the schema
        :param tname: name of the table
        :param table_doc: deserialized json document of the table definition
        :param kwargs: must include `catalog`
        """
        self.catalog = kwargs['catalog']
        self.sname = sname
        self.name = tname
        self._table_doc = table_doc

        self.columns = {}
        self._identifiers = dir(Table) + ['columns']
        for cdoc in table_doc.get('column_definitions', {}):
            column_name = cdoc['name']
            self.columns[column_name] = kwargs.get('column_class', Column)(sname, tname, cdoc, **_kwargs(table=self, **kwargs))
            if _isidentifier(column_name):
                self._identifiers.append(column_name)

    def __dir__(self):
        return self._identifiers

    def __getattr__(self, a):
        return self.columns[a]

    def __repr__(self):
        s = "Table name: '%s'\nList of columns:\n" % self.name
        if len(self.columns) == 0:
            s += "none"
        else:
            s += "\n".join("  %s" % repr(col) for col in self.columns.values())
        return s

    @property
    def uname(self):
        """the url encoded name"""
        return urlquote(self.name)

    @property
    def fqname(self):
        """the url encoded fully qualified name"""
        return "%s:%s" % (urlquote(self.sname), self.uname)

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
        return DataPath(self.alias(self.name))

    @path.setter
    def path(self, value):
        """Not allowed on base tables.
        """
        raise Exception("Path assignment not allowed on base table objects.")

    @property
    def _contextualized_path(self):
        """Returns the path as contextualized for this table instance.
        Conditionally updates the context of the path to which this table instance is bound.
        """
        return self.path

    @property
    def uri(self):
        return self.path.uri

    def alias(self, alias_name):
        """Returns a table alias object.
        :param alias_name: a string to use as the alias name
        """
        return TableAlias(self, alias_name)

    def filter(self, filter_expression):
        return self._contextualized_path.filter(filter_expression)

    def link(self, right, on=None, join_type=''):
        return self._contextualized_path.link(right, on, join_type)

    def entities(self, *attributes, **renamed_attributes):
        return self.path._entities(attributes, renamed_attributes)

    def insert(self, entities, defaults=None, add_system_defaults=True):
        """Inserts entities into the table.
        :param entities: an iterable collection of entities (i.e., rows) to be inserted into the table.
        :param defaults: optional, set of column names to be assigned the default expression value.
        :param add_system_defaults: flag to add system columns to the set of default columns.
        :return newly created entities.
        """
        defaults_enc = {urlquote(cname) for cname in defaults} if defaults else set()
        if add_system_defaults:
            defaults_enc |= {'RID', 'RCT', 'RMT', 'RCB', 'RMT'}

        path = '/entity/' + self.fqname
        if defaults_enc:
            path += "?defaults={cols}".format(cols=','.join(defaults_enc))
        logger.debug("Inserting entities to path: {path}".format(path=path))

        # JSONEncoder does not handle general iterable objects, so we have to make sure its an acceptable collection
        entities = entities if isinstance(entities, (list, tuple)) else list(entities)
        try:
            resp = self.catalog.post(path, json=entities, headers={'Content-Type': 'application/json'})
            return EntitySet(self.path.uri, lambda ignore: resp.json())
        except HTTPError as e:
            logger.error(e.response.text)
            if 400 <= e.response.status_code < 500:
                raise DataPathException(_http_error_message(e), e)
            else:
                raise e

    def update(self, entities, defaults=None, add_system_defaults=True):
        """Update entities of a table.
        :param entities: an iterable collection of entities (i.e., rows) to be updated in the table.
        :return updated entities.
        """
        # JSONEncoder does not handle general iterable objects, so we have to make sure its an acceptable collection
        entities = entities if isinstance(entities, (list, tuple)) else list(entities)
        try:
            resp = self.catalog.put('/entity/' + self.fqname,
                                     json=entities,
                                     headers={'Content-Type': 'application/json'})
            return EntitySet(self.path.uri, lambda ignore: resp.json())
        except HTTPError as e:
            logger.error(e.response.text)
            if 400 <= e.response.status_code < 500:
                raise DataPathException(_http_error_message(e), e)
            else:
                raise e


class TableAlias (Table):
    """Represents a table alias.
    """
    def __init__(self, table, alias):
        """Initializes the table alias.
        :param table: the base table to be given an alias name
        :param alias: the alias name
        """
        assert isinstance(table, Table)
        super(TableAlias, self).__init__(table.sname, table.name, table._table_doc, **_kwargs(catalog=table.catalog))
        self.base_table = table
        self.name = alias
        self._parent = None

    @property
    def uname(self):
        """the url encoded name"""
        return urlquote(self.name)

    @property
    def fqname(self):
        """the url encoded fully qualified name"""
        return self.base_table.fqname

    @property
    def instancename(self):
        return self.uname

    @property
    def fromname(self):
        return "%s:=%s" % (self.uname, self.base_table.fqname)

    @property
    def path(self):
        """Returns the parent path for this alias.
        """
        if not self._parent:
            self._parent = DataPath(self)
        return self._parent

    @path.setter
    def path(self, value):
        if self._parent:
            raise Exception("Cannot bind a table instance that has already been bound.")
        elif not isinstance(value, DataPath):
            raise Exception("value must be a DataPath instance.")
        self._parent = value

    @property
    def _contextualized_path(self):
        """Returns the path as contextualized for this table instance.
        Conditionally updates the context of the path to which this table instance is bound.
        """
        path = self.path
        if path.context != self:
            path.context = self
        return path

    @property
    def uri(self):
        return self.path._contextualized_uri(self)

    def entities(self, *attributes, **renamed_attributes):
        return self.path._entities(attributes, renamed_attributes, self)


class Column (object):
    """Represents a column in a table.
    """
    def __init__(self, sname, tname, column_doc, **kwargs):
        """Creates a Column object.
        :param sname: schema name
        :param tname: table name
        :param doc: column definition document
        :param kwargs: kwargs must include `table` a Table instance
        """
        super(Column, self).__init__()
        assert 'table' in kwargs
        assert isinstance(kwargs['table'], Table)
        self._table = kwargs['table']
        self._name = column_doc['name']
        self._doc = column_doc

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
        if other is None:
            return FilterPredicate(self, "::null::", '')
        else:
            return FilterPredicate(self, "=", other)

    def __lt__(self, other):
        return FilterPredicate(self, "::lt::", other)

    def __le__(self, other):
        return FilterPredicate(self, "::leq::", other)

    def __gt__(self, other):
        return FilterPredicate(self, "::gt::", other)

    def __ge__(self, other):
        return FilterPredicate(self, "::geq::", other)

    def regexp(self, other):
        assert isinstance(other, str), "This comparison only supports string literals."
        return FilterPredicate(self, "::regexp::", other)

    def ciregexp(self, other):
        assert isinstance(other, str), "This comparison only supports string literals."
        return FilterPredicate(self, "::ciregexp::", other)

    def ts(self, other):
        assert isinstance(other, str), "This comparison only supports string literals."
        return FilterPredicate(self, "::ts::", other)



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
        return "%s/$%s" % (self._r._path, self._alias.uname)


class Filter(PathOperator):
    def __init__(self, r, formula):
        super(Filter, self).__init__(r)
        assert isinstance(formula, Predicate)
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
        assert isinstance(on, FilterPredicate) or isinstance(on, Table)
        assert as_ is None or isinstance(as_, TableAlias)
        assert join_type == '' or (join_type in ('left', 'right', 'full') and isinstance(on, FilterPredicate))
        self._on = on
        self._as = as_
        self._join_type = join_type

    @property
    def _path(self):
        assert isinstance(self._r, PathOperator)
        assign = '' if self._as is None else "%s:=" % self._as.uname
        cond = self._on.fqname if isinstance(self._on, Table) else str(self._on)
        return "%s/%s%s%s" % (self._r._path, assign, self._join_type, cond)


class Predicate (object):
    def __init__(self):
        pass

    def __and__(self, other):
        return JunctionPredicate(self, "&", other)

    def __or__(self, other):
        return JunctionPredicate(self, ";", other)

    def __invert__(self):
        return NegationPredicate(self)


class FilterPredicate (Predicate):
    def __init__(self, lop, op, rop):
        super(FilterPredicate, self).__init__()
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


class NegationPredicate (Predicate):
    def __init__(self, child):
        super(NegationPredicate, self).__init__()
        assert isinstance(child, Predicate)
        self._child = child

    def __str__(self):
        return "!(%s)" % self._child
