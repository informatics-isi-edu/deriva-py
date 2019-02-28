from . import urlquote
from datetime import date
import logging
import re
from requests import HTTPError

logger = logging.getLogger(__name__)
"""Logger for this module"""

_system_defaults = {'RID', 'RCT', 'RCB', 'RMT', 'RMB'}
"""Set of system default column names"""


def _kwargs(**kwargs):
    """Helper for extending datapath with sub-types for the whole model tree."""
    kwargs2 = {
        'schema_class': Schema,
        'table_class': Table,
        'column_class': Column
    }
    kwargs2.update(kwargs)
    return kwargs2


def from_catalog(catalog):
    """Creates a datapath.Catalog object from an ErmrestCatalog object.
    :param catalog: an ErmrestCatalog object
    :return: a datapath.Catalog object
    """
    return Catalog(catalog.getCatalogSchema(), **_kwargs(catalog=catalog))


def _isidentifier(a):
    """Tests if string is a valid python identifier.
    This function is intended for internal usage within this module.
    :param a: a string
    """
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
        self.message = message
        self.reason = reason

    def __str__(self):
        return self.message


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
        return list(dir(super(Catalog, self))) + [key for key in self.schemas if _isidentifier(key)]

    def __getattr__(self, a):
        if a in self.schemas:
            return self.schemas[a]
        else:
            return getattr(super(Catalog, self), a)


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
        return list(dir(super(Schema, self))) + ['name', 'tables', 'describe'] + [key for key in self.tables if _isidentifier(key)]

    def __getattr__(self, a):
        if a in self.tables:
            return self.tables[a]
        else:
            return getattr(super(Schema, self), a)

    def describe(self):
        """Provides a description of the model element.

        :return: a user-friendly string representation of the model element.
        """
        s = "Schema name: '%s'\nList of tables:\n" % self.name
        if len(self.tables) == 0:
            s += "none"
        else:
            s += "\n".join("  '%s'" % tname for tname in self.tables)
        return s

    def _repr_html_(self):
        return self.describe()


class DataPath (object):
    """Represents an arbitrary data path."""
    def __init__(self, root):
        assert isinstance(root, TableAlias)
        self._path_expression = Root(root)
        self._root = root
        self._base_uri = root.catalog._server_uri
        self._table_instances = dict()  # map of alias_name => TableAlias object
        self._context = None
        self._identifiers = []
        self._bind_table_instance(root)

    def __dir__(self):
        return list(dir(super(DataPath, self))) + self._identifiers

    def __getattr__(self, a):
        if a in self._table_instances:
            return self._table_instances[a]
        else:
            return getattr(super(DataPath, self), a)

    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, value):
        assert isinstance(value, TableAlias)
        assert value.name in self._table_instances
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
        assert context.name in self._table_instances
        if self._context != context:
            return self._base_uri + str(ResetContext(self._path_expression, context))
        else:
            return self.uri

    def _bind_table_instance(self, alias):
        """Binds a new table instance into this path.
        """
        assert isinstance(alias, TableAlias)
        alias.path = self
        self._table_instances[alias.name] = self._context = alias
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
        :return: self
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
        :return: self
        """
        assert isinstance(right, Table)
        assert on is None or isinstance(on, FilterPredicate)
        assert join_type == '' or on is not None

        if right.catalog != self._root.catalog:
            raise Exception("Cannot link across catalogs.")

        if isinstance(right, TableAlias):
            # Validate that alias has not been used
            if right.name in self._table_instances:
                raise Exception("Table instance is already linked. "
                                "Consider aliasing it if you want to link another instance of the base table.")
        else:
            # Generate an unused alias name for the table
            table_name = right.name
            alias_name = table_name
            counter = 1
            while alias_name in self._table_instances:
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
        :return: an entity set
        """
        return self._entities(attributes, renamed_attributes)

    def _entities(self, attributes, renamed_attributes, context=None):
        """Returns the entity set computed by this data path from the perspective of the given 'context'.
        :param attributes: a list of Columns.
        :param renamed_attributes: a list of renamed Columns.
        :param context: optional context for the entities.
        :return: an entity set
        """
        assert context is None or isinstance(context, TableAlias)
        catalog = self._root.catalog

        expression = self._path_expression
        if context:
            expression = ResetContext(expression, context)
        if attributes or renamed_attributes:
            expression = Project(expression, attributes, renamed_attributes)
        base_path = str(expression)

        def fetcher(limit=None, sort=None):
            assert limit is None or isinstance(limit, int)
            assert sort is None or hasattr(sort, '__iter__')
            limiting = '?limit=%d' % limit if limit else ''
            sorting = '@sort(' + ','.join([col.uname for col in sort]) + ')' if sort else ''
            path = base_path + sorting + limiting
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
        """Pandas DataFrame representation of this path.

        :return: a pandas dataframe object
        :raise ImportError: exception if the 'pandas' library is not available
        """
        if not self._dataframe:
            from pandas import DataFrame
            self._dataframe = DataFrame(self._results)
        return self._dataframe

    def __len__(self):
        return len(self._results)

    def __getitem__(self, item):
        return self._results[item]

    def __iter__(self):
        return iter(self._results)

    def fetch(self, limit=None, sort=None):
        """Fetches the entities from the catalog.
        :param limit: maximum number of entities to fetch from the catalog.
        :param sort: collection of columns to use for sorting.
        :return: self
        """
        limit = int(limit) if limit else None
        self._results_doc = self._fetcher_fn(limit, sort)
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
        self._kwargs = kwargs

        kwargs.update(table=self)
        self.column_definitions = {
            cdoc['name']: kwargs.get('column_class', Column)(sname, tname, cdoc, **kwargs)
            for cdoc in table_doc.get('column_definitions', [])
        }

    def __dir__(self):
        return list(dir(super(Table, self))) + \
               ['catalog', 'sname', 'name', 'describe'] + \
               [key for key in self.column_definitions if _isidentifier(key)]

    def __getattr__(self, a):
        if a in self.column_definitions:
            return self.column_definitions[a]
        else:
            return getattr(super(Table, self), a)

    def describe(self):
        """Provides a description of the model element.

        :return: a user-friendly string representation of the model element.
        """
        s = "Table name: '%s'\nList of columns:\n" % self.name
        if len(self.column_definitions) == 0:
            s += "none"
        else:
            s += "\n".join("  %s" % col.name for col in self.column_definitions.values())
        return s

    def _repr_html_(self):
        return self.describe()

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

    def insert(self, entities, defaults=set(), nondefaults=set(), add_system_defaults=True):
        """Inserts entities into the table.
        :param entities: an iterable collection of entities (i.e., rows) to be inserted into the table.
        :param defaults: optional, set of column names to be assigned the default expression value.
        :param nondefaults: optional, set of columns names to override implicit system defaults
        :param add_system_defaults: flag to add system columns to the set of default columns.
        :return newly created entities.
        """
        # empty entities will be accepted but results are therefore an empty entity set
        if not entities:
            return EntitySet(self.path.uri, lambda ignore1, ignore2: [])

        options = []

        if defaults or add_system_defaults:
            defaults_enc = {urlquote(cname) for cname in defaults}
            if add_system_defaults:
                defaults_enc |= _system_defaults - nondefaults
            options.append("defaults={cols}".format(cols=','.join(defaults_enc)))

        if nondefaults:
            nondefaults_enc = {urlquote(cname) for cname in nondefaults}
            options.append("nondefaults={cols}".format(cols=','.join(nondefaults_enc)))

        path = '/entity/' + self.fqname
        if options:
            path += "?" + "&".join(options)
        logger.debug("Inserting entities to path: {path}".format(path=path))

        # JSONEncoder does not handle general iterable objects, so we have to make sure its an acceptable collection
        if not hasattr(entities, '__iter__'):
            raise ValueError('entities is not iterable')
        entities = entities if isinstance(entities, (list, tuple)) else list(entities)

        # test the first entity element to make sure that it looks like a dictionary
        if not hasattr(entities[0], 'keys'):
            raise ValueError('entities[0] does not look like a dictionary -- does not have a "keys()" method')

        try:
            resp = self.catalog.post(path, json=entities, headers={'Content-Type': 'application/json'})
            return EntitySet(self.path.uri, lambda ignore1, ignore2: resp.json())
        except HTTPError as e:
            logger.error(e.response.text)
            if 400 <= e.response.status_code < 500:
                raise DataPathException(_http_error_message(e), e)
            else:
                raise e

    def update(self, entities, correlation={'RID'}, targets=None):
        """Update entities of a table.

        For more information see the ERMrest protocol for the `attributegroup` interface. By default, this method will
        correlate the input data (entities) based on the `RID` column of the table. By default, the method will use all
        column names found in the first row of the `entities` input, which are not found in the `correlation` set and
        not defined as 'system columns' by ERMrest, as the targets if `targets` is not set.

        :param entities: an iterable collection of entities (i.e., rows) to be updated in the table.
        :param correlation: an iterable collection of column names used to correlate input set to the set of rows to be
        updated in the catalog. E.g., `{'col name'}` or `{mytable.mycolumn}` will work if you pass a Column object.
        :param targets: an iterable collection of column names used as the targets of the update operation.
        :return: EntitySet of updated entities as returned by the corresponding ERMrest interface.
        """
        # empty entities will be accepted but results are therefore an empty entity set
        if not entities:
            return EntitySet(self.path.uri, lambda ignore1, ignore2: [])

        # JSONEncoder does not handle general iterable objects, so we have to make sure its an acceptable collection
        if not hasattr(entities, '__iter__'):
            raise ValueError('entities is not iterable')
        entities = entities if isinstance(entities, (list, tuple)) else list(entities)

        # test the first entity element to make sure that it looks like a dictionary
        if not hasattr(entities[0], 'keys'):
            raise ValueError('entities[0] does not look like a dictionary -- does not have a "keys()" method')

        # Form the correlation keys and the targets
        correlation_cnames = {urlquote(str(c)) for c in correlation}
        if targets:
            target_cnames = {urlquote(str(t)) for t in targets}
        else:
            exclusions = correlation_cnames | _system_defaults
            target_cnames = {urlquote(str(t)) for t in entities[0].keys() if urlquote(str(t)) not in exclusions}

        # Form the path
        path = '/attributegroup/{table}/{correlation};{targets}'.format(
            table=self.fqname,
            correlation=','.join(correlation_cnames),
            targets=','.join(target_cnames)
        )

        try:
            resp = self.catalog.put(path, json=entities, headers={'Content-Type': 'application/json'})
            return EntitySet(self.path.uri, lambda ignore1, ignore2: resp.json())
        except HTTPError as e:
            logger.error(e.response.text)
            if 400 <= e.response.status_code < 500:
                raise DataPathException(_http_error_message(e), e)
            else:
                raise e


class TableAlias (Table):
    """Represents a table alias.
    """
    def __init__(self, base_table, alias_name):
        """Initializes the table alias.
        :param base_table: the base table to be given an alias name
        :param alias_name: the alias name
        """
        assert isinstance(base_table, Table)
        super(TableAlias, self).__init__(base_table.sname, base_table.name, base_table._table_doc, **base_table._kwargs)
        self._base_table = base_table
        self.name = alias_name
        self._parent = None

    @property
    def uname(self):
        """the url encoded name"""
        return urlquote(self.name)

    @property
    def fqname(self):
        """the url encoded fully qualified name"""
        return self._base_table.fqname

    @property
    def instancename(self):
        return self.uname

    @property
    def fromname(self):
        return "%s:=%s" % (self.uname, self._base_table.fqname)

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
        :param column_doc: column definition document
        :param kwargs: kwargs must include `table` a Table instance
        """
        super(Column, self).__init__()
        assert 'table' in kwargs
        assert isinstance(kwargs['table'], Table)
        self._table = kwargs['table']
        self.sname = sname
        self.tname = tname
        self.name = column_doc['name']
        self.type = column_doc['type']
        self.comment = column_doc['comment']

    def describe(self):
        """Provides a description of the model element.

        :return: a user-friendly string representation of the model element.
        """
        return "Column name: '%s'\tType: %s\tComment: '%s'" % \
               (self.name, self.type['typename'], self.comment)

    def _repr_html_(self):
        return self.describe()

    @property
    def uname(self):
        """the url encoded name"""
        return urlquote(self.name)

    @property
    def fqname(self):
        """the url encoded fully qualified name"""
        return "%s:%s" % (self._table.fqname, self.uname)

    @property
    def instancename(self):
        table_instancename = self._table.instancename
        if len(table_instancename) > 0:
            return "%s:%s" % (table_instancename, self.uname)
        else:
            return self.uname

    @property
    def desc(self):
        """A descending sort modifier based on this column."""
        return SortDescending(self)

    def __str__(self):
        return self.name

    def eq(self, other):
        """Returns an 'equality' comparison predicate.

        :param other: `None` or any other literal value.
        :return: a filter predicate object
        """
        if other is None:
            return FilterPredicate(self, "::null::", '')
        else:
            return FilterPredicate(self, "=", other)

    __eq__ = eq

    def lt(self, other):
        """Returns a 'less than' comparison predicate.

        :param other: a literal value.
        :return: a filter predicate object
        """
        return FilterPredicate(self, "::lt::", other)

    __lt__ = lt

    def le(self, other):
        """Returns a 'less than or equal' comparison predicate.

        :param other: a literal value.
        :return: a filter predicate object
        """
        return FilterPredicate(self, "::leq::", other)

    __le__ = le

    def gt(self, other):
        """Returns a 'greater than' comparison predicate.

        :param other: a literal value.
        :return: a filter predicate object
        """
        return FilterPredicate(self, "::gt::", other)

    __gt__ = gt

    def ge(self, other):
        """Returns a 'greater than or equal' comparison predicate.

        :param other: a literal value.
        :return: a filter predicate object
        """
        return FilterPredicate(self, "::geq::", other)

    __ge__ = ge

    def regexp(self, other):
        """Returns a 'regular expression' comparison predicate.

        :param other: a _string_ literal value.
        :return: a filter predicate object
        """
        if not isinstance(other, str):
            logger.warning("'regexp' method comparison only supports string literals.")
        return FilterPredicate(self, "::regexp::", other)

    def ciregexp(self, other):
        """Returns a 'case-insensitive regular expression' comparison predicate.

        :param other: a _string_ literal value.
        :return: a filter predicate object
        """
        if not isinstance(other, str):
            logger.warning("'ciregexp' method comparison only supports string literals.")
        return FilterPredicate(self, "::ciregexp::", other)

    def ts(self, other):
        """Returns a 'text search' comparison predicate.

        :param other: a _string_ literal value.
        :return: a filter predicate object
        """
        if not isinstance(other, str):
            logger.warning("'ts' method comparison only supports string literals.")
        return FilterPredicate(self, "::ts::", other)


class SortDescending (object):
    """Represents a descending sort condition.
    """
    def __init__(self, col):
        """Creates sort descending object.

        :param col: a column object
        """
        assert isinstance(col, Column)
        self.col = col

    @property
    def uname(self):
        """the url encoded name"""
        return urlquote(self.col.uname) + "::desc::"


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

    def and_(self, other):
        """Returns a conjunction predicate.

        :param other: a predicate object.
        :return: a junction predicate object.
        """
        return JunctionPredicate(self, "&", other)

    __and__ = and_

    def or_(self, other):
        """Returns a disjunction predicate.

        :param other: a predicate object.
        :return: a junction predicate object.
        """
        return JunctionPredicate(self, ";", other)

    __or__ = or_

    def negate(self):
        """Returns a negation predicate.

        This predicate is wrapped in a negation predicate which is returned to the caller.

        :return: a negation predicate object.
        """
        return NegationPredicate(self)

    __invert__ = negate


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
