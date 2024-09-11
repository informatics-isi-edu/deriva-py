"""Definitions and implementations for data-path expressions to query and manipulate (insert, update, delete)."""

from . import urlquote
import copy
from datetime import date
import itertools
import logging
import time
import re
from requests import HTTPError
import warnings
from . import DEFAULT_HEADERS, ermrest_model as _erm

__all__ = ['DataPathException', 'Min', 'Max', 'Sum', 'Avg', 'Cnt', 'CntD', 'Array', 'ArrayD', 'Bin']

logger = logging.getLogger(__name__)
"""Logger for this module"""

_system_defaults = {'RID', 'RCT', 'RCB', 'RMT', 'RMB'}
"""Set of system default column names"""


def deprecated(f):
    """A simple 'deprecated' function decorator."""
    def wrapper(*args, **kwargs):
        warnings.warn("'%s' has been deprecated" % f.__name__, DeprecationWarning, stacklevel=2)
        return f(*args, **kwargs)
    return wrapper


def from_catalog(catalog):
    """Wraps an ErmrestCatalog object for use in datapath expressions.

    :param catalog: an ErmrestCatalog object
    :return: a datapath._CatalogWrapper object
    """
    return _CatalogWrapper(catalog)


def _isidentifier(a):
    """Tests if string is a valid python identifier.

    This function is intended for internal usage within this module.

    :param a: a string
    """
    if hasattr(a, 'isidentifier'):
        return a.isidentifier()
    else:
        return re.match("[_A-Za-z][_a-zA-Z0-9]*$", a) is not None


def _identifier_for_name(name, *reserveds):
    """Makes an identifier from a given name and disambiguates if it is reserved.

    1. replace invalid identifier characters with '_'
    2. prepend with '_' if first character is a digit
    3. append a disambiguating positive integer if it is reserved

    :param name: a string of any format
    :param *reserveds: iterable collections of reserved strings
    :return: a valid identifier string for the given name
    """
    assert len(name) > 0, 'empty strings are not allowed'

    # replace invalid characters with '_'s
    identifier = re.sub("[^_a-zA-Z0-9]", "_", name)

    # prepend with '_' is it starts with a digit
    if identifier[0].isdigit():
        identifier = '_' + identifier

    # append a disambiguating positive integer if it is reserved
    disambiguator = 1
    ambiguous = identifier
    while any(identifier in reserved for reserved in reserveds):
        identifier = ambiguous + str(disambiguator)
        disambiguator += 1

    return identifier


def _make_identifier_to_name_mapping(names, reserved):
    """Makes a dictionary of (valid) identifiers to (original) names.

    Try to favor the names that require the least modification:
    1. add all names that are valid identifiers and do not conflict with reserved names
    2. add all names that are valid identifiers but do conflict with reserved names by appending a disambiguator
    3. add an unambiguous identifier made from the name, when the name is not already a valid identifier

    :param names: iterable collection of strings
    :param reserved: iterable collection of reserved identifiers
    :return: a dictionary to map from identifier to name
    """
    reserved = set(reserved)
    assert all(_isidentifier(r) for r in reserved), 'all reserved names must be valid identifiers'

    mappings = {  # first, add all non-offending names
        name: name
        for name in names if _isidentifier(name) and name not in reserved
    }
    mappings.update({  # second, add all names that conflict with reserved strings
        name + '1': name
        for name in names if name in reserved and name + '1' not in mappings
    })
    invalid_names = set(names) - mappings.keys()

    # third, convert and disambiguate remaining names
    for name in invalid_names:
        mappings[_identifier_for_name(name, mappings.keys(), reserved)] = name

    return mappings


def _http_error_message(e):
    """Returns a formatted error message from the raw HTTPError.
    """
    return '\n'.join(e.response.text.splitlines()[1:]) + '\n' + str(e)


class DataPathException (Exception):
    """Exception in a datapath expression.
    """
    def __init__(self, message, reason=None):
        super(DataPathException, self).__init__(message, reason)
        self.message = message
        self.reason = reason

    def __str__(self):
        return self.message


class _CatalogWrapper (object):
    """Wraps a Catalog for datapath expressions.
    """
    def __init__(self, catalog):
        """Creates the _CatalogWrapper.

        :param catalog: ErmrestCatalog object
        """
        super(_CatalogWrapper, self).__init__()
        self._wrapped_catalog = catalog
        self._wrapped_model = catalog.getCatalogModel()
        self.schemas = {
            k: _SchemaWrapper(self, v)
            for k, v in self._wrapped_model.schemas.items()
        }
        self._identifiers = _make_identifier_to_name_mapping(
            self.schemas.keys(),
            super(_CatalogWrapper, self).__dir__())

    def __dir__(self):
        return itertools.chain(
            super(_CatalogWrapper, self).__dir__(),
            self._identifiers.keys()
        )

    def __getattr__(self, a):
        if a in self._identifiers:
            return self.schemas[self._identifiers[a]]
        elif hasattr(super(_CatalogWrapper, self), a):
            return getattr(super(_CatalogWrapper, self), a)
        else:
            raise AttributeError("'%s' object for catalog '%s' has no attribute or schema '%s'" % (type(self).__name__, self._wrapped_catalog.catalog_id, a))

    @classmethod
    def compose(cls, *paths):
        """Compose path fragments into a path.

        The root of any path fragment must be found in the table instances of the currently composed path from left
        to right, _but_ it does not have to be the current context (last table instance) of the last left hand path.

        Paths must not have overlapping table instances with the currently composed path from left to right, except for
        each subsequent path's root table instance which _must_ be defined in one of the left hand paths.

        No input path in 'paths' will be mutated.

        :param paths: instances of `DataPath`
        :return: a new `DataPath` instance composed from the 'paths'
        """
        if not paths:
            raise ValueError("No input path(s) given")
        if not all(isinstance(path, DataPath) for path in paths):
            raise TypeError("Input 'paths' must be an instance of %s" % type(DataPath).__name__)
        base = copy.deepcopy(paths[0])
        for path in paths[1:]:
            base.merge(path)
        return base


class _SchemaWrapper (object):
    """Wraps a Schema for datapath expressions.
    """
    def __init__(self, catalog, schema):
        """Creates the _SchemaWrapper.

        :param catalog: the catalog wrapper to which this schema wrapper belongs
        :param schema: the wrapped schema object
        """
        super(_SchemaWrapper, self).__init__()
        self._catalog = catalog
        self._wrapped_schema = schema
        self._name = schema.name
        self.tables = {
            k: _TableWrapper(self, v)
            for k, v in schema.tables.items()
        }
        self._identifiers = _make_identifier_to_name_mapping(
            self.tables.keys(),
            super(_SchemaWrapper, self).__dir__())

    def __dir__(self):
        return itertools.chain(
            super(_SchemaWrapper, self).__dir__(),
            self._identifiers.keys()
        )

    def __getattr__(self, a):
        if a in self._identifiers:
            return self.tables[self._identifiers[a]]
        elif hasattr(super(_SchemaWrapper, self), a):
            return getattr(super(_SchemaWrapper, self), a)
        else:
            raise AttributeError("'%s' object for schema '%s' has no attribute or table '%s'" % (type(self).__name__, self._name, a))

    @deprecated
    def describe(self):
        """Provides a description of the model element.

        :return: a user-friendly string representation of the model element.
        """
        s = "_SchemaWrapper name: '%s'\nList of tables:\n" % self._name
        if len(self.tables) == 0:
            s += "none"
        else:
            s += "\n".join("  '%s'" % tname for tname in self.tables)
        return s

    @deprecated
    def _repr_html_(self):
        return self.describe()


class DataPath (object):
    """Represents a datapath expression.
    """
    def __init__(self, root):
        assert isinstance(root, _TableAlias)
        self._path_expression = _Root(root)
        self._root = root
        self._base_uri = root._schema._catalog._wrapped_catalog._server_uri
        self._table_instances = dict()  # map of alias_name => _TableAlias object
        self._context = None
        self._identifiers = {}
        self._bind_table_instance(root)

    def __dir__(self):
        return itertools.chain(
            super(DataPath, self).__dir__(),
            self._identifiers.keys()
        )

    def __getattr__(self, a):
        if a in self._identifiers:
            return self._table_instances[self._identifiers[a]]
        elif hasattr(super(DataPath, self), a):
            return getattr(super(DataPath, self), a)
        else:
            raise AttributeError("'%s' object has no attribute or table instance '%s'" % (type(self).__name__, a))

    def __deepcopy__(self, memodict={}):
        cp = DataPath(copy.deepcopy(self._root, memo=memodict))
        for alias in copy.deepcopy(self._table_instances, memo=memodict).values():
            if alias != cp._root:
                cp._bind_table_instance(alias)
        cp._context = cp._table_instances[self._context._name]
        cp._path_expression = copy.deepcopy(self._path_expression, memo=memodict)
        assert not cp._table_instances.keys() - set(cp._identifiers)
        assert cp._table_instances.keys() == self._table_instances.keys()
        assert cp._identifiers.keys() == self._identifiers.keys()
        assert cp._root._name in cp._table_instances
        assert cp._root == cp._table_instances[cp._root._name]
        assert cp._root != self._root
        assert cp._root._name == self._root._name
        assert cp._context != self._context
        assert cp._context._name == self._context._name
        assert str(cp._path_expression) == str(self._path_expression)
        assert cp._path_expression != self._path_expression
        return cp

    @property
    def table_instances(self):
        """Collection of the table instances in this datapath expression."""
        return self._table_instances

    @property
    def context(self):
        """Context (i.e., last bound table instance) of this datapath expression."""
        return self._context

    @context.setter
    def context(self, value):
        """Updates the context of this datapath expression (must be a table instance bound to this expression)."""
        if not isinstance(value, _TableAlias):
            raise TypeError('context must be a table alias object')
        if value._name not in self._table_instances:
            raise ValueError('table alias must be bound in this path')
        if self._context != value:
            self._path_expression = _ResetContext(self._path_expression, value)
            self._context = value

    @property
    def uri(self):
        """The current URI serialization of this datapath expression."""
        return self._base_uri + str(self._path_expression)

    def _contextualized_uri(self, context):
        """Returns a path uri for the specified context.

        :param context: a table instance that is bound to this path
        :return: string representation of the path uri
        """
        assert isinstance(context, _TableAlias)
        assert context._name in self._table_instances
        if self._context != context:
            return self._base_uri + str(_ResetContext(self._path_expression, context))
        else:
            return self.uri

    def _bind_table_instance(self, alias):
        """Binds a new table instance into this path.
        """
        assert isinstance(alias, _TableAlias)
        alias._bind(self)
        self._table_instances[alias._name] = self._context = alias
        self._identifiers[_identifier_for_name(alias._name, self._identifiers.keys(), super(DataPath, self).__dir__())] = alias._name

    def delete(self):
        """Deletes the entity set referenced by the data path.
        """
        try:
            path = str(self._path_expression)
            logger.debug("Deleting: {p}".format(p=path))
            self._root._schema._catalog._wrapped_catalog.delete(path)
        except HTTPError as e:
            logger.debug(e.response.text)
            if 400 <= e.response.status_code < 500:
                raise DataPathException(_http_error_message(e), e)
            else:
                raise e

    def filter(self, filter_expression):
        """Filters the path based on the specified formula.

        :param filter_expression: should be a valid _Predicate object
        :return: self
        """
        assert isinstance(filter_expression, _Predicate)
        self._path_expression = _Filter(self._path_expression, filter_expression)
        return self

    def link(self, right, on=None, join_type=''):
        """Links this path with another table.

        To link a table with an unambigious relationship where table A is related to table B via a single foreign key
        reference, the `on` clause is not.

        ```
        # let A and B be variables for tables from the catalog
        path = A.link(B)
        ```

        To link tables with more than one foreign key reference between them, use explicit `on` clause.

        ```
        # let A.c1 be a column that is a simple foreign key to B.c1 that is a simple key in B
        path = A.link(B, on=(A.c1 == B.c1))
        ```

        To link tables with foreign keys on composite keys, use a conjunction of 2 or more equality comparisons in the
        `on` clause.

        ```
        # let A.c1, A.c2 be columns that form a foreign key to B.c1, B.c2 that are a composite key in B
        path = A.link(B, on=((A.c1 == B.c1) & (A.c2 == B.c2)))
        ```

        Alternatively, use an `ermrest_model.ForeignKey` object to link the table to the path. Both "inbound" and
        "outbound" foreign keys are supported by the `link` method.

        ```
        # let fk be a foreign key object from table A to table B (or from table B to table A)
        path = A.link(B, on=fk)
        ```

        By default links use inner join semantics on the foreign key / key equality comparison. The `join_type`
        parameter can be used to specify `left`, `right`, or `full` outer join semantics.

        :param right: the right hand table of the link expression; if the table or alias name is in use, an incremental
        number will be used to disambiguate tables instances of the same original name.
        :param on: an equality comparison between key and foreign key columns, a conjunction of such comparisons, or a foreign key object
        :param join_type: the join type of this link which may be 'left', 'right', 'full' outer joins or '' for inner
        join link by default.
        :return: self
        """
        if not isinstance(right, _TableWrapper):
            raise TypeError("'right' must be a '_TableWrapper' instance")
        if on and not (
            isinstance(on, _ComparisonPredicate) or
            (isinstance(on, _ConjunctionPredicate) and on.is_valid_join_condition) or
            isinstance(on, _erm.ForeignKey)
        ):
            raise TypeError("'on' must be a comparison, conjuction of comparisons, or foreign key object")
        if join_type and on is None:
            raise ValueError("'on' must be specified for outer joins")
        if right._schema._catalog != self._root._schema._catalog:
            raise ValueError("'right' is from a different catalog. Cannot link across catalogs.")
        if isinstance(right, _TableAlias) and right._parent == self:
            raise ValueError("'right' is a table alias that has already been used.")
        else:
            # Generate an unused alias name for the table
            table_name = right._name
            alias_name = table_name
            counter = 1
            while alias_name in self._table_instances:
                counter += 1
                alias_name = table_name + str(counter)
            right = right.alias(alias_name)

        if on is None:
            # if 'on' not given, default to the 'right' table
            on = right
        elif isinstance(on, _erm.ForeignKey):
            catalog = self._root._schema._catalog
            fk = on
            # determine 'direction' -- inbound or outbound
            path_context_table = self.context._base_table._wrapped_table
            if (path_context_table.schema.name, path_context_table.name) == (fk.table.schema.name, fk.table.name):
                fkcols = zip(fk.foreign_key_columns, fk.referenced_columns)
            elif (path_context_table.schema.name, path_context_table.name) == (fk.pk_table.schema.name, fk.pk_table.name):
                fkcols = zip(fk.referenced_columns, fk.foreign_key_columns)
            else:
                raise ValueError('"%s" is not an inbound or outbound foreign key for the path\'s context, table "%s"' % (fk.constraint_name, path_context_table.name))

            # compose join condition
            on = None
            for lcol, rcol in fkcols:
                lcol = catalog.schemas[lcol.table.schema.name].tables[lcol.table.name].columns[lcol.name]
                rcol = catalog.schemas[rcol.table.schema.name].tables[rcol.table.name].columns[rcol.name]
                if on:
                    on = on & (lcol == rcol)
                else:
                    on = lcol == rcol

        # Extend path expression
        self._path_expression = _Link(self._path_expression, on, right, join_type)

        # Bind alias and this data path
        self._bind_table_instance(right)

        return self

    def entities(self):
        """Returns a results set of whole entities from this data path's current context.

        ```
        results1 = my_path.entities()
        ```

        :return: a result set of entities where each element is a whole entity per the table definition and policy.
        """
        return self._query()

    def aggregates(self, *functions):
        """Returns a results set of computed aggregates from this data path.

        By using the built-in subclasses of the `AggregateFunction` class, including `Min`, `Max`, `Sum`, `Avg`, `Cnt`,
        `CntD`, `Array`, and `ArrayD`, aggregates can be computed and fetched. These aggregates must be passed as named
        parameters since they require _alias names_.

        ```
        results1 = my_path.aggregates(Min(col1).alias('mincol1'), Array(col2).alias('arrcol2'))
        results2 = my_path.aggregates(Min(col1), Array(col2))  # Error! Aggregates must be aliased.
        results3 = my_path.aggregates(col1, Array(col2).alias('arrcol2'))  # Error! Cannot mix columns and aggregate functions.
        ```

        :param functions: aliased aggregate functions
        :return: a results set with a single row of results.
        """
        return self._query(mode=_Project.AGGREGATE, projection=list(functions))

    def attributes(self, *attributes):
        """Returns a results set of attributes projected and optionally renamed from this data path.

        ```
        results1 = my_path.attributes(col1, col2)  # fetch a subset of attributes of the path
        results2 = my_path.attributes(col1.alias('col_1'), col2.alias('col_2'))  # fetch and rename the attributes
        results3 = my_path.attributes(col1, col2.alias('col_2'))  # rename some but not others
        ```

        :param attributes: a list of Columns.
        :return: a results set of the projected attributes from this data path.
        """
        return self._query(mode=_Project.ATTRIBUTE, projection=list(attributes))

    def groupby(self, *keys):
        """Returns an attribute group object.

        The attribute group object returned by this method can be used to get a results set of computed aggregates for
        groups of attributes from this data path.

        With a single group key:
        ```
        results1 = my_path.groupby(col1).attributes(Min(col2).alias('min_col1'), Array(col3).alias('arr_col2'))
        ```

        With more than one group key:
        ```
        results2 = my_path.groupby(col1, col2).attributes(Min(col3).alias('min_col1'), Array(col4).alias('arr_col2'))
        ```

        With aliased group keys:
        ```
        results3 = my_path.groupby(col1.alias('key_one'), col2.alias('keyTwo'))\
                          .attributes(Min(col3).alias('min_col1'), Array(col4).alias('arr_col2'))
        ```

        With binning:
        ```
        results3 = my_path.groupby(col1.alias('key_one'), Bin(col2;10;0;9999).alias('my_bin'))\
                          .attributes(Min(col3).alias('min_col1'), Array(col4).alias('arr_col2'))
        ```

        :param keys: a list of columns, aliased columns, or aliased bins, to be used as the grouping key.
        :return: an attribute group that supports an `.attributes(...)` method that accepts columns, aliased columns,
        and/or aliased aggregate functions as its arguments.
        """
        return _AttributeGroup(self, self._query, keys)

    def _query(self, mode='entity', projection=[], group_key=[], context=None):
        """Internal method for querying the data path from the perspective of the given 'context'.

        :param mode: a valid mode in Project.MODES
        :param projection: a projection list.
        :param group_key: a group key list (only for attributegroup queries).
        :param context: optional context for the query.
        :return: a results set.
        """
        assert context is None or isinstance(context, _TableAlias)
        catalog = self._root._schema._catalog._wrapped_catalog

        expression = self._path_expression
        if context:
            expression = _ResetContext(expression, context)
        if mode != _Project.ENTITY:
            expression = _Project(expression, mode, projection, group_key)
        base_path = str(expression)

        def fetcher(limit=None, sort=None, headers=DEFAULT_HEADERS):
            assert limit is None or isinstance(limit, int)
            assert sort is None or hasattr(sort, '__iter__')
            limiting = '?limit=%d' % limit if limit else ''
            sorting = '@sort(' + ','.join([col._uname for col in sort]) + ')' if sort else ''
            path = base_path + sorting + limiting
            logger.debug("Fetching " + path)
            try:
                resp = catalog.get(path, headers=headers)
                return resp.json()
            except HTTPError as e:
                logger.debug(e.response.text)
                if 400 <= e.response.status_code < 500:
                    raise DataPathException(_http_error_message(e), e)
                else:
                    raise e

        return _ResultSet(self._base_uri + base_path, fetcher)

    def merge(self, path):
        """Merges the current path with the given path.

        The right-hand 'path' must be rooted on a `_TableAlias` object that exists (by alias name) within this path
        (the left-hand path). It _must not_ have other shared table aliases.

        :param path: a `DataPath` object rooted on a table alias that can be found in this path
        :return: this path merged with the given (right-hand) path
        """
        if not isinstance(path, DataPath):
            raise TypeError("'path' must be an instance of %s" % type(self).__name__)
        if path._root._name not in self._table_instances:
            raise ValueError("right-hand path root not found in this path's table instances")
        if not path._root._equivalent(self._table_instances[path._root._name]):
            raise ValueError("right-hand path root is not equivalent to the matching table instance in this path")
        if self._table_instances.keys() & path._table_instances.keys() != {path._root._name}:
            raise ValueError("overlapping table instances found in right-hand path")

        # update this path as rebased right-hand path
        temp = copy.deepcopy(path._path_expression)
        temp.rebase(self._path_expression, self._table_instances[path._root._name])
        self._path_expression = temp

        # copy and bind table instances from right-hand path
        for alias in path._table_instances:
            if alias not in self.table_instances:
                self._bind_table_instance(copy.deepcopy(path._table_instances[alias]))

        # set the context
        self._context = self._table_instances[path._context._name]

        return self

    def denormalize(self, context_name=None, heuristic=None, groupkey_name='RID'):
        """Denormalizes a path based on a visible-columns annotation 'context' or a heuristic approach.

        This method does not mutate this object. It returns a result set representing the denormalization of the path.

        :param context_name: name of the visible-columns context or if none given, will attempt apply heuristics
        :param heuristic: heuristic to apply if no context name specified
        :param groupkey_name: column name for the group by key of the generated query expression (default: 'RID')
        :return: a results set.
        """
        return _datapath_denormalize(self, context_name=context_name, heuristic=heuristic, groupkey_name=groupkey_name)


class _ResultSet (object):
    """A set of results for various queries or data manipulations.

    The result set is produced by a path. The results may be explicitly fetched. The result set behaves like a
    container. If the result set has not been fetched explicitly, on first use of container operations, it will
    be implicitly fetched from the catalog.
    """
    def __init__(self, uri, fetcher_fn):
        """Initializes the _ResultSet.
        :param uri: the uri for the entity set in the catalog.
        :param fetcher_fn: a function that fetches the entities from the catalog.
        """
        assert fetcher_fn is not None
        self._fetcher_fn = fetcher_fn
        self._results_doc = None
        self._sort_keys = None
        self._limit = None
        self.uri = uri

    @property
    def _results(self):
        if self._results_doc is None:
            self.fetch()
        return self._results_doc

    def __len__(self):
        return len(self._results)

    def __getitem__(self, item):
        return self._results[item]

    def __iter__(self):
        return iter(self._results)

    def sort(self, *attributes):
        """Orders the results set by the given attributes.

        :param keys: Columns, column aliases, or aggregate function aliases. The sort attributes must be projected by
        the originating query.
        :return: self
        """
        if not attributes:
            raise ValueError("No sort attributes given.")
        if not all(isinstance(a, _ColumnWrapper) or isinstance(a, _ColumnAlias) or isinstance(a, _AggregateFunctionAlias)
                   or isinstance(a, _SortDescending) for a in attributes):
            raise TypeError("Sort keys must be column, column alias, or aggregate function alias")
        self._sort_keys = attributes
        self._results_doc = None
        return self

    def limit(self, n):
        """Set a limit on the number of results to be returned.

        :param n: integer or None.
        :return: self
        """
        try:
            self._limit = None if n is None else int(n)
            self._results_doc = None
            return self
        except ValueError:
            raise ValueError('limit argument "n" must be an integer or None')

    def fetch(self, limit=None, headers=DEFAULT_HEADERS):
        """Fetches the results from the catalog.

        :param limit: maximum number of results to fetch from the catalog.
        :param headers: headers to send in request to server
        :return: self
        """
        limit = int(limit) if limit else self._limit
        self._results_doc = self._fetcher_fn(limit, self._sort_keys, headers)
        logger.debug("Fetched %d entities" % len(self._results_doc))
        return self

def _json_size_approx(data):
    """Return approximate byte count for minimal JSON encoding of data

    Minimal encoding has no optional whitespace/indentation.
    """
    nbytes = 0

    if isinstance(data, (list, tuple)):
        nbytes += 2
        for elem in data:
            nbytes += _json_size_approx(elem) + 1
    elif isinstance(data, dict):
        nbytes += 2
        for k, v in data.items():
            nbytes += _json_size_approx(k) + _json_size_approx(v) + 2
    elif isinstance(data, str):
        nbytes += len(data.encode("utf-8")) + 2
    else:
        nbytes += len(str(data))

    return nbytes

def _generate_batches(entities, max_batch_rows=1000, max_batch_bytes=250*1024):
    """Generate a series of entity batches as slices of the input entities

    """
    if not isinstance(entities, (list, tuple)):
        raise TypeError('invalid type %s for entities, list or tuple expected' % (type(entities),))

    if not max_batch_rows:
        logger.debug("disabling batching due to max_batch_rows=%r" % (max_batch_rows,))
        return entities

    top = len(entities)
    lower = 0

    while lower < top:
        # to ensure progress, always use at least one row per batch regardless of nbytes
        upper = lower + 1
        batch_nbytes = _json_size_approx(entities[lower])

        # advance upper position until a batch size limit is reached
        while (upper - lower) < max_batch_rows:
            if upper >= top:
                break
            batch_nbytes += _json_size_approx(entities[upper])
            if batch_nbytes > max_batch_bytes:
                break
            upper += 1

        # generate one batch and advance for next batch
        logger.debug("yielding batch of %d/%d entities (%d:%d)" % (upper-lower, top, lower, upper))
        yield entities[lower:upper]
        lower = upper

def _request_with_retry(request_func, retry_codes={408, 429, 500, 502, 503, 504}, backoff_factor=4, max_attempts=5):
    """Perform request func with exponential backoff and retry.

    :param request_func: A function returning a requests.Response object or raising HTTPError
    :param retry_codes: HTTPError status codes on which to attempt retry
    :param backoff_factor: Base number of seconds for factor**attempt exponential backoff
    :param max_attempts: Max number of request attempts.

    Retry will be attempted on HTTPError exceptions which match retry_codes and
    also on other unknown exceptions, presumed to be transport errors.

    The request_func should do the equivalent of resp.raise_on_status() so that
    it only returns a response object for successful requests.
    """
    attempt = 0
    last_ex = None

    while attempt < max_attempts:
        try:
            if attempt > 0:
                delay = backoff_factor**(attempt-1)
                logger.debug("sleeping %d seconds before retry %d..." % (delay, attempt))
                time.sleep(delay)
            attempt += 1
            return request_func()
        except HTTPError as e:
            logger.debug(e.response.text)
            last_ex = e
            if 400 <= e.response.status_code < 500:
                last_ex = DataPathException(_http_error_message(e), e)
            if int(e.response.status_code) not in retry_codes:
                raise last_ex
        except Exception as e:
            logger.debug(e.response.text)
            last_ex = e

    # early return means we don't get here on successful requests
    logger.warning("maximum request retry limit %d exceeded" % (max_attempts,))
    if last_ex is None:
        raise ValueError('exceeded max_attempts without catching a request exception')
    raise last_ex

class _TableWrapper (object):
    """Wraps a Table for datapath expressions.
    """
    def __init__(self, schema, table):
        """Creates a _TableWrapper object.

        :param schema: the schema objec to which this table belongs
        :param table: the wrapped table
        """
        self._schema = schema
        self._wrapped_table = table
        self._name = table.name
        self._uname = urlquote(table.name)
        self._fqname = "%s:%s" % (urlquote(self._schema._name), self._uname)
        self._instancename = '*'
        self._projection_name = self._instancename
        self._fromname = self._fqname
        self.column_definitions = {
            v.name: _ColumnWrapper(self, v)
            for v in table.column_definitions
        }
        self._identifiers = _make_identifier_to_name_mapping(
            self.column_definitions.keys(),
            super(_TableWrapper, self).__dir__())

    def __dir__(self):
        return itertools.chain(
            super(_TableWrapper, self).__dir__(),
            self._identifiers.keys()
        )

    def __getattr__(self, a):
        if a in self._identifiers:
            return self.column_definitions[self._identifiers[a]]
        elif hasattr(super(_TableWrapper, self), a):
            return getattr(super(_TableWrapper, self), a)
        else:
            raise AttributeError("'%s' object for table '%s' has no attribute or column '%s'" % (type(self).__name__, self._wrapped_table.name, a))

    @deprecated
    def describe(self):
        """Provides a description of the model element.

        :return: a user-friendly string representation of the model element.
        """
        s = "_TableWrapper name: '%s'\nList of columns:\n" % self._name
        if len(self.column_definitions) == 0:
            s += "none"
        else:
            s += "\n".join("  %s" % col._name for col in self.column_definitions.values())
        return s

    @deprecated
    def _repr_html_(self):
        return self.describe()

    @property
    def columns(self):
        """Sugared access to self.column_definitions"""
        return self.column_definitions

    @property
    def path(self):
        """Always a new DataPath instance that is rooted at this table.

        Note that this table will be automatically aliased using its own table name.
        """
        return DataPath(self.alias(self._name))

    @property
    def _contextualized_path(self):
        """Returns the path as contextualized for this table instance.

        Conditionally updates the context of the path to which this table instance is bound.
        """
        return self.path

    @property
    @deprecated
    def uri(self):
        return self.path.uri

    def alias(self, alias_name):
        """Returns a table alias object.
        :param alias_name: a string to use as the alias name
        """
        return _TableAlias(self, alias_name)

    def filter(self, filter_expression):
        """See the docs for this method in `DataPath` for more information."""
        return self._contextualized_path.filter(filter_expression)

    def link(self, right, on=None, join_type=''):
        """See the docs for this method in `DataPath` for more information."""
        return self._contextualized_path.link(right, on, join_type)

    def _query(self, mode='entity', projection=[], group_key=[], context=None):
        """Invokes query on the path for this table."""
        return self.path._query(mode, projection, group_key=group_key, context=context)

    def entities(self):
        """Returns a results set of whole entities from this data path's current context.

        See the docs for this method in `DataPath` for more information.
        """
        return self._query()

    def aggregates(self, *functions):
        """Returns a results set of computed aggregates from this data path.

        See the docs for this method in `DataPath` for more information.
        """
        return self._query(mode=_Project.AGGREGATE, projection=list(functions))

    def attributes(self, *attributes):
        """Returns a results set of attributes projected and optionally renamed from this data path.

        See the docs for this method in `DataPath` for more information.
        """
        return self._query(mode=_Project.ATTRIBUTE, projection=list(attributes))

    def groupby(self, *keys):
        """Returns an attribute group object.

        See the docs for this method in `DataPath` for more information.
        """
        return _AttributeGroup(self, self._query, keys)

    def denormalize(self, context_name=None, heuristic=None, groupkey_name='RID'):
        """Denormalizes a path based on a visible-columns annotation 'context' or a heuristic approach.

        This method does not mutate this object. It returns a result set representing the denormalization of the path.

        :param context_name: name of the visible-columns context or if none given, will attempt apply heuristics
        :param heuristic: heuristic to apply if no context name specified
        :param groupkey_name: column name for the group by key of the generated query expression (default: 'RID')
        :return: a results set.
        """
        return self.path.denormalize(context_name=context_name, heuristic=heuristic, groupkey_name=groupkey_name)

    def insert(self, entities, defaults=set(), nondefaults=set(), add_system_defaults=True, on_conflict_skip=False, retry_codes={408, 429, 500, 502, 503, 504}, backoff_factor=4, max_attempts=5, max_batch_rows=1000, max_batch_bytes=250*1024):
        """Inserts entities into the table.

        :param entities: an iterable collection of entities (i.e., rows) to be inserted into the table.
        :param defaults: optional, set of column names to be assigned the default expression value.
        :param nondefaults: optional, set of columns names to override implicit system defaults
        :param add_system_defaults: flag to add system columns to the set of default columns.
        :param on_conflict_skip: flag to skip entities that violate uniqueness constraints.
        :param retry_codes: set of HTTP status codes for which retry should be considered.
        :param backoff_factor: number of seconds for base of exponential retry backoff.
        :param max_attempts: maximum number of requests attempts with retry.
        :param max_batch_rows: maximum number of rows for one request, or False to disable batching.
        :param max_batch_bytes: approximate maximum number of bytes for one request.
        :return a collection of newly created entities.

        Retry will only be attempted for idempotent insertion
        requests, which are when a user-controlled, non-nullable key
        is present in the table and the key's constituent column(s)
        are not listed as defaults, and on_conflict_skip=True.

        When performing retries, an exponential backoff delay is
        introduced after each failed attempt. The delay is
        backoff_factor**attempt_number seconds for attempts 0 through
        max_attempts-1.

        """
        # empty entities will be accepted but results are therefore an empty entity set
        if not entities:
            return _ResultSet(self.path.uri, lambda ignore1, ignore2, ignore3: [])

        options = []

        if on_conflict_skip:
            options.append('onconflict=skip')

        if defaults or add_system_defaults:
            defaults_enc = {urlquote(cname) for cname in defaults}
            if add_system_defaults:
                defaults_enc |= _system_defaults - nondefaults
            options.append("defaults={cols}".format(cols=','.join(defaults_enc)))

        if nondefaults:
            nondefaults_enc = {urlquote(cname) for cname in nondefaults}
            options.append("nondefaults={cols}".format(cols=','.join(nondefaults_enc)))

        path = '/entity/' + self._fqname
        if options:
            path += "?" + "&".join(options)
        logger.debug("Inserting entities to path: {path}".format(path=path))

        # JSONEncoder does not handle general iterable objects, so we have to make sure its an acceptable collection
        if not hasattr(entities, '__iter__'):
            raise TypeError('entities is not iterable')
        entities = entities if isinstance(entities, (list, tuple)) else list(entities)

        # test the first entity element to make sure that it looks like a dictionary
        if not hasattr(entities[0], 'keys'):
            raise TypeError('entities[0] does not look like a dictionary -- does not have a "keys()" method')

        # perform one batch request in a helper we can hand to retry helper
        def request_func(batch):
            return self._schema._catalog._wrapped_catalog.post(path, json=batch, headers={'Content-Type': 'application/json'})

        def _has_user_pkey(table):
            """Return True if table has at least one primary key other than the system RID key"""
            for key in table.keys:
                if { c.name for c in key.unique_columns } != {'RID'}:
                    if all([ not c.nullok for c in key.unique_columns ]) \
                       and all([ c.name not in defaults for c in key.unique_columns ]):
                        return True
            return False

        # determine whether insert is idempotent and therefore retry safe
        retry_safe = on_conflict_skip and _has_user_pkey(self._wrapped_table)

        # perform all requests synchronously so the caller can get exceptions
        results = []
        for batch in _generate_batches(
            entities,
            max_batch_rows=max_batch_rows,
            max_batch_bytes=max_batch_bytes
        ):
            try:
                if retry_safe:
                    resp = _request_with_retry(
                        lambda: request_func(batch),
                        retry_codes=retry_codes,
                        backoff_factor=backoff_factor,
                        max_attempts=max_attempts
                    )
                else:
                    resp = request_func(batch)
                results.extend(resp.json())
            except HTTPError as e:
                logger.debug(e.response.text)
                if 400 <= e.response.status_code < 500:
                    raise DataPathException(_http_error_message(e), e)
                else:
                    raise e

        result = _ResultSet(self.path.uri, lambda ignore1, ignore2, ignore3: results)
        return result


    def update(self, entities, correlation={'RID'}, targets=None, retry_codes={408, 429, 500, 502, 503, 504}, backoff_factor=4, max_attempts=5, max_batch_rows=1000, max_batch_bytes=250*1024):
        """Update entities of a table.

        For more information see the ERMrest protocol for the `attributegroup` interface. By default, this method will
        correlate the input data (entities) based on the `RID` column of the table. By default, the method will use all
        column names found in the first row of the `entities` input, which are not found in the `correlation` set and
        not defined as 'system columns' by ERMrest, as the targets if `targets` is not set.

        :param entities: an iterable collection of entities (i.e., rows) to be updated in the table.
        :param correlation: an iterable collection of column names used to correlate input set to the set of rows to be
        updated in the catalog. E.g., `{'col name'}` or `{mytable.mycolumn}` will work if you pass a _ColumnWrapper object.
        :param targets: an iterable collection of column names used as the targets of the update operation.
        :param retry_codes: set of HTTP status codes for which retry should be considered.
        :param backoff_factor: number of seconds for base of exponential retry backoff.
        :param max_attempts: maximum number of requests attempts with retry.
        :param max_batch_rows: maximum number of rows for one request, or False to disable batching.
        :param max_batch_bytes: approximate maximum number of bytes for one request.
        :return a collection of newly created entities.

        When performing retries, an exponential backoff delay is
        introduced after each failed attempt. The delay is
        backoff_factor**attempt_number seconds for attempts 0 through
        max_attempts-1.
        """
        # empty entities will be accepted but results are therefore an empty entity set
        if not entities:
            return _ResultSet(self.path.uri, lambda ignore1, ignore2, ignore3: [])

        # JSONEncoder does not handle general iterable objects, so we have to make sure its an acceptable collection
        if not hasattr(entities, '__iter__'):
            raise TypeError('entities is not iterable')
        entities = entities if isinstance(entities, (list, tuple)) else list(entities)

        # test the first entity element to make sure that it looks like a dictionary
        if not hasattr(entities[0], 'keys'):
            raise TypeError('entities[0] does not look like a dictionary -- does not have a "keys()" method')

        # Form the correlation keys and the targets
        correlation_cnames = {urlquote(str(c)) for c in correlation}
        if targets:
            target_cnames = {urlquote(str(t)) for t in targets}
        else:
            exclusions = correlation_cnames | _system_defaults
            target_cnames = {urlquote(str(t)) for t in entities[0].keys() if urlquote(str(t)) not in exclusions}

        # test if there are any targets after excluding for correlation keys and system columns
        if not target_cnames:
            raise ValueError('No "targets" for the update. There must be at least one column as a target of the update,'
                             ' and targets cannot overlap with "correlation" keys and system columns.')

        # Form the path
        path = '/attributegroup/{table}/{correlation};{targets}'.format(
            table=self._fqname,
            correlation=','.join(correlation_cnames),
            targets=','.join(target_cnames)
        )

        # perform one batch request in a helper we can hand to retry helper
        def request_func(batch):
            return self._schema._catalog._wrapped_catalog.put(path, json=batch, headers={'Content-Type': 'application/json'})

        # perform all requests synchronously so the caller can get exceptions
        results = []
        for batch in _generate_batches(
            entities,
            max_batch_rows=max_batch_rows,
            max_batch_bytes=max_batch_bytes
        ):
            try:
                resp = _request_with_retry(
                    lambda: request_func(batch),
                    retry_codes=retry_codes,
                    backoff_factor=backoff_factor,
                    max_attempts=max_attempts
                )
                results.extend(resp.json())
            except HTTPError as e:
                logger.debug(e.response.text)
                if 400 <= e.response.status_code < 500:
                    raise DataPathException(_http_error_message(e), e)
                else:
                    raise e

        result = _ResultSet(self.path.uri, lambda ignore1, ignore2, ignore3: results)
        return result

    def delete(self):
        """Deletes the entity set referenced by the Table.
        """
        self.path.delete()


class _TableAlias (_TableWrapper):
    """Represents a table alias in datapath expressions.
    """
    def __init__(self, base_table, alias_name):
        """Initializes the table alias.

        :param base_table: the base table to be given an alias name
        :param alias_name: the alias name
        """
        assert isinstance(base_table, _TableWrapper)
        super(_TableAlias, self).__init__(base_table._schema, base_table._wrapped_table)
        self._parent = None
        self._base_table = base_table
        self._name = alias_name
        self._uname = urlquote(alias_name)
        self._fqname = self._base_table._fqname
        self._instancename = self._uname + ":*"
        self._projection_name = self._instancename
        self._fromname = "%s:=%s" % (self._uname, self._base_table._fqname)

    def __deepcopy__(self, memodict={}):
        # deep copy implementation of a table alias should not make copies of model objects (ie, the base table)
        return _TableAlias(self._base_table, self._name)

    def _equivalent(self, alias):
        """Equivalence comparison between table aliases.

        :param alias: another table alias
        :return: True, if the base table and alias name match, else False
        """
        if not isinstance(alias, _TableAlias):
            raise TypeError("'alias' must be an instance of '%s'" % type(self).__name__)
        return self._name == alias._name and self._base_table == alias._base_table

    @property
    def path(self):
        """Returns the parent path for this alias.
        """
        if not self._parent:
            self._parent = DataPath(self)
        return self._parent

    def _bind(self, parent_path):
        """Binds this table instance to the given parent path."""
        if self._parent:
            raise ValueError("Cannot bind a table instance that has already been bound.")
        elif not isinstance(parent_path, DataPath):
            raise TypeError("value must be a DataPath instance.")
        self._parent = parent_path

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
    @deprecated
    def uri(self):
        return self.path._contextualized_uri(self)

    def _query(self, mode='entity', projection=[], group_key=[], context=None):
        """Overridden method to set context of query to this table instance."""
        return self.path._query(mode, projection, group_key=group_key, context=self)


class _ColumnWrapper (object):
    """Wraps a Column for datapath expressions.
    """

    def __init__(self, table, column):
        """Creates a _ColumnWrapper object.

        :param table: the table to which this column belongs
        :param column: the wrapped column
        """
        super(_ColumnWrapper, self).__init__()
        self._table = table
        self._wrapped_column = column
        self._name = column.name
        self._uname = urlquote(self._name)

    @property
    def _fqname(self):
        """Late binding needed for table alias instances."""
        return "%s:%s" % (self._table._fqname, self._uname)

    @property
    def _instancename(self):
        """Late binding needed for table alias instances."""
        return "%s:%s" % (self._table._uname, self._uname) if isinstance(self._table, _TableAlias) else self._uname

    @property
    def _projection_name(self):
        """Late binding needed for table alias instances."""
        return self._instancename

    @deprecated
    def describe(self):
        """Provides a description of the model element.

        :return: a user-friendly string representation of the model element.
        """
        return "_ColumnWrapper name: '%s'\tType: %s\tComment: '%s'" % \
               (self._name, self._wrapped_column.type.typename, self._wrapped_column.comment)

    @deprecated
    def _repr_html_(self):
        return self.describe()

    @property
    def desc(self):
        """A descending sort modifier based on this column."""
        return _SortDescending(self)

    def __str__(self):
        return self._name

    def eq(self, other):
        """Returns an 'equality' comparison predicate.

        :param other: `None` or any other literal value.
        :return: a filter predicate object
        """
        if other is None:
            return _ComparisonPredicate(self, "::null::", '')
        else:
            return _ComparisonPredicate(self, "=", other)

    __eq__ = eq

    def lt(self, other):
        """Returns a 'less than' comparison predicate.

        :param other: a literal value.
        :return: a filter predicate object
        """
        return _ComparisonPredicate(self, "::lt::", other)

    __lt__ = lt

    def le(self, other):
        """Returns a 'less than or equal' comparison predicate.

        :param other: a literal value.
        :return: a filter predicate object
        """
        return _ComparisonPredicate(self, "::leq::", other)

    __le__ = le

    def gt(self, other):
        """Returns a 'greater than' comparison predicate.

        :param other: a literal value.
        :return: a filter predicate object
        """
        return _ComparisonPredicate(self, "::gt::", other)

    __gt__ = gt

    def ge(self, other):
        """Returns a 'greater than or equal' comparison predicate.

        :param other: a literal value.
        :return: a filter predicate object
        """
        return _ComparisonPredicate(self, "::geq::", other)

    __ge__ = ge

    def regexp(self, other):
        """Returns a 'regular expression' comparison predicate.

        :param other: a _string_ literal value.
        :return: a filter predicate object
        """
        if not isinstance(other, str):
            logger.warning("'regexp' method comparison only supports string literals.")
        return _ComparisonPredicate(self, "::regexp::", other)

    def ciregexp(self, other):
        """Returns a 'case-insensitive regular expression' comparison predicate.

        :param other: a _string_ literal value.
        :return: a filter predicate object
        """
        if not isinstance(other, str):
            logger.warning("'ciregexp' method comparison only supports string literals.")
        return _ComparisonPredicate(self, "::ciregexp::", other)

    def ts(self, other):
        """Returns a 'text search' comparison predicate.

        :param other: a _string_ literal value.
        :return: a filter predicate object
        """
        if not isinstance(other, str):
            logger.warning("'ts' method comparison only supports string literals.")
        return _ComparisonPredicate(self, "::ts::", other)

    def alias(self, name):
        """Returns an alias for this column."""
        return _ColumnAlias(self, name)


class _ColumnAlias (object):
    """Represents an (output) alias for a column instance in a datapath expression.
    """
    def __init__(self, base_column, alias_name):
        """Initializes the column alias.

        :param base_column: the base column to be given an alias name
        :param alias_name: the alias name
        """
        assert isinstance(base_column, _ColumnWrapper)
        super(_ColumnAlias, self).__init__()
        self._name = alias_name
        self._base_column = base_column
        self._uname = urlquote(self._name)

    @property
    def _projection_name(self):
        """Late binding needed for table alias instances."""
        return "%s:=%s" % (self._uname, self._base_column._instancename)

    def __deepcopy__(self, memodict={}):
        # deep copy implementation of a column alias should not make copies of model objects (ie, the base column)
        return _ColumnAlias(self._base_column, self._name)

    @deprecated
    def describe(self):
        """Provides a description of the model element.

        :return: a user-friendly string representation of the model element.
        """
        return "_ColumnWrapper name: '%s'\tAlias for: %s" % \
               (self._name, self._base_column.describe())

    @deprecated
    def _repr_html_(self):
        return self.describe()

    @property
    def desc(self):
        """A descending sort modifier based on this column."""
        return _SortDescending(self)

    def __str__(self):
        return self._name

    def eq(self, other):
        """Returns an 'equality' comparison predicate.

        :param other: `None` or any other literal value.
        :return: a filter predicate object
        """
        return self._base_column.eq(other)

    __eq__ = eq

    def lt(self, other):
        """Returns a 'less than' comparison predicate.

        :param other: a literal value.
        :return: a filter predicate object
        """
        return self._base_column.lt(other)

    __lt__ = lt

    def le(self, other):
        """Returns a 'less than or equal' comparison predicate.

        :param other: a literal value.
        :return: a filter predicate object
        """
        return self._base_column.le(other)

    __le__ = le

    def gt(self, other):
        """Returns a 'greater than' comparison predicate.

        :param other: a literal value.
        :return: a filter predicate object
        """
        return self._base_column.gt(other)

    __gt__ = gt

    def ge(self, other):
        """Returns a 'greater than or equal' comparison predicate.

        :param other: a literal value.
        :return: a filter predicate object
        """
        return self._base_column.ge(other)

    __ge__ = ge

    def regexp(self, other):
        """Returns a 'regular expression' comparison predicate.

        :param other: a _string_ literal value.
        :return: a filter predicate object
        """
        return self._base_column.regexp(other)

    def ciregexp(self, other):
        """Returns a 'case-insensitive regular expression' comparison predicate.

        :param other: a _string_ literal value.
        :return: a filter predicate object
        """
        return self._base_column.ciregexp(other)

    def ts(self, other):
        """Returns a 'text search' comparison predicate.

        :param other: a _string_ literal value.
        :return: a filter predicate object
        """
        return self._base_column.ts(other)


class _SortDescending (object):
    """A descending sort condition."""

    def __init__(self, attr):
        """Creates sort descending object.

        :param attr: a column, column alias, or aggrfn alias object
        """
        assert isinstance(attr, _ColumnWrapper) or isinstance(attr, _ColumnAlias) or isinstance(attr, _AggregateFunctionAlias)
        self._attr = attr
        self._uname = urlquote(self._attr._uname) + "::desc::"


class _PathOperator (object):
    def __init__(self, r):
        assert isinstance(r, _PathOperator) or isinstance(r, _TableAlias)
        if isinstance(r, _Project):
            raise Exception("Cannot extend a path after an attribute projection")
        self._r = r

    def __deepcopy__(self, memodict={}):
        return type(self)(copy.deepcopy(self._r, memo=memodict))

    @property
    def _path(self):
        assert isinstance(self._r, _PathOperator)
        return self._r._path

    @property
    def _mode(self):
        assert isinstance(self._r, _PathOperator)
        return self._r._mode

    def __str__(self):
        return "/%s/%s" % (self._mode, self._path)

    def rebase(self, base, root_context):
        """Rebases the current path expression to begin as a reset context following 'base'.

        :param base: a valid path expresion
        :param root_context: root context on which to rebase this path expression
        :return: rebased expresion _or_ a new `_ResetContext` instance if `self` was the root
        """
        assert isinstance(base, _PathOperator)
        assert isinstance(root_context, _TableAlias)
        if isinstance(self, _Root):
            return _ResetContext(base, self._table)
        else:
            pathobj = self
            while not isinstance(pathobj._r, _Root):
                pathobj = self._r
            assert root_context._equivalent(pathobj._r._table)
            pathobj._r = _ResetContext(base, root_context)
            return self


class _Root (_PathOperator):
    def __init__(self, r):
        super(_Root, self).__init__(r)
        assert isinstance(r, _TableAlias)
        self._table = r

    @property
    def _path(self):
        return self._table._fromname

    @property
    def _mode(self):
        return 'entity'


class _ResetContext (_PathOperator):
    def __init__(self, r, alias):
        if isinstance(r, _ResetContext):
            r = r._r  # discard the previous context reset operator
        super(_ResetContext, self).__init__(r)
        assert isinstance(alias, _TableAlias)
        self._alias = alias

    def __deepcopy__(self, memodict={}):
        return _ResetContext(copy.deepcopy(self._r, memo=memodict), copy.deepcopy(self._alias, memo=memodict))

    @property
    def _path(self):
        assert isinstance(self._r, _PathOperator)
        return "%s/$%s" % (self._r._path, self._alias._uname)


class _Filter(_PathOperator):
    def __init__(self, r, formula):
        super(_Filter, self).__init__(r)
        assert isinstance(formula, _Predicate)
        self._formula = formula

    def __deepcopy__(self, memodict={}):
        return _Filter(copy.deepcopy(self._r, memo=memodict), copy.deepcopy(self._formula, memo=memodict))

    @property
    def _path(self):
        assert isinstance(self._r, _PathOperator)
        return "%s/%s" % (self._r._path, str(self._formula))


class _Project (_PathOperator):
    """Projection path component."""

    ENTITY = 'entity'
    ATTRIBUTE = 'attribute'
    AGGREGATE = 'aggregate'
    ATTRGROUP = 'attributegroup'
    MODES = (ENTITY, ATTRIBUTE, AGGREGATE, ATTRGROUP)

    def __init__(self, r, mode=ENTITY, projection=[], group_key=[]):
        """Initializes the projection component.

        :param r: the parent path component.
        :param r: the 'mode' of the projection (entity, attribute, etc.)
        :param projection: projection list.
        :param group_key: grouping keys list.
        """
        super(_Project, self).__init__(r)
        assert mode in self.MODES
        assert mode == self.ENTITY or mode == self.ATTRGROUP or len(projection) > 0
        assert mode != self.ATTRGROUP or len(group_key) > 0
        self._projection_mode = mode
        self._projection = []
        self._group_key = []

        if mode == self.ATTRIBUTE:
            if not all(isinstance(obj, _TableWrapper) or isinstance(obj, _TableAlias) or isinstance(obj, _ColumnWrapper) or isinstance(obj, _ColumnAlias) for obj in projection):
                raise TypeError("Only columns or column aliases can be retrieved by an 'attribute' query.")
        elif mode == self.AGGREGATE:
            if not all(isinstance(obj, _AggregateFunctionAlias) for obj in projection):
                raise TypeError("Only aggregate function aliases can be retrieved by an 'aggregate' query.")
        elif mode == self.ATTRGROUP:
            if not all(isinstance(obj, _ColumnWrapper) or isinstance(obj, _ColumnAlias) or isinstance(obj, _AggregateFunctionAlias) for obj in projection):
                raise TypeError("Only columns, column aliases, or aggregate function aliases can be retrieved by an 'attributegroup' query.")
            if not all(isinstance(obj, _ColumnWrapper) or isinstance(obj, _ColumnAlias) or isinstance(obj, _AggregateFunctionAlias) for obj in group_key):
                raise TypeError("Only column aliases or aggregate function aliases can be used to group an 'attributegroup' query.")
            self._group_key = [obj._projection_name for obj in group_key]

        self._projection = [obj._projection_name for obj in projection]

    def __deepcopy__(self, memodict={}):
        cp = super(_Project, self).__deepcopy__(memodict=memodict)
        cp._projection_mode = self._projection_mode
        cp._projection = copy.deepcopy(self._projection, memo=memodict)
        cp._group_key = copy.deepcopy(self._group_key, memo=memodict)
        return cp

    @property
    def _path(self):
        assert isinstance(self._r, _PathOperator)
        projection = ','.join(self._projection)
        if self._projection_mode == self.ATTRGROUP:
            assert self._group_key
            grouping = ','.join(self._group_key)
            return "%s/%s;%s" % (self._r._path, grouping, projection)
        else:
            return "%s/%s" % (self._r._path, projection)

    @property
    def _mode(self):
        return self._projection_mode


class _Link (_PathOperator):
    def __init__(self, r, on, as_=None, join_type=''):
        """Initialize the _Link operator

        :param r: parent path operator
        :param on: a table alias, a comparison predicate, or a conjunction of comparisons
        :param as_: table alias
        :param join_type: left, right or full for outer join semantics, or '' for inner join semantics
        """
        super(_Link, self).__init__(r)
        assert isinstance(on, _ComparisonPredicate) or isinstance(on, _TableAlias) or (
                isinstance(on, _ConjunctionPredicate) and on.is_valid_join_condition), "Invalid join 'on' clause"
        assert as_ is None or isinstance(as_, _TableAlias)
        assert join_type == '' or (join_type in ('left', 'right', 'full') and isinstance(on, _Predicate))
        self._on = on
        self._as = as_
        self._join_type = join_type

    def __deepcopy__(self, memodict={}):
        return _Link(
            copy.deepcopy(self._r, memo=memodict),
            copy.deepcopy(self._on, memo=memodict),
            as_=copy.deepcopy(self._as, memo=memodict),
            join_type=self._join_type
        )

    @property
    def _path(self):
        assert isinstance(self._r, _PathOperator)
        assign = '' if self._as is None else "%s:=" % self._as._uname
        if isinstance(self._on, _TableWrapper):
            cond = self._on._fqname
        elif isinstance(self._on, _ComparisonPredicate):
            cond = str(self._on)
        elif isinstance(self._on, _ConjunctionPredicate):
            cond = self._on.as_join_condition
        else:
            raise DataPathException("Invalid join condition: " + str(self._on))
        return "%s/%s%s%s" % (self._r._path, assign, self._join_type, cond)


class _Predicate (object):
    """Common base class for all predicate types."""

    def and_(self, other):
        """Returns a conjunction predicate.

        :param other: a predicate object.
        :return: a junction predicate object.
        """
        if not isinstance(other, _Predicate):
            raise TypeError("Invalid comparison with object that is not a _Predicate instance.")
        return _ConjunctionPredicate([self, other])

    __and__ = and_

    def or_(self, other):
        """Returns a disjunction predicate.

        :param other: a predicate object.
        :return: a junction predicate object.
        """
        if not isinstance(other, _Predicate):
            raise TypeError("Invalid comparison with object that is not a _Predicate instance.")
        return _DisjunctionPredicate([self, other])

    __or__ = or_

    def negate(self):
        """Returns a negation predicate.

        This predicate is wrapped in a negation predicate which is returned to the caller.

        :return: a negation predicate object.
        """
        return _NegationPredicate(self)

    __invert__ = negate


class _ComparisonPredicate (_Predicate):
    """Comparison (left-operand operator right-operand) predicate"""
    def __init__(self, lop, op, rop):
        super(_ComparisonPredicate, self).__init__()
        assert isinstance(lop, _ColumnWrapper)
        assert isinstance(rop, _ColumnWrapper) or isinstance(rop, int) or \
               isinstance(rop, float) or isinstance(rop, str) or \
               isinstance(rop, date)
        assert isinstance(op, str)
        self._lop = lop
        self._op = op
        self._rop = rop

    def __deepcopy__(self, memodict={}):
        # deep copy of predicate should not deep copy the model object references (i.e., _ColumnWrapper objects)
        return _ComparisonPredicate(self._lop, self._op, self._rop)

    @property
    def is_equality(self):
        return self._op == '='

    @property
    def left(self):
        return self._lop

    @property
    def right(self):
        return self._rop

    def __str__(self):
        if isinstance(self._rop, _ColumnWrapper):
            # The only valid circumstance for a _ColumnWrapper rop is in a link 'on' predicate for simple key/fkey joins
            return "(%s)=(%s)" % (self._lop._instancename, self._rop._fqname)
        else:
            # All other comparisons are serialized per the usual form
            return "%s%s%s" % (self._lop._instancename, self._op, urlquote(str(self._rop)))


class _JunctionPredicate (_Predicate):
    """Junction (and/or) of child predicates."""
    def __init__(self, op, operands):
        super(_JunctionPredicate, self).__init__()
        assert operands and hasattr(operands, '__iter__') and len(operands) > 1
        assert all(isinstance(operand, _Predicate) for operand in operands)
        assert isinstance(op, str)
        self._operands = operands
        self._op = op

    def __str__(self):
        return self._op.join(["(%s)" % operand for operand in self._operands])


class _ConjunctionPredicate (_JunctionPredicate):
    """Conjunction (and) or child predicates."""
    def __init__(self, operands):
        super(_ConjunctionPredicate, self).__init__('&', operands)

    def and_(self, other):
        return _ConjunctionPredicate(self._operands + [other])

    @property
    def is_valid_join_condition(self):
        """Tests if this conjunction is a valid join condition."""
        return all(isinstance(o, _ComparisonPredicate) and o.is_equality for o in self._operands)

    @property
    def as_join_condition(self):
        """Returns the conjunction in the 'join condition' serialized format."""
        lhs = []
        rhs = []

        for operand in self._operands:
            assert isinstance(operand, _ComparisonPredicate) and operand.is_equality
            assert isinstance(operand.left, _ColumnWrapper)
            assert isinstance(operand.right, _ColumnWrapper)
            lhs.append(operand.left)
            rhs.append(operand.right)

        return "({left})=({right})".format(
            left=",".join(lop._instancename for lop in lhs),
            right=",".join(rop._fqname for rop in rhs)
        )


class _DisjunctionPredicate (_JunctionPredicate):
    """Disjunction (or) of child predicates."""
    def __init__(self, operands):
        super(_DisjunctionPredicate, self).__init__(';', operands)

    def or_(self, other):
        return _DisjunctionPredicate(self._operands + [other])


class _NegationPredicate (_Predicate):
    """Negates the child predicate."""
    def __init__(self, child):
        super(_NegationPredicate, self).__init__()
        assert isinstance(child, _Predicate)
        self._child = child

    def __str__(self):
        return "!(%s)" % self._child


class AggregateFunction (object):
    """Base class of all aggregate functions."""
    def __init__(self, fn_name, arg):
        """Initializes the aggregate function.

        :param fn_name: name of the function per ERMrest specification.
        :param arg: argument of the function per ERMrest specification.
        """
        super(AggregateFunction, self).__init__()
        self._fn_name = fn_name
        self._arg = arg

    def __str__(self):
        return "%s(%s)" % (self._fn_name, self._arg)

    @property
    def _instancename(self):
        return "%s(%s)" % (self._fn_name, self._arg._instancename)

    def alias(self, alias_name):
        """Returns an (output) alias for this aggregate function instance."""
        return _AggregateFunctionAlias(self, alias_name)


class Min (AggregateFunction):
    """Aggregate function for minimum non-NULL value."""
    def __init__(self, arg):
        super(Min, self).__init__('min', arg)


class Max (AggregateFunction):
    """Aggregate function for maximum non-NULL value."""
    def __init__(self, arg):
        super(Max, self).__init__('max', arg)


class Sum (AggregateFunction):
    """Aggregate function for sum of non-NULL values."""
    def __init__(self, arg):
        super(Sum, self).__init__('sum', arg)


class Avg (AggregateFunction):
    """Aggregate function for average of non-NULL values."""
    def __init__(self, arg):
        super(Avg, self).__init__('avg', arg)


class Cnt (AggregateFunction):
    """Aggregate function for count of non-NULL values."""
    def __init__(self, arg):
        super(Cnt, self).__init__('cnt', arg)


class CntD (AggregateFunction):
    """Aggregate function for count of distinct non-NULL values."""
    def __init__(self, arg):
        super(CntD, self).__init__('cnt_d', arg)


class Array (AggregateFunction):
    """Aggregate function for an array containing all values (including NULL)."""
    def __init__(self, arg):
        super(Array, self).__init__('array', arg)


class ArrayD (AggregateFunction):
    """Aggregate function for an array containing distinct values (including NULL)."""
    def __init__(self, arg):
        super(ArrayD, self).__init__('array_d', arg)


class Bin (AggregateFunction):
    """Binning function."""
    def __init__(self, arg, nbins, minval=None, maxval=None):
        """Initialize the bin function.

        If `minval` or `maxval` are not given, they will be set based on the min and/or max values for the column
        (`operand` parameter) as determined by issuing an aggregate query over the current data path.

        :param arg: a column or aliased column instance
        :param nbins: number of bins
        :param minval: minimum value (optional)
        :param maxval: maximum value (optional)
        """
        super(Bin, self).__init__('bin', arg)
        if not (isinstance(arg, _ColumnWrapper) or isinstance(arg, _ColumnAlias)):
            raise TypeError("Bin argument must be a column or column alias")
        self.nbins = nbins
        self.minval = minval
        self.maxval = maxval

    def __str__(self):
        return "%s(%s;%s;%s;%s)" % (self._fn_name, self._arg, self.nbins, self.minval, self.maxval)

    @property
    def _instancename(self):
        return "%s(%s;%s;%s;%s)" % (self._fn_name, self._arg._instancename, self.nbins, self.minval, self.maxval)


class _AggregateFunctionAlias (object):
    """Alias for aggregate functions."""
    def __init__(self, fn, alias_name):
        """Initializes the aggregate function alias.

        :param fn: aggregate function instance
        :param alias_name: alias name
        """
        super(_AggregateFunctionAlias, self).__init__()
        assert isinstance(fn, AggregateFunction)
        self._fn = fn
        self._name = alias_name
        self._uname = urlquote(self._name)

    def __str__(self):
        return str(self._fn)

    @property
    def _projection_name(self):
        """In a projection, the object uses this name."""
        return "%s:=%s" % (self._uname, self._fn._instancename)

    @property
    def desc(self):
        """A descending sort modifier based on this alias."""
        return _SortDescending(self)


class _AttributeGroup (object):
    """A computed attribute group."""
    def __init__(self, source, queryfn, keys):
        """Initializes an attribute group instance.

        :param source: the source object for the group (DataPath, _TableWrapper, _TableAlias)
        :param queryfn: a query function that takes mode, projection, and group_key parameters
        :param keys: an iterable collection of group keys
        """
        super(_AttributeGroup, self).__init__()
        assert any(isinstance(source, valid_type) for valid_type in [DataPath, _TableWrapper, _TableAlias])
        assert isinstance(keys, tuple)
        if not keys:
            raise ValueError("No groupby keys.")
        self._source = source
        self._queryfn = queryfn
        self._grouping_keys = list(keys)

    def attributes(self, *attributes):
        """Returns a results set of attributes projected and optionally renamed from this group.

        :param attributes: the columns, aliased columns, and/or aliased aggregate functions to be retrieved for this group.
        :return: a results set of the projected attributes from this group.
        """
        self._resolve_binning_ranges()
        return self._queryfn(mode=_Project.ATTRGROUP, projection=list(attributes), group_key=self._grouping_keys)

    def _resolve_binning_ranges(self):
        """Helper method to resolve any unspecified binning ranges."""
        for key in self._grouping_keys:
            if isinstance(key, _AggregateFunctionAlias) and isinstance(key._fn, Bin):
                bin = key._fn
                aggrs = []
                if bin.minval is None:
                    aggrs.append(Min(bin._arg).alias('minval'))
                if bin.maxval is None:
                    aggrs.append(Max(bin._arg).alias('maxval'))
                if aggrs:
                    result = self._source.aggregates(*aggrs)[0]
                    bin.minval = result.get('minval', bin.minval)
                    bin.maxval = result.get('maxval', bin.maxval)
                    if (bin.minval is None) or (bin.maxval is None):
                        raise ValueError('Automatic determination of binning bounds failed.')

##
## UTILITIES FOR DENORMALIZATION ##############################################
##

def _datapath_left_outer_join_by_fkey(path, fk, alias_name=None):
    """Link a table to the path based on a foreign key reference.

    :param path: a DataPath object
    :param fk: an ermrest_model.ForeignKey object
    :param alias_name: an optional 'alias' name to use for the foreign table
    """
    assert isinstance(path, DataPath)
    assert isinstance(fk, _erm.ForeignKey)
    catalog = path._root._schema._catalog

    # determine 'direction' -- inbound or outbound
    path_context_table = path.context._base_table._wrapped_table
    if (path_context_table.schema.name, path_context_table.name) == (fk.table.schema.name, fk.table.name):
        right = catalog.schemas[fk.pk_table.schema.name].tables[fk.pk_table.name]
        fkcols = zip(fk.foreign_key_columns, fk.referenced_columns)
    elif (path_context_table.schema.name, path_context_table.name) == (fk.pk_table.schema.name, fk.pk_table.name):
        right = catalog.schemas[fk.table.schema.name].tables[fk.table.name]
        fkcols = zip(fk.referenced_columns, fk.foreign_key_columns)
    else:
        raise ValueError('Context table "%s" not referenced by foreign key "%s"' % (path_context_table.name, fk.constraint_name))

    # compose join condition
    on = None
    for lcol, rcol in fkcols:
        lcol = catalog.schemas[lcol.table.schema.name].tables[lcol.table.name].columns[lcol.name]
        rcol = catalog.schemas[rcol.table.schema.name].tables[rcol.table.name].columns[rcol.name]
        if on:
            on = on & (lcol == rcol)
        else:
            on = lcol == rcol

    # link
    path.link(right.alias(alias_name) if alias_name else right, on=on, join_type='left')


def _datapath_deserialize_vizcolumn(path, vizcol, sources=None):
    """Deserializes a visual column specification.

    If the visible column specifies a foreign key path, the datapath object
    will be changed by linking the foreign keys in the path.

    :param path: a datapath object
    :param vizcol: a visible column specification
    :return: the element to be projected from the datapath or None
    """
    assert isinstance(path, DataPath)
    sources = sources if sources else {}
    context = path.context
    table = context._wrapped_table
    model = table.schema.model

    if isinstance(vizcol, str):
        # column name specification
        return context.columns[vizcol]
    elif isinstance(vizcol, list):
        # constraint specification
        try:
            fk = model.fkey(vizcol)
            _datapath_left_outer_join_by_fkey(path, fk, alias_name='F')
            return ArrayD(path.context).alias(path.context._name)  # project all attributes
        except KeyError as e:
            raise ValueError('Invalid foreign key constraint name: %s. If this is a key constraint name, note that keys are not supported at this time.' % str(e))
    elif isinstance(vizcol, dict):
        # resolve visible column
        while 'sourcekey' in vizcol:
            temp = sources.get(vizcol['sourcekey'], {})
            if temp == vizcol:
                raise ValueError('Visible column self reference for sourcekey "%s"' % vizcol['sourcekey'])
            vizcol = temp
        # deserialize source definition
        source = vizcol.get('source')
        if not source:
            # case: none
            raise ValueError('Could not resolve source definition for visible column')
        elif isinstance(source, str):
            # case: column name
            return context.columns[source]
        elif isinstance(source, list):
            # case: path expression
            # ...validate syntax
            if not all(isinstance(obj, dict) for obj in source[:-1]):
                raise ValueError('Source path element must be a foreign key dict')
            if not isinstance(source[-1], str):
                raise ValueError('Source path must terminate in a column name string')
            # link path elements by fkey; and track whether path is outbound only fkeys
            outbound_only = True
            for path_elem in source[:-1]:
                try:
                    fk = model.fkey(path_elem.get('inbound', path_elem.get('outbound')))
                    _datapath_left_outer_join_by_fkey(path, fk, alias_name='F')
                    outbound_only = outbound_only and 'outbound' in path_elem
                except KeyError as e:
                    raise ValueError('Invalid foreign key constraint name: %s' % str(e))
            # return terminating column or entity
            # ...get terminal name
            terminal = source[-1]
            # ...get alias name
            alias = vizcol.get('markdown_name', vizcol.get('name', path.context._name + '_' + terminal))
            # ...get aggregate function
            aggregate = {
                'min': Min,
                'max': Max,
                'cnt': Cnt,
                'cnd_d': CntD,
                'array': Array,
                'array_d': ArrayD
            }.get(vizcol.get('aggregate'), ArrayD)
            # ...determine projection mode
            if vizcol.get('entity', True):
                # case: whole entities
                return aggregate(path.context).alias(alias)
            else:
                # case: specified attribute value(s)
                if outbound_only:
                    # for outbound only paths, we can project a single value
                    return path.context.columns[terminal].alias(alias)
                else:
                    # otherwise, we need to use aggregate the values
                    return aggregate(path.context.columns[terminal]).alias(alias)
        else:
            raise ValueError('Malformed source: %s' % str(source))
    else:
        raise ValueError('Malformed visible column: %s' % str(vizcol))


def _datapath_contextualize(path, context_name='*', context_body=None, groupkey_name='RID'):
    """Contextualizes a data path to a named visible columns context.

    :param path: a datapath object
    :param context_name: name of the context within the path's terminating table's "visible columns" annotations
    :param context_body: a list of visible column definitions, if given, the `context_name` will be ignored
    :param groupkey_name: column name for the group by key of the generated query expression (default: 'RID')
    :return: a 'contextualized' attribute group query object
    """
    assert isinstance(path, DataPath)
    path = copy.deepcopy(path)
    context = path.context
    table = context._wrapped_table
    sources = table.annotations.get(_erm.tag.source_definitions, {}).get('sources')
    vizcols = context_body if context_body else table.annotations.get(_erm.tag.visible_columns, {}).get(context_name, [])
    if not vizcols:
        raise ValueError('Visible columns context "%s" not found for table %s:%s' % (context_name, table.schema.name, table.name))
    groupkey = context.columns[groupkey_name]
    projection = []

    for vizcol in vizcols:
        try:
            projection.append(_datapath_deserialize_vizcolumn(path, vizcol, sources=sources))
            path.context = context
        except ValueError as e:
            logger.warning(str(e))

    def not_same_as_group_key(x):
        assert isinstance(groupkey, _ColumnWrapper)
        if not isinstance(x, _ColumnWrapper):
            return True
        return groupkey._wrapped_column != x._wrapped_column

    projection = filter(not_same_as_group_key, projection)  # project groupkey only once
    query = path.groupby(groupkey).attributes(*projection)
    return query


def _datapath_generate_simple_denormalization(path, include_whole_entities=False):
    """Generates a denormalized form of the table expressed in a visible columns specification.

    :param path: a datapath object
    :param include_whole_entities: if a denormalization cannot find a 'name' like terminal, include the whole entity (i.e., all attributes), else return just the 'RID'
    :return: a generated visible columns specification based on a denormalization heuristic
    """
    assert isinstance(path, DataPath)
    context = path.context
    table = context._wrapped_table

    fkeys = list(table.foreign_keys)
    single_column_fkeys = {
        fkey.foreign_key_columns[0].name: fkey
        for fkey in table.foreign_keys if len(fkey.foreign_key_columns) == 1
    }

    def _fkey_to_vizcol(name, fk, inbound=None):
        # name columns to look for in related tables
        name_candidates = [
            'displayname',
            'preferredname',
            'fullname',
            'name',
            'title',
            'label'
        ]

        # determine terminal column
        terminal = 'RID'
        for candidate_col in fk.pk_table.columns:
            if candidate_col.name.lower().replace(' ', '').replace('_', '') in name_candidates:
                terminal = candidate_col.name
                break

        # define source path
        source = [{'outbound': fk.names[0]}, terminal]
        if inbound:
            source = [{'inbound': inbound.names[0]}] + source

        # return vizcol spec
        return {
            'markdown_name': name,
            'source': source,
            'entity': include_whole_entities and terminal == 'RID'
        }

    # assemble the visible column:
    #  1. column or single column fkeys
    #  2. all other (outbound fkey) related tables
    #  3. all associated tables
    vizcols = []
    for col in table.column_definitions:
        if col.name in single_column_fkeys:
            fkey = single_column_fkeys[col.name]
            vizcols.append(_fkey_to_vizcol(col.name, fkey))
            del single_column_fkeys[col.name]
            fkeys.remove(fkey)
        else:
            vizcols.append(col.name)

    for outbound_fkey in fkeys:
        vizcols.append(_fkey_to_vizcol(outbound_fkey.constraint_name, outbound_fkey))

    for inbound_fkey in table.referenced_by:
        if inbound_fkey.table.is_association():
            vizcols.append(
                _fkey_to_vizcol(
                    inbound_fkey.table.name,
                    inbound_fkey.table.foreign_keys[0] if inbound_fkey != inbound_fkey.table.foreign_keys[0] else inbound_fkey.table.foreign_keys[1],
                    inbound=inbound_fkey
                )
            )

    return vizcols

def simple_denormalization(path):
    """A simple heuristic denormalization."""
    return _datapath_generate_simple_denormalization(path)

def simple_denormalization_with_whole_entities(path):
    """A simple heuristic denormalization with related and associated entities."""
    return _datapath_generate_simple_denormalization(path, include_whole_entities=True)

def _datapath_denormalize(path, context_name=None, heuristic=None, groupkey_name='RID'):
    """Denormalizes a path based on annotations or heuristics.

    :param path: a DataPath object
    :param context_name: name of the visible-columns context or if none given, will attempt apply heuristics
    :param heuristic: heuristic to apply if no context name specified
    :param groupkey_name: column name for the group by key of the generated query expression (default: 'RID')
    """
    assert isinstance(path, DataPath)
    assert context_name is None or isinstance(context_name, str)
    assert isinstance(groupkey_name, str)
    heuristic = heuristic or simple_denormalization
    assert callable(heuristic)
    return _datapath_contextualize(
        path,
        context_name=context_name,
        context_body=None if context_name else heuristic(path),
        groupkey_name=groupkey_name
    )
