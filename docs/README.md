## DataPath

The `datapath` module is an interface for building ERMrest "data paths" and retrieving data from ERMrest catalogs. It also supports data manipulation (insert, update, delete). In its present form, the module provides a limited 
programmatic interface to ERMrest.

### Features

- Build ERMrest "data path" URLs with a Pythonic interface
- Covers the essentials for data retrieval: link tables, filter on attributes, select attributes, alias tables
- Retrieve entity sets; all or limited numbers of entities
- Fetch computed aggregates or grouped aggregates
- Convert entity sets to Pandas DataFrames
- Insert and update entities of a table
- Delete entities identified by a (potentially, complex) data path

### Limitations

- Only supports `application/json` CONTENT-TYPE (i.e., protocol could be made more efficient).
- The `ResultSet` interface is a thin wrapper over a dictionary of a list of results.
- Many user errors are caught by Python `assert` statements rather than checking for "invalid parameters" and throwing 
custom `Exception` objects.
  
### Tutorials

See the Jupyter Notebook tutorials in the `docs/` folder.

- [Example 1](./derivapy-datapath-example-1.ipynb): basic schema inspection
- [Example 2](./derivapy-datapath-example-2.ipynb): basic data retrieval
- [Example 3](./derivapy-datapath-example-3.ipynb): building simple data paths
- [Example 4](./derivapy-datapath-example-4.ipynb): slightly more advanced topics
- [Data Update Example](./derivapy-datapath-update.ipynb): examples of insert, update, and delete

Now, [get started](./get-started.ipynb)!

## ERMrest Model Management

The `core.ermrest_model` module provides an interface for managing the
model (schema definitions) of an ERMrest catalog.  This library
provides an (incomplete) set of helper routines for common model
management idioms.

For some advanced scenarios supported by the server but not yet
supported in this library, a client may need to resort to direct usage
of the low-level `deriva.core.ermrest_catalog.ErmrestCatalog` HTTP
access layer.

### Features

- Obtain an object hierarchy mirroring the model of a catalog or catalog snapshot.
- Discover names of schemas, tables, and columns as well as definitions where applicable.
- Discover names and definitions of key and foreign key constraints.
- Discover annotations on catalog and model elements.
- Discover policies on catalog and model elements (if sufficiently privileged).
- Create model elements
   - Create new schemas.
   - Create new tables.
   - Create new columns on existing tables.
   - Create new key constraints over existing columns.
   - Create new foreign key constraints over existing columns and key constraints.
- Delete model elements
   - Drop schemas.
   - Drop tables.
   - Drop columns.
   - Drop key constraints.
   - Drop foreign key constraints.

### Limitations

Because the model management interface mirrors a complex remote
catalog model with a hierarchy of local objects, it is possible for
the local objects to get out of synchronization with the remote
catalog and either represent model elements which no longer exist or
lack model elements recently added.

The provided management methods, when used carefully, can
incrementally update the local representation with changes made to the
server by the calling client. However, if other clients make
concurrent changes, it is likely that the local representation will
diverge.

The only robust solution to this problem is for the caller to discard
its model representation, reconstruct it to match the latest server
state, and retry whatever changes are intended.

### Examples

For the following examples, we assume this common setup:

    from deriva.core import ErmrestCatalog
    import deriva.core.ermrest_model as em
    from deriva.core.ermrest_model import builtin_types as typ

    
    catalog = ErmrestCatalog(...)
    model_root = catalog.getCatalogModel()

Also, when examples show keyword arguments, they illustrate a typical
override value. If omitted, a default value will apply. Many parts of
the model definition are immutable once set, but in general `comment`,
`acl`, `acl_binding`, and `annotation` attributes can be modified after
the fact through configuration management APIs.

#### Add Table to Schema

To create a new table, you build a table definition document and pass
it to the table-creation method on the object representing an existing
schema. The various classes involved include class-methods
`define(...)` to construct the constituent parts of the table
definition:

    column_defs = [ 
      em.Column.define("Col1", typ.text), 
      em.Column.define("Col2", typ.int8),
    ]
    key_defs = [
      em.Key.define(
        ["Col1"], # this is a list to allow for compound keys
        constraint_names=[ [schema_name, "My New Table_Col1_key"] ],
        comment="Col1 text values must be distinct.",
        annotations={},
      )
    ]
    fkey_defs = [
      em.ForeignKey.define(
        ["Col2"], # this is a list to allow for compound foreign keys
        "Foreign Schema",
        "Referenced Table",
        ["Referenced Column"], # this is a list to allow for compound keys
        on_update='CASCADE',
        on_delete='SET NULL',
        constraint_names=[ [schema_name, "My New Table_Col2_fkey"] ],
        comment="Col2 must be a valid reference value from the domain table.",
        acls={},
        acl_bindings={},
        annotations={},
      )
    ]
    table_def = em.Table.define(
      "My New Table",
      column_defs,
      key_defs=key_defs,
      fkey_defs=fkey_defs,
      comment="My new entity type.",
      acls={},
      acl_bindings={},
      annotations={},
      provide_system=True,
    )
    schema = model_root.schemas[schema_name]
    new_table = schema.create_table(catalog, table_def)

By default, `create_table(...)` will add system columns to the table
definition, so the caller does not need to reconstruct these standard elements
of the column definitions nor the `RID` key definition.

#### Add a Vocabulary Term Table

A vocabulary term table is often useful to track a controlled
vocabulary used as a domain table for foreign key values used in
science data columns.  A simple vocabulary term table can be
created with a helper function:

    schema = model_root.schemas[schema_name]
    new_vocab_table = schema.create_table(catalog,
      Table.define_vocabulary(
        "My Vocabulary",
        "MYPROJECT:{RID}",
		"https://server.example.org/id/{RID}"
      )
    )

The `Table.define_vocabular()` method is a convenience wrapper around
`Table.define()` to automatically generate core vocabulary table
structures. It accepts other table definition parameters which a
sophisticated caller can use to override or extend these core
structures.

#### Add Column to Table

To create a new column, you build a column definition document and
pass it to the column-creation method on the object representing an
existing table.

    column_def = em.Column.define(
      "My New Column",
      typ.text,
      nullok=False,
      comment="A string representing my new stuff.",
      annotations={},
      acls={},
      acl_bindings={},
    )
    table = model_root.table(schema_name, table_name)
    new_column = table.create_column(catalog, column_def)

The same pattern can be used to add a key or foreign key to an
existing table via `table.create_key(catalog, key_def)` or
`table.create_fkey(catalog, fkey_def)`, respectively. Similarly, a
schema can be added to a model with `model.create_schema(catalog,
schema_def)`.

#### Remove a Column from a Table

To delete a column, you invoke the `delete()` method on the
column object itself:

    table = model_root.table(schema_name, table_name)
	column = table.column_definitions[column_name]
	column.delete(catalog, table=table)

The optional `table` argument allows the method to prune the stale
object from the table object to reflect the change made on
the server. If this is omitted, the server change will be made but the
local table object will fall out of synchronization.

The same pattern can be used to remove a key or foreign key from a
table via `key.delete(catalog, table)` or `foreign_key.delete(catalog,
table)`, respectively. Similarly, a schema or table can be removed
with `schema.delete(catalog, model)` or `table.delete(catalog,
schema)`, respectively.

## ErmrestCatalog

The `deriva.core.ermrest_catalog.ErmrestCatalog` class provides HTTP
bindings to an ERMrest catalog as a thin wrapper around the Python
Requests library.  Likewise, the
`deriva.core.ermrest_catalog.ErmrestSnapshot` class provides HTTP
bindings to an ERMrest catalog snapshot. While catalogs permit
mutation of stored content, a snapshot is mostly read-only and only
permits retrieval of content representing the state of the catalog at
a specific time in the past.

Instances of `ErmrestCatalog` or `ErmrestSnapshot` represent a
particular remote catalog or catalog snapshot, respectively. They
allow the client to perform HTTP requests against individual ERMrest
resources, but require clients to know how to formulate those
requests in terms of URL paths and resource representations.

Other, higher-level client APIs are layered on top of this implementation
class and exposed via factory-like methods integrated into each catalog
instance.

### Catalog Binding

A catalog is bound using the class constructor, given parameters
necessary for binding:

    from deriva.core.ermrest_catalog import ErmrestCatalog
    from deriva.core import get_credential
    
    scheme = "https"
    server = "myserver.example.com"
    catalog_id = "1"
    credentials = get_credential(server)
    
    catalog = ErmrestCatalog(scheme, server, catalog_id, credentials=credentials)

### Client Credentials

In the preceding example, a credential is obtained from the filesystem
assuming that the user has activated the `deriva-auth` authentication
agent prior to executing this code. For catalogs allowing anonymous
access, the optional `credentials` parameter can be omitted to
establish an anonymous binding.

The same client credentials (or anonymous access) is applied to all
HTTP operations performed by the subsequent calls to the catalog
object's methods. If a calling program wishes to perform a mixture of
requests with different credentials, they should create multiple
catalog objects and choose the appropriate object for each request
scenario.

### High-Level API Factories

Several optional access APIs are layered on top of `ErmrestCatalog`
and/or `ErmrestSnapshot` and may be accessed by invoking convenient
factory methods on a catalog or snapshot object:

- `catalog_snapshot = catalog.latest_snapshot()`
   - `ErmrestSnapshot` binding for latest known revision of catalog
- `path_builder = catalog.getPathBuilder()`
   - `deriva.core.datapath.Catalog` path builder for catalog (or snapshot)
   - Allows higher-level data access idioms as described previously.
- `config_root = catalog.getCatalogConfig()`
   - `deriva.core.ermrest_config.CatalogConfig` object for catalog (or snapshot)
   - The `config_root` object roots a tree of objects isomorphic to the catalog model, organizing configuration data according to each part of the model.
   - Allows inspection of catalog/snapshot annotations and policies.
   - Allows mutation to draft a new configuration objective.
   - Draft changes are applied with `catalog.applyCatalogConfig(config_root)`
- `model_root = catalog.getCatalogModel()`
   - `deriva.core.ermrest_model.Model` object for catalog (or snapshot)
   - The `model_root` object roots a tree of objects isomorphic to the catalog model, organizing model definitions according to each part of the model.
   - Allows inspection of catalog/snapshot models (schemas, tables, columns, constraints)
   - Some model management idioms are exposed as methods on individual objects in the model hierarchy.

### Low-Level HTTP Methods

When the client understands the URL structuring conventions of
ERMrest, they can use basic Python Requests idioms on a catalog
instance:

- resp = catalog.get(path)
- resp = catalog.delete(path)
- resp = catalog.put(path, json=data)
- resp = catalog.post(path, json=data)

Unlike Python Requests, the `path` argument to each of these methods
should exclude the static prefix of the catalog itself. For example,
assuming `catalog` has been bound to
`https://myserver.example.com/ermrest/catalog/1` as in the constructor
example above, an attempt to access table content at
`https://myserver.example.com/ermrest/catalog/1/entity/MyTable` would
call `catalog.get(`/entity/MyTable`) and the catalog binding would
prepend the complete catalog prefix.

The `json` input to the `catalog.put` and `catalog.post` methods
behaves just as in Python Requests. The data is supplied as native
Python lists, dictionaries, numbers, strings, and booleans. The method
implicitly serializes the data to JSON format and sets the appropriate
Content-Type header to inform the server we are sending JSON content.

All of these HTTP methods return a `requests.Response` object which
must be further interrogated to determine request status or to
retrieve any content produced by the server:

- resp.status_code: the HTTP response status code
- resp.raise_for_status(): raise a Python exception for non-success codes
- resp.json(): deserialize JSON content from server response
- resp.headers: a dictionary of HTTP headers from the server response

Low-level usage errors may raise exceptions directly from the HTTP
methods. However, normal server-indicated errors will produce a
response object and the caller must interrogate the `status_code`
field or use the `raise_for_status()` helper to determine whether the
request was successful.

### HTTP Caching

By default, the catalog binding uses HTTP caching for the
`catalog.get` method: it will store previous responses, include
appropriate `If-None-Match` headers in the new HTTP GET request,
detect `304 Not Modified` responses indicating that cached content is
valid, and return the cached content to the caller. This mechanism can
be disabled by specifying `caching=False` in the ErmrestCatalog
constructor call.
