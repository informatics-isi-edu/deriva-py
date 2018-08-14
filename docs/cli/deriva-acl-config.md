# deriva-acl-config

## Using deriva-acl-config to configure ACLs 

The `deriva-acl-config` utility reads a configuration file and uses it to set ACLs for an ermrest catalog (or for a schema or table within that catalog). Usage is:

`deriva-acl-config` [`-g`|`--groups-only`] [`-n`|`--dryrun`] [`-v`|`--verbose`] [`-s`|`--schema` schema] [`-t`|`--table` table] [`--host host`] [`--config-file` config_file] [`--credential-file` credential_file] catalog

where the required arguments are:

_catalog_: an ermrest catalog number (e.g., 1)

`--config_file` file: the name of a configuration file

Options are:

`-credential_file` file: read credentials from the named _file_ (if not specified, look for credentials maintained by `deriva-auth`)

`--groups-only`: create and populate a group table (used for dynamic ACLs) based on the contents of the config file

`--dryrun`: do nothing, just print out the catalog schema that would be applied

`--verbose`: verbose, print acls and acl bindings for each object

`--schema` schema: operate only on the named _schema_, not the whole catalog

`--table` table: operate only on the named _table_, not the whole catalog (requires the `--schema` option)

`--host` host: configure the server on the specified _host_ (default `localhost`)

## Config file format 

The config file is a json file divided into the following stanzas:

`groups`: defines a set of named group lists, which can be used in ACL definitions later in the config file.

`group_list_table`: the schema and table name of a table to populate with the information from the `groups` stanza, so you can use the same named group lists in both static and dynamic ACLs and maintain them in one place. This table will typically be the last step in most dynamic ACL projections.

`acl_definitions`: static ACL definitions (e.g., "{"write": "consortium", "select": "everyone"}) that can be referred to later on in the config file (in this example, `consortium` and `everyone` are group lists defined in the `groups` stanza).

`acl_bindings`: dynamic ACL definitions

`catalog_acl`: the ACL for the catalog; this will be one of the ACLs defined in the `acl_definitions` stanza.

`schema_acls`: ACLs for individual schemas. Static ACLs (from `acl_definitions`) stanza are assigned to schemas.

`table_acls`: ACLs for individual tables. Static ACLs (from `acl_definitions`) and dynamic ACLs (from `acl_bindings`) are assigned to tabless.

`column_acls`: ACLs for individual columns. Static ACLs (from `acl_definitions`) and dynamic ACLs (from `acl_bindings`) are assigned to columns

`foreign_key_acls`: ACLs for foreign keys. Static ACLs (from `acl_definitions`) and dynamic ACLs (from `acl_bindings`) are assigned to foreign keys

### The groups stanza 

The `groups` stanza is a list of entries of the form 

name: [values]

where _name_ is a name that will be used to refer to a set of groups, and _values_ is a list of group entries. The entries can be either the actual group IDs (from webauthn) or names of previously-defined groups. For example:

```
    "groups" : {
	"empty": [],
	"public": ["*"],	
        "isrd-staff": ["https://auth.globus.org/176baec4-ed26-11e5-8e88-22000ab4b42b"],
        "isrd-systems": ["https://auth.globus.org/3938e0d0-ed35-11e5-8641-22000ab4b42b"],
        "isrd-testers": ["https://auth.globus.org/9d596ac6-22b9-11e6-b519-22000aef184d"],
	"isrd-all": ["isrd-staff", "isrd-systems", "isrd-testers"]
    }
```

### The group_list_table stanza 

If you're defining dynamic ACLs, at some point you'll probably want a table in your catalog somewhere that maps names to lists of groups. The group_list_table stanza specifies the schema and table name of a table to create for this list of groups. For example:
```
    "group_list_table" : {"schema" : "_acl_admin", "table" : "group_lists"}
```
This will cause the `_acl_admin.group_lists` table to be created (if it doesn't already exist) and populated with the information specified in the `groups` stanza. The `name` column of the table will be the primary key and will contain group name, and the `groups` column will be the fully-expanded group list. The example `groups` stanza above will create this table:
```
     name     |                                                                                          groups                                                                                          
--------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 isrd-staff   | {https://auth.globus.org/176baec4-ed26-11e5-8e88-22000ab4b42b}
 isrd-testers | {https://auth.globus.org/9d596ac6-22b9-11e6-b519-22000aef184d}
 isrd-systems | {https://auth.globus.org/3938e0d0-ed35-11e5-8641-22000ab4b42b}
 isrd-all     | {https://auth.globus.org/176baec4-ed26-11e5-8e88-22000ab4b42b,https://auth.globus.org/9d596ac6-22b9-11e6-b519-22000aef184d,https://auth.globus.org/3938e0d0-ed35-11e5-8641-22000ab4b42b}
 public       | {*}
 empty        | {}
```

### The acl_definitions stanza 

This is where you define static ACLs for later use. The syntax is a list of

name: value

entries, where the _name_ is a name you can refer to later, to assign these ACLs to objects, and the _value_ is the ACL itself (which will probably contain references to the groups defined in the `groups` stanza). For example:
```
    "acl_definitions" : {
        "unrestricted_read" : {"select" : "public", "create": "isrd-systems", "write": "isrd-systems"},
        "isrd_read" : {"select" : "isrd-all", "create": "isrd-systems", "write": "isrd-systems"},
        "secret" : {"select" : "empty"}
     }
```
In this example, the `unrestricted_read` ACL grants read access to everyone and restricts create and write access to the `isrd-systems` group; the `isrd_read` ACL is the same,except that it grants read access only to the `isrd-all` set of groups. Note that this stanza only defines the set of permissions and who they're associated with; by itself, it doesn't apply these ACLs to any object in the catalog.

### The acl_bindings stanza 

This is where you define dynamic ACLs for later use. The syntax is a list of

name: value

pairs, where the _name_ is a name you can refer to later, and the _value_ is a dynamic ACL. For example:
```
    "acl_bindings" : {
	"a_binding" : {
	    "scope_acl": "isrd-staff",
	    "types" : ["select"],
	    "projection" : [{"outbound_col" : "allowed_groups"}, "groups"],
	    "projection_type" : "acl"
	}
```
This defines an ACL binding called `a_binding`. The syntax of the binding itself is the same as defined in the ermrest ACL docs, with one exception: to specify an outbound foreign key, you can either use ermrest-standard `outbound` syntax and use the constraint name, or you can use `outbound_col` and specify the name of a column on which a foreign key is defined. It will also populate the `scope_acl` based on the referenced group list. For example, if you apply the binding `a_binding` to this table:
```
     Column     | Type | Modifiers 
----------------+------+-----------
 my_data        | text | 
 allowed_groups | text | not null
Foreign-key constraints:
    "mytable_allowed_groups_fkey" FOREIGN KEY (allowed_groups) REFERENCES _acl_admin.group_lists(name)
```
then the actual ermrest dynamic ACL will be:
```
        "a_binding" : {
            "scope_acl": ["https://auth.globus.org/176baec4-ed26-11e5-8e88-22000ab4b42b"],
            "types" : ["select"],
            "projection" : [{"outbound" : "mytable_allowed_groups_fkey"}, "groups"],
            "projection_type" : "acl"
        }
```
This can be useful if you want to apply the same dynamic ACL to mutliple tables, since each table's foreign key will have a different name by default.

### The catalog_acls stanza 

This is where ACLs for the catalog itself are set. (Note: there's a bootstrapping issue for new catalogs - since this tool uses ermrest, you need to already have permission on the catalog before you can set any new permissions). For example:
```
    "catalog_acl" : {"acl" : "unrestricted_read"}
```
This applies the `unrestricted_read` ACL defined above to the catalog.

### The schema_acls stanza 

This is where ACLs for schemas are set. The syntax is a list of entries of the form:

{schema_descriptor: value, acl_descriptor: value}

A schema_descriptor is either:

`"schema":` _schema_name_

or

`"schema_pattern:"` _regular_expression_

When setting permissions on a schema:
* if an exact `schema` match is found, the associated ACL is used (and any matching `schema_pattern` entries are ignored).
* If no exact `schema` match is found and exactly one matching `schema_pattern` entry is found, then that ACL is used.
* If no exact `schema` match is found and more than one matching `schema_pattern` entry is found, then an error is thrown.

An acl descriptor has the form

`"acl":` _name_of_acl_defined_earlier_

or

`"no_acl": true`

If an `acl` is specified, the named static ACL is expanded and applied to the schema. If `no_acl` is specified, no ACL is applied to the schema (and as a result, it inherits whatever permissions are set by the catalog ACL).

For example:
```
    "schema_acls" : [
	{"schema" : "Vocabulary", "no_acl" : "true"},
	{"schema" : "ISRD_Internal", "acl" : "isrd_read"},
        {"schema_pattern" : ".*", "acl": "secret"}
    ]
```
This, paired with the catalog_acl stanza above, allows anyone to read the Vocabulary schema and ISRD people to read the "ISRD_Internal". It forbids anyone from reading any other schema in the catalog.

### The table_acls stanza 

This is where ACLs for tables are set. The syntax is a list of entries of the form:
{schema_descriptor, table_descriptor, [acl_descriptor], [acl_bindings_descriptor]}
A schema descriptor has the same form as in the schema_acls stanza.

A table_descriptor is either:

`"table":` _table_name_

or

`"table_pattern:"` _regular_expression_

Regular expression matching is used:
* If an entry with an exact `schema` and `table` match is found, the associated ACL is used (and any other matching entries are ignored).
* Otherwise, if entry with an exact `schema` match and exactly one `table_pattern` match is found, that ACL is used.
* Otherwise, if exactly one entry with a `schema_pattern` and `table_pattern` match is found, that ACL is used.
* If none of the above is true, and multiple matching entries are found, then an error is thrown.

An acl_descriptor is the same as defined above.

An `acl_bindings_descriptor` has the form:

`"acl_bindings":` [list_of_bindings]`

where _list_of_bindings_ is a list of ACL bindings defined in the acl_bindings stanza.

### The column_acls stanza 

This is where ACLs for columns are set. The syntax is a list of entries of the form:
{schema_descriptor, table_descriptor, column_descriptor, [acl_descriptor], [acl_bindings_descriptor] [invalidate_bindings_descriptor]}
The schema, table, acl, and acl_bindings descriptors have the same form as above.
The column_descriptor is either:

`"column":` _table_name_

or

`"column_pattern:"` _regular_expression_

Regular expression matching is used:
* If an entry with exact `schema`, `table`, and `column` matches is found, the associated ACL is used (and any other matching entries are ignored).
* Otherwise, if exactly one entry is found with `schema`, `table`, and `column` matches is found, that ACL is used
* If multiple regular-expression matches are found and no exact match is found, an exception is thrown.

The i`nvalidate_bindings` descriptor has the form:

`"invalidate_bindings"`: [list_of_bindings]

where _list_of_bindings_ is a list of bindng names to invalidate (i.e., ACL bindings that were defined on the column's table but that should not be applied to the column).

### The foreign_key_acls stanza 

This is where ACLs for foreign keys are set. The syntax is a list of entries of the form:

{schema_descriptor, table_descriptor, fkey_schema_descriptor, fkey_name_descriptor, [acl_descriptor], [acl_bindings_descriptor], [invalidate_bindings_descriptor]}

The schema, table, acl, acl_bindings, and invalidate_bindings descriptors have the same form as above.
The fkey_schema_descriptor is either:

`"foreign_key_schema":` foreign_key_schema_name

or

`"foreign_key_schema_pattern":` regular_expression

The fkey_name_descriptor is either:

`"foreign_key":` foreign_key_name

or

`"foreign_key_pattern":` regular_expression

These specify the foreign key schema and name. As with the column_acls stanza:
* If an exact match is found, it's used
* If no exact match is found and exactly one regular expression match is found, it's used.
* If no exact match is found and more than one regular expression match is found, an error is thrown.

## Security Considerations 

There's no guarantee of the order in which changes will be applied. For example, if your current state restricts access to a table, like this:

```
   "table_acls" : [
       {"schema" : "myschema", "table" : "mytable", "acl": "restricted_access"}
   ]
```

and you decide to change to a configuration that restricts access only to one sensitive column in that table:
```
   "table_acls" : [
       {"schema" : "myschema", "table" : "mytable", "acl": "open_access"}
   ],
   "column_acls" : [
       {"schema" : "myschema", "table" : "mytable", "column" : "sensitive_column", "acl": "restricted_access"}
   ]

```

then it's possible that the change to the table ACL will occur before the change to the column ACL, temporarily exposing the table and all its columns. The solution is to run `acl_config` in two passes, first adding the new restrictions and then removing the old ones.
