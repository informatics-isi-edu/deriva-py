# deriva-annotation-config

## Using deriva-annotation-config to configure annotations 

The `deriva-annotation-config` utility reads a configuration file and uses it to set annotationss for an ermrest catalog (or for a schema or table within that catalog). Usage is:

`deriva-annotation-config` [`-n`|`--dryrun`] [`-v`|`--verbose`] [`-s`|`--schema` schema] [`-t`|`--table` table] [`--host host`] [`--config-file` config_file] [`--credential-file` credential_file] catalog

where the required arguments are:

_catalog_: an ermrest catalog number (e.g., 1)

`--config_file` file: the name of a configuration file

Options are:

`-credential_file` file: read credentials from the named _file_ (if not specified, look for credentials maintained by `deriva-auth`)

`--dryrun`: do nothing, just print out the catalog schema that would be applied

`--verbose`: verbose, print acls and acl bindings for each object

`--schema` schema: operate only on the named _schema_, not the whole catalog

`--table` table: operate only on the named _table_, not the whole catalog (requires the `--schema` option)

`--host` host: configure the server on the specified _host_ (default `localhost`)

## Config file format 

The config file is a JSON file divided into the following stanzas:

`known_attributes`: Attributes (i.e., annotation URIs, such as `tag:isrd.isi.edu,2016:display`) managed through this config file

`schema_annotations`: Catalog-level annotations.

`schema_annotations`: Annotations for individual schemas.

`table_annotations`: Annotations for individual tables.

`column_annotations`: Annotations for individual columns.

`foreign_key_annotations`: Annotations for foreign keys.

### The `known_attributes` stanza 

This stanza contains parameters that control the behavior of `deriva-annotation-config` itself. This section has three sub-sections: `managed`, `ignored`, and `ignore_all_unmanaged`.

The `managed` section is a list of annotation types managed by `deriva-annotation-config`.

The `ignore_all_unmanaged` section is a boolean: if `true`, `deriva-annotation-config` will leave any annotations not in the `managed` list unchanged. If `false`, `deriva-annotation-config` will clear any annotations not in the `managed` list.

The `ignored` section is a list of annotation types that are recognized by `deriva-annotation-config` but that aren't created or updated by it, depending on the value of `ignore_all_unmanaged`. If `ignore_all_unmanaged` is `true`, the program leaves existing annotations of this type alone. If `ignore_all_unmanaged` is `false`, the program removes existing annotation sof this type.

Example:
```
    "known_attributes": {
        "ignore_all_unmanaged": false,
        "managed": [
            "tag:isrd.isi.edu,2016:column-display", 
            "tag:isrd.isi.edu,2016:display", 
            "tag:isrd.isi.edu,2016:foreign-key", 
            ...
	],
	"ignored" : [
            "comment", 
            "description", 
            "facetOrder"
	]
    }
```

This is a version one might use to remove all `comment`, `description`, and `facetOrder` annotations from a catalog. If `ignore_all_attributes` were `true`, those annotations would be left unchanged.


### The schema_annotations stanza 

This is where annotations for schemas are set. The syntax is a list of entries of the form:

{schema_descriptor: value, annotation_descriptor: value}

A schema_descriptor is either:

`"schema":` _schema_name_

or

`"schema_pattern:"` _regular_expression_

When setting permissions on a schema:
* if an exact `schema` match is found, the associated ACL is used (and any matching `schema_pattern` entries are ignored).
* If no exact `schema` match is found and exactly one matching `schema_pattern` entry is found, then that ACL is used.
* If no exact `schema` match is found and more than one matching `schema_pattern` entry is found, then an error is thrown.

An annotation_descriptor has the form:

`"uri":` _annotation_uri_
`"value":`: _annotation_value_

For example:
```
        {
            "schema": "Vocabulary", 
            "uri": "tag:misd.isi.edu,2015:display", 
            "value": {
                "name_style": {
                    "title_case": true, 
                    "underline_space": true
                }
            }
        }
```

### The table_annotations stanza 

This is where annotationss for tables are set. The syntax is a list of entries of the form:
{schema_descriptor, table_descriptor, annotation_desciptor}
The schema_descriptor and annotation_descriptor have the same form as above.

A table_descriptor is either:

`"table":` _table_name_

or

`"table_pattern:"` _regular_expression_

Regular expression matching is used:
* If an entry with an exact `schema` and `table` match is found, the associated ACL is used (and any other matching entries are ignored).
* Otherwise, if entry with an exact `schema` match and exactly one `table_pattern` match is found, that ACL is used.
* Otherwise, if exactly one entry with a `schema_pattern` and `table_pattern` match is found, that ACL is used.
* If none of the above is true, and multiple matching entries are found, then an error is thrown.

For example:

```
    "table_annotations": [
        {
            "schema": "Vocabulary", 
            "table_pattern": ".*_terms", 
            "uri": "tag:isrd.isi.edu,2016:table-display", 
            "value": {
                "row_name": {
                    "row_markdown_pattern": "{{name}}"
                }
            }
        }
```

This sets an annotation for any table in the "Vocabulary" schema whose table name ends in "\_terms".

### The column_annotations stanza 

This is where ACLs for columns are set. The syntax is a list of entries of the form:
{schema_descriptor, table_descriptor, column_descriptor, annotation_descriptor}
The schema, table, and annotation descriptors have the same form as above.
The column_descriptor is either:

`"column":` _table_name_

or

`"column_pattern:"` _regular_expression_

Regular expression matching is used:
* If an entry with exact `schema`, `table`, and `column` matches is found, the associated ACL is used (and any other matching entries are ignored).
* Otherwise, if exactly one entry is found with `schema`, `table`, and `column` matches is found, that ACL is used
* If multiple regular-expression matches are found and no exact match is found, an exception is thrown.

Example:
```
        {
            "column": "RCT", 
            "schema_pattern": ".*", 
            "table_pattern": ".*", 
            "uri": "tag:misd.isi.edu,2015:display", 
            "value": {
                "name": "Creation Time"
            }
        }
```

This sets the display name for any column named "RCT" in the catalog.

### The foreign_key_annotations stanza 

This is where annotations for foreign keys are set. The syntax is a list of entries of the form:

{schema_descriptor, table_descriptor, fkey_schema_descriptor, fkey_name_descriptor, annotation_descriptor}

The schema, table, and annotation descriptors have the same form as above.
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

Example:

```
        {
            "foreign_key": "Antibody_Gene_Association_NCBI_GeneID_fkey", 
            "foreign_key_schema": "", 
            "schema": "Antibody", 
            "table": "Antibody_Gene_Association", 
            "uri": "tag:isrd.isi.edu,2016:foreign-key", 
            "value": {
                "to_name": "Related Genes"
            }
        }
```
