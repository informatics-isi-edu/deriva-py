# deriva-annotation-validate

The `deriva-annotation-validate` is a command-line utility for validating
the structure and contents of schema annotations in an ERMrest catalog.

## Features

- Validate annotations against the specifications defined by DERIVA for
 known schema annotations.
 
- Validate annotations against the catalog model; to ensure that column names, key names,
  etc. exist within the correct model object for usage in an annotation.
 
- Validation can target specific schemas, tables, keys, and foreign keys
 by regular expression pattern matching.
 
- The process does not alter the contents of the catalog in any way.

## Limitations

The following annotation is not yet supported:
- `tag:isrd.isi.edu,2019:chaise-config`: any JSON object will be considered valid,
  for this annotation, at this time.

## Command-Line options

```
usage: deriva-annotation-validate [-h] [--version] [--quiet] [--debug]
                                  [--credential-file <file>]
                                  [--token <auth-token> | --oauth2-token <oauth2-token>]
                                  [-s <schema>] [-t <table>] [-k <key>]
                                  [-f <foreign_key>] [-a <tag>]
                                  <host> <catalog>

DERIVA command-line interface for validating annotations.

positional arguments:
  <host>                Fully qualified host name.
  <catalog>             Catalog identifier.

optional arguments:
  -h, --help            show this help message and exit
  --version             Print version and exit.
  --quiet               Suppress logging output.
  --debug               Enable debug logging output.
  --credential-file <file>
                        Optional path to a credential file.
  --token <auth-token>  Authorization bearer token.
  --oauth2-token <oauth2-token>
                        OAuth2 bearer token.
  -s <schema>, --schema <schema>
                        Regular expression pattern for schema name
  -t <table>, --table <table>
                        Regular expression pattern for table name
  -k <key>, --key <key>
                        Regular expression pattern for key constraint name
  -f <foreign_key>, --foreign-key <foreign_key>
                        Regular expression pattern for foreign key constraint
                        name
  -a <tag>, --tag <tag>
                        Tag name of annotation
  --skip-model-names    Skip validation of model names found inside of
                        annotations

Known tag names include: tag:misd.isi.edu,2015:display,
tag:isrd.isi.edu,2016:table-alternatives, tag:isrd.isi.edu,2016:column-
display, tag:isrd.isi.edu,2017:key-display, tag:isrd.isi.edu,2016:foreign-key,
tag:isrd.isi.edu,2016:generated, tag:isrd.isi.edu,2016:immutable,
tag:isrd.isi.edu,2016:non-deletable, tag:isrd.isi.edu,2016:app-links,
tag:isrd.isi.edu,2016:table-display, tag:isrd.isi.edu,2016:visible-columns,
tag:isrd.isi.edu,2016:visible-foreign-keys, tag:isrd.isi.edu,2016:export,
tag:isrd.isi.edu,2017:asset, tag:isrd.isi.edu,2018:citation,
tag:isrd.isi.edu,2018:required, tag:isrd.isi.edu,2018:indexing-preferences,
tag:isrd.isi.edu,2017:bulk-upload, tag:isrd.isi.edu,2019:chaise-config,
tag:isrd.isi.edu,2019:source-definitions.
```

### Positional arguments:

#### `<host>`
The hostname of the ERMrest catalog service.

#### `<catalog>`
The catalog name/number of the ERMrest catalog on which validation will be performed.

### Optional arguments:

#### `--token`
The CLI accepts an authentication token with the `--token TOKEN` option. If this
option is not given, it will look in the user home dir where the `DERIVA-Auth`
client would store the credentials.

#### `--oauth2-token`
An OAuth2 bearer token. This argument is mutually exclusive to the `--token` option.

#### `--credential-file`
If `--token` or `--oauth2-token` is not specified, the program will look in the user home dir where the `DERIVA-Auth`
client would store the credentials.  Use the `--credential file` argument to override this behavior and specify an alternative credential file.

#### `--schema <schema>` (default: `.*`)
A regular expression search pattern for matching against schema names. The default `.*` 
will match all schemas in the catalog.

For example:
```
--schema ^isa$|^vocab$|^extra.*
```
This will match on schemas named `isa`, `vocab`, or any schema that starts with `extra`.

#### `--table <table>` (default: `.*`)
A regular expression search pattern for matching against table names. The default `.*` 
will match all tables in the schema.

For example:
```
--table status
```
This will match on tables that have `status` anywhere in their name such as 
`dataset_status` or `status`.

#### `--key <key>` (default: `.*`)
A regular expression search pattern for matching against key names. The default `.*` 
will match all keys in the table.

For example:
```
--key $PK
```
This will match on keys that start with `PK` in the constraint name.

#### `--foreign-key <foreign_key>` (default: `.*`)
A regular expression search pattern for matching against foreign key names. The default `.*` 
will match all foreign keys in the table.

For example:
```
--foreign-key fkey$
```
This will match on foreign keys that end with `fkey` in the constraint name.

#### `--tag <tag>` (default: `None`)
An annotation tag name. If this option is used, only the specified annotation tag name 
will be evaluated. If this option is not used, all annotations present will be evaluated.
__Note__ that this is an _exact match_ not a regular expression match.

For example:
```
--tag tag:isrd.isi.edu,2016:visible-columns
```
This will evaluate only the `tag:isrd.isi.edu,2016:visible-columns` annotations found
throughout the catalog.

#### Notes on regular expression pattern options
- Matching is performed hierarchically such that tables are only matched if the 
 containing schema matched, and keys or foreign-keys are matched on if the containing
 table matched.

#### `--skip-model-names`  (default: `False`)

If this flag is set, the utility will skip validation of model names found 
inside of annotations. For example, it will not validate that the column names,
foreign key names, etc. used in `visible-columns` and other annotations are valid
per the catalog schema.

### Examples

The simplest usage example will check all known annotations on all model objects.
For best results, you should run the tool when logged in with `owner` rights on
a catalog. Otherwise, the model name validation may not be accurate, if you are 
unable to enumerate the entire catalog schema. To ignore model name errors, use
the `--skip-model-names` flag.

```shell script
$ deriva-annotation-validate HOSTNAME CATALOG
```

Where `HOSTNAME` is a host name such as `www.example.org` and `CATALOG` is a 
catalog identifier such as `1`.

To validate only within a single schema named `foo`.

```shell script
$ deriva-annotation-validate HOSTNAME CATALOG -s '^foo$'
```

To validate any table that has the word `bar` in it within the schema `foo`.

```shell script
$ deriva-annotation-validate HOSTNAME CATALOG -s '^foo$' -t 'bar'
```

Note that the above patterns will also validate annotations on the columns, 
keys, and foreign keys of any matching table.

To validate only the tables with `bar` in the name.

```shell script
$ deriva-annotation-validate HOSTNAME CATALOG -s '^foo$' -t 'bar' -c '^$' -k '^$' -f '^$'
```

The patterns `^$` will match empty strings and therefore will not match any model 
objects by name, since they cannot have empty strings as names.

To validate only the `tag:isrd.isi.edu,2016:visible-columns` annotation on any
model object.

```shell script
$ deriva-annotation-validate HOSTNAME CATALOG -a tag:isrd.isi.edu,2016:visible-columns
```

The above command can be combined with other examples above to validate a 
subset of model objects against only a single annotation schema.
