# deriva-restore-cli

The `deriva-restore-cli` is a command-line utility for orchestrating the 
restoration of schema and tabular data to ERMRest catalogs and file 
assets to Hatrac object stores from a specifically formatted directory 
structure (e.g., one generated from `deriva-backup-cli`) on the local 
filesystem.

## Features

- Restore schema, tabular data and file assets to Deriva servers from a
 `bag` archive or regular directory.

- The restore process can be configured to selectively exclude the 
restoration of both schema and data for a given set of 
schema or tables. It is also possible to toggle off the restoration of
catalog annotations or catalog access policy (ACLs).

- The table data restoration process is resilient to interruption or 
partial completion and may be restarted. However, if any existing catalog 
schema or data is mutated outside of the scope of the restore function and
in-between such restarts, the restored catalog's consistency cannot be 
guaranteed.
            
__NOTE__: File asset restore is not fully implemented at this time. Only
baseline cases where the restored catalog has a proper bulk-upload 
catalog-level annotation and assets are properly populated in the input 
directory (or referenced via a bag's `fetch.txt`) are supported.

## Command-Line options

```
usage: deriva-restore-cli.py [-h] [--version] [--quiet] [--debug]
                   [--credential-file <file>]
                   [--token <auth-token> | --oauth2-token <oauth2-token>]
                   [--config-file <config file>] [--catalog <1>]
                   [--no-data | --no-schema] [--no-assets] [--no-annotations]
                   [--no-policy] [--no-bag-materialize]
                   [--weak-bag-validation]
                   [--exclude-object <schema>, <schema:table>, ...]
                   [--exclude-data <schema>, <schema:table>, ...]
                   <host> <input_path> ...

Deriva Catalog Restore Utility - CLI

positional arguments:
  <host>                Fully qualified host name.
  <input_path>          Path to backup file or directory.
  [key=value key=value ...]
                        Variable length of whitespace-delimited key=value pair
                        arguments used for populating the processing
                        environment with parameters for keyword
                        substitution.For example: key1=value1 key2=value2

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
  --config-file <config file>
                        Path to an optional configuration file.
  --catalog <1>         Catalog number. If a catalog number is not specified,
                        a new catalog will be created.
  --no-data             Do not restore table data, restore schema only.
  --no-schema           Do not restore schema, restore data only.
  --no-assets           Do not restore asset data, if present.
  --no-annotations      Do not restore annotations.
  --no-policy           Do not restore access policy and ACLs.
  --no-bag-materialize  If the input format is a bag, do not materialize prior
                        to restore.
  --weak-bag-validation
                        If the input format is a bag, do not abort the restore
                        if the bag fails validation.
  --exclude-object <schema>, <schema:table>, ...
                        List of comma-delimited schema-name and/or schema-
                        name/table-name to exclude from the restore process,
                        in the form <schema> or <schema:table>.
  --exclude-data <schema>, <schema:table>, ...
                        List of comma-delimited schema-name and/or schema-
                        name/table-name to exclude from the restore process,
                        in the form <schema> or <schema:table>.

```

### Positional arguments:

#### `<host>`
All restore functions are performed with respect to a specific host and most hosts will
require authentication.

#### `<input path>`
A path to an input file or directory is required. This can be an absolute path or a path relative to the current working directory.

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

#### `--catalog`
The catalog number (or path specifier). Defaults to 1. If this argument 
is not specified, a new catalog instance will be created.

#### `--no-data`
Do not restore table data, restore schema only.

#### `--no-schema`
Do not restore schema, restore data only.

#### `--no-assets`
Do not restore asset data, if present.

#### `--no-annotations`
Do not restore annotations.

#### `--no-policy`
Do not restore access policy and ACLs.

#### `--no-bag-materialize`
If the input format is a bag, do not materialize prior to restore. The 
bag _materialization_ process attempts to resolve any missing remote files 
referenced in the bag's `fetch.txt` and then validate the bag for both 
completeness and file integrity. 
 
__Warning__: By skipping the materialization step it 
is possible to restore a catalog instance only partially, or potentially 
restore corrupted data. There may however be certain scenarios where this
is desired, for example if you have already validated the integrity of 
the existing files in the bag and do not wish to fetch and include any 
remote file references in the restore operation.

#### `--weak-bag-validation`
If the input format is a bag, do not abort the restore if the bag fails 
validation. This argument can be used when the bag materialization process 
raises an error but you wish for the restore process to continue rather 
than abort. This may be desirable if you have already validated the 
integrity of the existing files in the bag and wish to ignore any validation 
errors referring to missing remote file references.

#### `--exclude-object <schema>, <schema:table>, ...`
List of comma-delimited schema-name and/or schema-name/table-name to 
exclude from the restore process, in the form `schema` or `schema:table`.

For example:
```
--exclude object public,demo:samples
```

#### `--exclude-data <schema>, <schema:table>, ...`
List of comma-delimited schema-name and/or schema-name/table-name to 
exclude from the restore process, in the form `schema` or `schema:table`.

For example:
```
--exclude data public,demo:samples
```