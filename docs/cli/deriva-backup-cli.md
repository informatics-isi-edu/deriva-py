# deriva-backup-cli

The `deriva-backup-cli` is a command-line utility for orchestrating the 
backup of schema and tabular data from ERMRest catalogs and 
file assets from Hatrac object stores to a local directory either as a 
`bag` archive or plain directory.

## Features

- Backup schema, tabular data and file assets to Deriva catalogs to a
 `bag` or plain directory on a mounted filesystem. 
- File assets stored in bags can be stored as remote file references in 
the bag's `fetch.txt` file (aka a "holey bag").

__NOTE__: Automatic file asset backup is not currently implemented. 

## Command-Line options

```
usage: deriva-backup-cli.py [-h] [--version] [--quiet] [--debug]
                   [--credential-file <file>]
                   [--token <auth-token> | --oauth2-token <oauth2-token>]
                   [--config-file <config file>] [--catalog <1>]
                   [--no-data | --no-schema]
                   [--no-bag | --include-assets {full,references}]
                   [--bag-archiver {zip,tgz,bz2}]
                   [--exclude-data <schema>, <schema:table>, ...]
                   <host> <output dir> ...

Deriva Catalog Backup Utility - CLI

positional arguments:
  <host>                Fully qualified host name.
  <output dir>          Path to an output directory.
  [key=value key=value ...]
                        Variable length of whitespace-delimited key=value pair
                        arguments used for string interpolation in specific
                        parts of the configuration file. For example:
                        key1=value1 key2=value2

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
                        Path to a configuration file.
  --catalog <1>         Catalog number. Default: 1
  --no-data             Do not export data, export schema only.
  --no-schema           Do not export schema, export data only.
  --no-bag              Do not store the output in a bag container.
  --include-assets {full,references}
                        Include related file assets in output bag. Use "full"
                        to download related assets to the output bag. Use
                        "references" to store references to asset files in the
                        bag's "fetch.txt" file.
  --bag-archiver {zip,tgz,bz2}
                        Format for compressed bag output.
  --exclude-data <schema>, <schema:table>, ...
                        List of comma-delimited schema-name and/or schema-
                        name/table-name to exclude from data export, in the
                        form <schema> or <schema:table>.

```

### Positional arguments:

#### `<host>`
All backup functions are performed with respect to a specific host and most hosts will
require authentication.

#### `<input path>`
A path to an output directory is required. This can be an absolute path or a path relative to the current working directory.

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
The catalog number (or path specifier). Defaults to 1.

#### `--no-data`
Do not export data, export schema only.

#### `--no-schema`
Do not export schema, export data only.

#### `--no-bag`
Do not store the output in a bag container.

#### `--bag-archiver {zip,tgz,bz2}`
Compression format for output bag archive.

#### `--include-assets {full,references}`
Include related file assets in output bag. Use `full` to download 
related assets to the output bag. Use `references` to store references 
to asset files in the bag's `fetch.txt` file.

#### `--exclude-data <schema>, <schema:table>, ...`
List of comma-delimited schema-name and/or schema-name/table-name to 
exclude from data export, in the form `schema` or `schema:table`.

For example:
```
--exclude data public,demo:samples
```