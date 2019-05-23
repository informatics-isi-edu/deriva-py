# deriva-sitemap-cli

## Using deriva-sitemap-cli to create sitemaps

The `deriva-sitemap-cli` utility creates a sitemap containing `record` entries for all publicly-readable rows in one or more ermrest tables.

`deriva-sitemap-cli` [`-h`] [`--version`] [`--quiet`] [`--debug`] [`--config-file` _config\_file_] [`--catalog` _catalog\_number_] [`-p` _priority_] [`-s` _schema_] [`-t` _table_]
                          <host>

Each sitemap entry contains a URL to a `record` page and an estimate of when that page last changed (based on the last modified times of the row from the primary table referred to by that page and the corresponding rows from tables with single-valued foreign keys pointing at that table). The resulting sitemap is written to stdout.

Arguments:

`-h`, `--help`: print help

`--version`: Print version and exit.

`--quiet`: Suppress logging output.

`--debug`: Enable debug logging output.

`--config-file` _config\_file_: Path to a configuation file. This should contain a JSON array of elements with `schema`, `table`, and (optionally) `priority` defined.

`--catalog` _catalog\_number_: Catalog number (default 1)

`-p` _priority_, `--priority` _priority_: A floating-point number between 0.0 and 1.0 indicating the table's priority (or, if a config file is used, for all tables that don't have a priority specified explictly in the config file)

`-s` _schema_, `--schema` _schema_: The name of the schema of the (single) table to include

`-t` _table_, `--table` _table_: The name of the (single) table to include

## Examples

Create a sitemap for the table MySchema:MyTable on the server myserver.org:

`deriva-sitemap-cli` -s MySchema -t MyTable myserver.org

Create a sitemap for tables Animals:Dogs without a specified priority and Animals.Cats with priority 0.9:

`deriva-sitemap-cli` --config-file animal_tables.json myserver.org

with this config file:
```
  [
    {"schema": "Animals", "table": Dogs"},
    {"schema": "Animals", "table": Cats", "priority": "0.9"}
  ]
```