# deriva-py

Python APIs and CLIs (Command-Line Interfaces) for the DERIVA platform.

## Installing

This project is mostly in an early development phase. The `master` branch is expect to be stable and usable at every
commit. The APIs and CLIs may change in backward-incompatible ways, so if you depend on an interface you should remember
the GIT commit number.

At this time, we recommend installing from source, which can be accomplished with the `pip` utility.

```
pip install git+git@github.com:informatics-isi-edu/deriva-py.git
```

## APIs

The APIs include:
- low-level ERMrest interface (see `ErmrestCatalog`)
- low-level Hatrac interface (see `HatracStore`)
- higher-level ERMrest catalog configuration (see `CatalogConfig`)
- higher-level ERMrest "data path" (see [documentation and tutorials](./docs/README.md))

## CLIs

The CLIs include:
- `deriva-acl-config`: a command-line ERMrest ACL configuration utility (see [documentation](docs/acl_config.md))
- `deriva-hatrac-cli`: a command-line hatrac client (see [documentation](docs/deriva-hatrac-cli.md))
- `deriva-upload-cli`: a command-line data upload and metadata update utility
