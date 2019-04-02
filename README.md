# deriva-py
[![PyPi Version](https://img.shields.io/pypi/v/deriva.svg)](https://pypi.python.org/pypi/deriva)
[![PyPi Wheel](https://img.shields.io/pypi/wheel/deriva.svg)](https://pypi.python.org/pypi/deriva)
[![Python Versions](https://img.shields.io/pypi/pyversions/deriva.svg)](https://pypi.python.org/pypi/deriva)
[![License](https://img.shields.io/pypi/l/deriva.svg)](http://www.apache.org/licenses/LICENSE-2.0)

Python APIs and CLIs (Command-Line Interfaces) for the DERIVA platform.

## Installing

This project is mostly in an early development phase. The `master` branch is expect to be stable and usable at every
commit. The APIs and CLIs may change in backward-incompatible ways, so if you depend on an interface you should remember
the GIT commit number.

At this time, we recommend installing from source, which can be accomplished with the `pip` utility.

If you have root access and wish to install into your system Python directory, use the following command:
```
$ sudo pip install git+https://github.com/informatics-isi-edu/deriva-py.git
```
Otherwise, it is recommended that you install into your user directory using the following command:
```
$ pip install --user git+https://github.com/informatics-isi-edu/deriva-py.git
```

## APIs

The APIs include:
- low-level ERMrest interface (see `ErmrestCatalog`)
- low-level Hatrac interface (see `HatracStore`)
- higher-level ERMrest catalog configuration (see `CatalogConfig`)
- higher-level ERMrest "data path" (see [documentation and tutorials](./docs/README.md))

## CLIs

The CLIs include:
- `deriva-acl-config`: a command-line ERMrest ACL configuration utility (see [documentation](docs/cli/deriva-acl-config.md))
- `deriva-hatrac-cli`: a command-line Hatrac client (see [documentation](docs/cli/deriva-hatrac-cli.md))
- `deriva-download-cli`: a command-line utility for batch export and  download of tabular data from ERMrest and objects from Hatrac (see [documentation](docs/cli/deriva-download-cli.md))
- `deriva-upload-cli`: a command-line data upload and metadata update utility
