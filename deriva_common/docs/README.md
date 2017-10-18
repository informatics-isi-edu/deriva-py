# deriva_common
Shared library functions for Python-based DERIVA platform code.

## DataPath

The `datapath` module is an interface for building ERMrest "data paths" and retrieving data from ERMrest catalogs. In
its present form, the module provides a limited programmatic interface to ERMrest.

### Features

- Build ERMrest "data path" URLs with a Pythonic interface
- Covers the essentials for data retrieval: link tables, filter on attributes, select attributes, alias tables
- Retrieve entity sets; all or limited numbers of entities
- Convert entity sets to Pandas DataFrames

### Limitations

- Only supports `entity` and `attribute` resources
- Only supports `GET` method
- Only supports binary predicates (e.g., `NOT` is not supported)
- Only supports `application/json` CONTENT-TYPE (i.e., protocol could be made more efficient)
- The `EntitySet` interface is a thin wrapper over a dictionary of a list of results
- Many user errors are caught by Python `assert` statements rather than checking for "invalid paramters" and throwing
  custom `Exception` objects
  
### Tutorials

See the Jupyter Notebook tutorials in the `docs/` folder.

- [Example 1](./derivapy-datapath-example-1.ipynb): basic schema inspection
- [Example 2](./derivapy-datapath-example-2.ipynb): basic data retrieval
- [Example 3](./derivapy-datapath-example-3.ipynb): building simple data paths
- [Example 4](./derivapy-datapath-example-4.ipynb): slightly more advanced topics
