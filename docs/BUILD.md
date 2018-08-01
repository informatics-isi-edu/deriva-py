# Build _deriva-py_ docs using `sphinx`
In order to build the _deriva-py_ documentation with `sphinx`, the prerequisite software dependencies must first be installed. Python 3 is the _recommended_ version of Python to use when building the documentation.

### Prerequisites
1. Install `pandoc` for Jupyter Notebook conversion support. Follow the instructions for your platform [here](http://pandoc.org/installing.html).
2. If necessary, clone the _deriva-py_ source code from GitHub ([here](https://github.com/informatics-isi-edu/deriva-py)) and `cd` into the source code root directory, `deriva-py`.
3. Install `deriva-py` in `user` mode via `pip` and include `requirements_dev.txt` via the following command:
    ```
    pip3 install --user -r requirements_dev.txt .
    ```

### Build
1. `cd ./docs`
2. `make clean html`

### Output
The output may have warnings but should not terminate with any errors. It should look something like this (warnings removed for brevity):
```sh
Removing everything under '_build'...
Running Sphinx v1.7.6
making output directory...
loading pickled environment... not yet created
loading intersphinx inventory from https://docs.python.org/objects.inv...
intersphinx inventory has moved: https://docs.python.org/objects.inv -> https://docs.python.org/3/objects.inv
building [mo]: targets for 0 po files that are out of date
building [html]: targets for 22 source files that are out of date
updating environment: 22 added, 0 changed, 0 removed
reading sources... [100%] index
looking for now-outdated files... none found
pickling environment... done
checking consistency... done
preparing documents... done
writing output... [100%] index
generating indices... genindex py-modindex
highlighting module code... [100%] logging
writing additional pages... search
copying static files... done
copying extra files... done
dumping search index in English (code: en) ... done
dumping object inventory... done
build succeeded, 20 warnings.

The HTML pages are in _build/html.
```