# Installing

This project is mostly in an early development phase. The `master` branch is expect to be stable and usable at every
commit. The APIs and CLIs may change in backward-incompatible ways, so if you depend on an interface you should remember
the GIT commit number.

At this time, we recommend installing from source, which can be accomplished with the `pip` utility.

If you have root access and wish to install into your system Python directory, use the following command:

```
$ sudo pip install --upgrade git+https://github.com/informatics-isi-edu/deriva-py.git
```

Otherwise, it is recommended that you install into your user directory using the following command:

```
$ pip install --user --upgrade git+https://github.com/informatics-isi-edu/deriva-py.git
```