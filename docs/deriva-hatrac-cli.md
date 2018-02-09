# deriva-hatrac-cli

The `deriva-hatrac-cli` is a command-line utility for interacting with the DERIVA 
HATRAC object store.

## Features

- List, create, and delete namespaces
- Get, put, and delete objects
- Get, set, and delete ACLs

See `deriva-hatrac-cli --help` for a complete list of its features, arguments and
options.

## Common options

All operations are performed with respect to a specific host and most hosts will
require authentication.

### Hostname

If the `--host HOSTNAME` option is not given, `localhost` will be assumed.

### Authentication

The CLI accepts an authentication token with the `--token TOKEN` option. If this 
option is not given, it will look in the user home dir where the `DERIVA-Auth` 
client would store the credentials.

## Namespace operation examples

### List

```bash
$ deriva-hatrac-cli --host example.org list /hatrac/
/hatrac/path1
/hatrac/path2
/hatrac/path3
```

### Create Namespace

```bash
$ deriva-hatrac-cli --host example.org mkdir /hatrac/path1/foo
```

### Delete Namespace

```bash
$ deriva-hatrac-cli --host example.org rmdir /hatrac/path1/foo
```

Hatrac does not allow reuse of namespace paths. If you create, delete, then 
(re)create a namespace you will get an error.

```bash
$ deriva-hatrac-cli --host example.org mkdir /hatrac/path1/foo
deriva-hatrac-cli mkdir: /hatrac/path1/foo: Namespace exists or the parent path is not a namespace
```

## Object operation examples

### Put an object

```bash
$ deriva-hatrac-cli --host example.org put bar.jpg /hatrac/path1/foo/bar.jpg
/hatrac/path1/foo/bar.jpg:LZJMSF6JQT7SFOVE2RBUZ4UEP4
```

The hatrac versioned path, ending in "...`:LZJMSF6JQT7SFOVE2RBUZ4UEP4`" is returned
on success.

### Get an object

```bash
$ deriva-hatrac-cli --host example.org get /hatrac/path1/foo/bar.jpg barcopy.jpg
2017-11-28 12:15:40,700 - INFO - File [barcopy.jpg] transfer successful. 195.99 KB transferred at 29655.20 MB/second. Elapsed time: 0:00:00.006609.
2017-11-28 12:15:40,700 - INFO - Verifying checksum for file [barcopy.jpg]
```

If the `outfilename` argument is not given it will take the `basename` of the 
object path.

```bash
$ deriva-hatrac-cli --host example.org get /hatrac/path1/foo/bar.jpg
2017-11-28 12:17:03,236 - INFO - File [bar.jpg] transfer successful. 195.99 KB transferred at 30447.60 MB/second. Elapsed time: 0:00:00.006437.
2017-11-28 12:17:03,236 - INFO - Verifying checksum for file [bar.jpg]
```

Alternately, the object can be streamed to `stdout` which you could then pipe 
through another utility or redirect to a file as in this example.

```bash
$ deriva-hatrac-cli --host example.org get /hatrac/path1/foo/bar.jpg - > barcopy2.jpg
```

Note that when streaming to `stdout` the CLI will not (be able to) compute and 
verify the retrieved object's checksum.

### Delete an object

```bash
$ deriva-hatrac-cli --host example.org del /hatrac/path1/foo/bar.jpg
```

As with namespaces, a deleted object path cannot be reused.

## ACL operation examples

ACL operations may be performed on any hatrac "path"; i.e., on namespaces and 
objects.

### Get ACLs

```bash
$ deriva-hatrac-cli --host example.org getacl /hatrac/path1/foo
subtree-update:
create:
subtree-create:
subtree-read:
owner:
  https://auth.globus.org/111111-2222-3333-4444-5555555
subtree-owner:
```

### Set ACLs

```bash
$ deriva-hatrac-cli --host example.org setacl /hatrac/path1/foo subtree-read http://0000000-1111-2222-333333
```

More than one role may be added at a time.

### Delete ACLs

Deleting an ACL deletes one or all `role`s from a specified ACL `access-mode`. See 
the output of the `getacl` operation for a list of all available `access-mode`s.

```bash
$ deriva-hatrac-cli --host example.org delacl /hatrac/path1/foo subtree-read
```
