# deriva-download-cli

The `deriva-download-cli` is a command-line utility for orchestrating the bulk export of tabular data 
(stored in ERMRest) and download of asset data (stored in Hatrac, or other supported HTTP-accessible object store).
It supports the transfer of data directly to local filesystems, or packaging results into the
[`bagit`](https://en.wikipedia.org/wiki/BagIt) container format.  The program is driven by the combined
usage of command-line arguments and a JSON-based configuration ("spec") file, which contains the processing
directives used to orchestrate the creation of the result data set.

## Features

- Transfer both tabular data and file assets from Deriva catalogs.
- Create `bag` containers, which may reference files stored in remote locations.
- Supports an extensible processing pipeline whereby data may be run through transform functions
or other arbitrary processing before final result packaging.

## Command-Line options

```
usage: deriva-download-cli.py [-h] [--version] [--quiet] [--debug]
                              [--credential-file <file>] [--catalog <1>]
                              [--token <auth-token>]
                              <host> <config file> <output dir> ...

Deriva Data Download Utility - CLI

positional arguments:
  <host>                Fully qualified host name.
  <config file>         Path to a configuration file.
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
  --catalog <1>         Catalog number. Default: 1
  --token <auth-token>  Authorization bearer token.
```

### Positional arguments:

#### `<host>`
All operations are performed with respect to a specific host and most hosts will
require authentication. If the `--host HOSTNAME` option is not given, `localhost` will be assumed.

#### `<config file>`
A path to a configuration file is required.  The format and syntax of the can be [configuration file](#example-configuration-file) is described below.

#### `<output dir>`
A path to a output base directory is required. This can be an absolute path or a path relative to the current working directory.

### Optional arguments:

#### `--token`
The CLI accepts an authentication token with the `--token TOKEN` option. If this
option is not given, it will look in the user home dir where the `DERIVA-Auth`
client would store the credentials.

#### `--credential-file`
If `--token` is not specified, the program will look in the user home dir where the `DERIVA-Auth`
client would store the credentials.  Use the `--credential file` argument to override this behavior and specify an alternative credential file.

#### `--catalog`
The catalog number (or path specifier). Defaults to 1.
<a name="configuration_file"></a>
## Configuration file format

The configuration JSON file (or "spec") is the primary mechanism for orchestrating the export and download of data for a given host.
There are three primary objects that comprise the configuration spec; the `catalog` element, the `env` element, and the `bag` element.

The `catalog` object is a REQUIRED element, and is principally composed of an array named `queries` which is a set of configuration stanzas,
executed in declared order, that individually describe _what_ data to retrieve, _how_ the data should be processed, and _where_
the result data should be placed in the target filesystem.

The `env` object is an OPTIONAL element which, if present, is expected to be a dictionary of key-value pairs that are available to use as
interpolation variables for various keywords in the `queries` section of the configuration file.  The string substitution is performed using the keyword
interpolation syntax of Python [`str.format`](https://docs.python.org/2/library/stdtypes.html#str.format).  NOTE: when specifying arbitrary
key-value pairs on the command-line, such pairs will OVERRIDE any matching keys found in the `env` element of the configuration file.

The `bag` object is an OPTIONAL element which, if present, declares that the aggregate output from all configuration stanzas listed in the
`catalog:queries` array should be packaged as a [`bagit`](ttps://en.wikipedia.org/wiki/BagIt) formatted container.  The `bag` element contains
various optional parameters which control bag creation specifics.

<a name="example-configuration-file"></a>
#### Example configuration file:
```json
{
  "env": {
    "accession": "XYZ123",
    "term": "Chip-seq"
  },
  "bag": {
    "bag_name": "test-bag",
    "bag_archiver": "zip",
    "bag_metadata": {
      "Source-Organization": "USC Information Sciences Institute, Informatics Systems Research Division"
    }
  },
  "catalog": {
    "queries": [
      {
        "processor": "csv",
        "processor_params": {
          "query_path": "/attribute/D:=isa:dataset/accession={accession}/E:=isa:experiment/experiment_type:=isa:experiment_type/term=RNA%20expression%20%28RNA-seq%29/$E/STRAND:=vocabulary:strandedness/$E/R:=isa:replicate/SAMPLE:=isa:biosample/SPEC:=vocabulary:species/$R/SEQ:=isa:sequencing_data/PAIRED:=vocabulary:paired_end_or_single_read/$SEQ/file_format:=vocabulary:file_format/term=FastQ/$SEQ/dataset:=D:accession,experiment:=E:RID,biosample:=SAMPLE:RID,replicate:=R:RID,bioreplicate_num:=R:bioreplicate_number,techreplicate_num:=R:technical_replicate_number,species:=SPEC:term,paired:=PAIRED:term,stranded:=STRAND:term,read:=SEQ:read,file:=SEQ:RID,filename:=SEQ:filename,url:=SEQ:url",
          "output_path": "{accession}/{accession}-RNA-Seq"
        }
      },
      {
        "processor": "download",
        "processor_params": {
          "query_path": "/attribute/D:=isa:dataset/accession={accession}/E:=isa:experiment/experiment_type:=isa:experiment_type/term=RNA%20expression%20%28RNA-seq%29/$E/STRAND:=vocabulary:strandedness/$E/R:=isa:replicate/SAMPLE:=isa:biosample/SPEC:=vocabulary:species/$R/SEQ:=isa:sequencing_data/PAIRED:=vocabulary:paired_end_or_single_read/$SEQ/file_format:=vocabulary:file_format/term=FastQ/$SEQ/dataset:=D:RID,experiment:=E:RID,biosample:=SAMPLE:RID,file:=SEQ:RID,filename:=SEQ:filename,size:=SEQ:byte_count,md5:=SEQ:md5,url:=SEQ:url",
          "output_path": "{dataset}/{experiment}/{biosample}/seq"
        }
      },
      {
        "processor": "csv",
        "processor_params": {
          "query_path": "/attribute/D:=isa:dataset/accession={accession}/E:=isa:experiment/experiment_type:=isa:experiment_type/term=Chip-seq/$E/TARGET:=vocabulary:target_of_assay/$E/R:=isa:replicate/SAMPLE:=isa:biosample/SPEC:=vocabulary:species/$R/SEQ:=isa:sequencing_data/PAIRED:=vocabulary:paired_end_or_single_read/$SEQ/file_format:=vocabulary:file_format/term=FastQ/$SEQ/dataset:=D:accession,experiment:=E:RID,control:=E:control_assay,biosample:=SAMPLE:RID,replicate:=R:RID,bioreplicate_num:=R:bioreplicate_number,technical_replicate_num:=R:technical_replicate_number,species:=SPEC:term,target:=TARGET:term,paired:=PAIRED:term,read:=SEQ:read,file:=SEQ:RID,filename:=SEQ:filename,url:=SEQ:url",
          "output_path": "{accession}/{accession}-ChIP-Seq"
        }
      },
      {
        "processor": "fetch",
        "processor_params": {
          "query_path": "/attribute/D:=isa:dataset/accession={accession}/E:=isa:experiment/experiment_type:=isa:experiment_type/term=Chip-seq/$E/TARGET:=vocabulary:target_of_assay/$E/R:=isa:replicate/SAMPLE:=isa:biosample/SPEC:=vocabulary:species/$R/SEQ:=isa:sequencing_data/PAIRED:=vocabulary:paired_end_or_single_read/$SEQ/file_format:=vocabulary:file_format/term=FastQ/$SEQ/dataset:=D:accession,experiment:=E:RID,biosample:=SAMPLE:RID,technical_replicate_num:=R:technical_replicate_number,rid:=SEQ:RID,filename:=SEQ:filename,size:=SEQ:byte_count,md5:=SEQ:md5,url:=SEQ:url",
          "output_path": "{dataset}/{experiment}/{biosample}/seq",
          "output_filename": "{rid}_{filename}"
        }
      }
    ]
  }
}
```

#### Configuration file element: `catalog`

Example:
```json
{
  "catalog": {
    "queries": [
      {
        "processor": "csv",
        "processor_params": {
          "query_path": "/attribute/D:=isa:dataset/accession={accession}/E:=isa:experiment/experiment_type:=isa:experiment_type/term=Chip-seq/$E/TARGET:=vocabulary:target_of_assay/$E/R:=isa:replicate/SAMPLE:=isa:biosample/SPEC:=vocabulary:species/$R/SEQ:=isa:sequencing_data/PAIRED:=vocabulary:paired_end_or_single_read/$SEQ/file_format:=vocabulary:file_format/term=FastQ/$SEQ/dataset:=D:accession,experiment:=E:RID,control:=E:control_assay,biosample:=SAMPLE:RID,replicate:=R:RID,bioreplicate_num:=R:bioreplicate_number,technical_replicate_num:=R:technical_replicate_number,species:=SPEC:term,target:=TARGET:term,paired:=PAIRED:term,read:=SEQ:read,file:=SEQ:RID,filename:=SEQ:filename,url:=SEQ:url",
          "output_path": "{accession}/{accession}-ChIP-Seq"
        }
      },
      {
        "processor": "fetch",
        "processor_params": {
          "query_path": "/attribute/D:=isa:dataset/accession={accession}/E:=isa:experiment/experiment_type:=isa:experiment_type/term=Chip-seq/$E/TARGET:=vocabulary:target_of_assay/$E/R:=isa:replicate/SAMPLE:=isa:biosample/SPEC:=vocabulary:species/$R/SEQ:=isa:sequencing_data/PAIRED:=vocabulary:paired_end_or_single_read/$SEQ/file_format:=vocabulary:file_format/term=FastQ/$SEQ/dataset:=D:accession,experiment:=E:RID,biosample:=SAMPLE:RID,technical_replicate_num:=R:technical_replicate_number,rid:=SEQ:RID,filename:=SEQ:filename,size:=SEQ:byte_count,md5:=SEQ:md5,url:=SEQ:url",
          "output_path": "{dataset}/{experiment}/{biosample}/seq",
          "output_filename": "{rid}_{filename}"
        }
      }
    ]
  }
}
```
Parameters:

| Parent Object | Parameter | Description|Interpolatable
| --- | --- | --- | --- |
|root|*catalog*|This is the parent object for all catalog related parameters.|No
|catalog|*queries*|This is an array of objects representing a list of `ERMRest` queries and the logical outputs of these queries. The logical outputs of each query are then in turn processed by an *output format processor*, which can either be one of a set of default processors, or an external class conforming to a specified interface.|No
|queries|*processor*|This is a string value used to select from one of the built-in query output processor formats. Valid values are `env`, `csv`, `json`, `json-stream`, `download`, or `fetch`.|No
|queries|*processor_type*|A fully qualified Python class name declaring an external processor class instance to use. If this parameter is present, it OVERRIDES the default value mapped to the specified `processor`. This class MUST be derived from the base class `deriva.transfer.download.processors.BaseDownloadProcessor`. For example, `"processor_type": "deriva.transfer.download.processors.CSVDownloadProcessor"`.|No
|queries|*processor_params*|This is an extensible JSON Object that contains processor implementation-specific parameters.|No
|processor_params|*query_path*|This is string representing the actual `ERMRest` query path to be used in the HTTP(S) GET request. It SHOULD already be percent-encoded per [RFC 3986](https://tools.ietf.org/html/rfc3986#section-2.1) if it contains any characters outside of the unreserved set.|Yes
|processor_params|*output_path*|This is a POSIX-compliant path fragment indicating the target location of the retrieved data relative to the specified base download directory.|Yes
|processor_params|*output_filename*|This is a POSIX-compliant path fragment indicating the OVERRIDE filename of the retrieved data relative to the specified base download directory and the value of `output_path`, if any.|Yes

#### Configuration file element: `bag`
Example:
```json
{
    "bag": {
        "bag_name": "test-bag",
        "bag_archiver": "zip",
        "bag_algorithms": ["sha256"],
        "bag_metadata": {
            "Source-Organization": "USC Information Sciences Institute, Informatics Systems Research Division"
        }
    }
}
```
Parameters:

| Parent Object | Parameter | Description|
| --- | --- | --- |
|root|*bag*|This is the parent object for all bag-related defaults.
|bag|*bag_algorithms*|This is an array of strings representing the default checksum algorithms to use for bag manifests, if not otherwise specified.  Valid values are "md5", "sha1", "sha256", and "sha512".
|bag|*bag_archiver*|This is a string representing the default archiving format to use if not otherwise specified.  Valid values are "zip", "tar", and "tgz".
|bag|*bag_metadata*|This is a list of simple JSON key-value pairs that will be written as-is to bag-info.txt.


#### Configuration file element: `env`
Example:
```json
{
    "env": {
        "accession": "XYZ123",
        "term": "Chip-seq"
    }
}
```
Parameters:

| Parent Object | Parameter | Description|
| --- | --- | --- |
|root|*env*|This is the parent object for all global "environment" variables. Note that the usage of _"env"_ in this case does not refer to the set of OS environment variables, but rather a combination of key-value pairs from the JSON configuration file and CLI arguments.
|env|*key:value, ...*|Any number of arguments in the form `key:value` where `value` is a `string`.

## Supported processors
The following `processor` tag values are supported by default:

| Tag | Type | Description|
| --- | --- | --- |
|[`env`](#env)|Metadata|Populates the context metadata ("environment") with values returned by the query.
|[`csv`](#csv)|CSV|CSV format with column header row
|[`json`](#json)|JSON|JSON Array of row objects.
|[`json-stream`](#json-stream)|"Streaming" JSON|Newline-delimited, multi-object JSON.
|[`download`](#download)|Asset download|File assets referenced by URL are download to local storage relative to `output_path`.
|[`fetch`](#fetch)|Asset reference|`Bag`-based. File assets referenced by URL are assigned as remote file references via `fetch.txt`.

## Processor details
Each _processor_ is designed for a specific task, and the task types may vary for a given data export task.
Some _processors_ are designed to handle the export of tabular data from the catalog, while others are meant to handle the export of file assets that are referenced by tables in the catalog.
Other _processors_ may be implemented that could perform a combination of these tasks, implement a new format, or perform some kind of data transformation.

<a name="env"></a>
### `env`
This `processor` processor performs a catalog query in JSON mode and stores the key-value pairs of the _first_ row of data returned into the metadata context or "working environment" for the download.
These key-value pairs can then be used as interpolation variables in subsequent stages of processing.

<a name="csv"></a>
### `csv`
This `processor` generates a standard Comma Separated Values formatted text file. The first row is a comma-delimited list of column names, and all subsequent rows are comma-delimted values.  Fields are not enclosed in quotation marks.

Example output:
```
subject_id,sample_id,snp_id,gt,chipset
CNP0001_F09,600009963128,rs6265,0/1,HumanOmniExpress
CNP0002_F15,600018902293,rs6265,0/0,HumanOmniExpress
```

<a name="json"></a>
### `json`
This `processor` generates a text file containing a JSON Array of row data, where each JSON object in the array represents one row.

Example output:
```json
[{"subject_id":"CNP0001_F09","sample_id":"600009963128","snp_id":"rs6265","gt":"0/1","chipset":"HumanOmniExpress"},
 {"subject_id":"CNP0002_F15","sample_id":"600018902293","snp_id":"rs6265","gt":"0/0","chipset":"HumanOmniExpress"}]
 ```

<a name="json-stream"></a>
### `json-stream`
This `processor` generates a text file containing multiple lines of individual JSON objects terminated by the _newline_ line terminator `\n`. This
format is generally used when the result set is too prohibitively large to parse as a single JSON object and instead can be processed on a line-by-line basis.

Example output:
```
{"subject_id":"CNP0001_F09","sample_id":"600009963128","snp_id":"rs6265","gt":"0/1","chipset":"HumanOmniExpress"}
{"subject_id":"CNP0002_F15","sample_id":"600018902293","snp_id":"rs6265","gt":"0/0","chipset":"HumanOmniExpress"}
```

<a name="download"></a>
### `download`
This `processor` performs multiple actions. First, it issues a `json-stream` catalog query against the specified `query_path`,
in order to generate a _file download manifest_ file named `download-manifest.json`. This manifest is simply a set of rows which MUST contain at least one field named `url`, MAY contain a field named `filename`,
and MAY contain other arbitrary fields.

If the `filename` field is present, it will be appended to the final (calculated) `output_path`, otherwise the application will perform a _HEAD_ HTTP request against
the `url` for the `Content-Disposition` of the referenced file asset. If this query fails to determine the filename, the application falls back to using the final string component of the `url` field after the last `/` character.
The `output_filename` field may be used to override all of the `output_path` filename computation logic stated above, in order to explicitly declare the desired filename.
If other fields are present, they are available for variable substitution in other parameters that support interpolation, e.g., `output_path` and `output_filename`.

After the _file download manifest_ is generated, the application attempts to download the files referenced in each `url` field to the local filesystem, storing them at the base relative path specified by `output_path`.

For example, the following configuration stanza:
```json
{
  "processor": "download",
  "processor_params": {
    "query_path": "/attribute/D:=isa:dataset/accession={accession}/E:=isa:experiment/experiment_type:=isa:experiment_type/term=RNA%20expression%20%28RNA-seq%29/$E/STRAND:=vocabulary:strandedness/$E/R:=isa:replicate/SAMPLE:=isa:biosample/SPEC:=vocabulary:species/$R/SEQ:=isa:sequencing_data/PAIRED:=vocabulary:paired_end_or_single_read/$SEQ/file_format:=vocabulary:file_format/term=FastQ/$SEQ/dataset:=D:RID,experiment:=E:RID,biosample:=SAMPLE:RID,file:=SEQ:RID,filename:=SEQ:filename,size:=SEQ:byte_count,md5:=SEQ:md5,url:=SEQ:url",
    "output_path": "{dataset}/{experiment}/{biosample}/seq"
  }
}
```

Produces a `download-manifest.json` with rows like:
```json
{
  "dataset":13641,
  "experiment":51203,
  "biosample":50233,
  "file":55121,
  "filename":"LPHW_111414_001A_e11.5_facebase_md_rna_R1.fastq.gz",
  "size":2976697043,
  "md5":"9139b1626a35122fa85688cbb7ae6a8a",
  "url":"/hatrac/facebase/data/fb2/FB00000806.2/LPHW_111414_001A_e11.5_facebase_md_rna_R1.fastq.gz"
}
```

After the `output_path` template string is interpolated with the values of the example row above, the file is then downloaded to the following relative path:
```
./13641/51203/50233/seq/LPHW_111414_001A_e11.5_facebase_md_rna_R1.fastq.gz
```

<a name="fetch"></a>
### `fetch`
This `processor` performs multiple actions. First, it issues a `json-stream` catalog query against the specified `query_path`, in order to generate a  _file download manifest_.
This manifest is simply a set of rows which MUST contain at least one field named `url`, and SHOULD contain two additional fields: `length`,
which is the size of the referenced file in bytes, and (at least) one of the following _checksum_ fields; `md5`, `sha1`, `sha256`, `sha512`. If the _length_ and appropriate _checksum_ fields are missing,
an attempt will be made to dynamically determine these fields from the remote `url` by issuing a _HEAD_ HTTP request and parsing the result headers for the missing information.
If the required values cannot be determined this way, it is an error condition and the transfer will abort.

Similar to the `download` processor, the output of the catalog query MAY contain other fields. If the `filename` field is present, it will be appended to the final (calculated) `output_path`, otherwise the application will perform a _HEAD_ HTTP request against
the `url` for the `Content-Disposition` of the referenced file asset. If this query fails to determine the filename, the application falls back to using the final name component of the `url` field after the last `/` character.
The `output_filename` field may be used to override all of the `output_path` filename computation logic stated above, in order to explicitly declare the desired filename.
If other fields are present, they are available for variable substitution in other parameters that support interpolation, e.g., `output_path` and `output_filename`.

Unlike the `download` processor, the `fetch` processor does not actually download any asset files, but rather uses the query results to create a `bag` with check-summed manifest entries that reference each remote asset via the `bag`'s `fetch.txt` file.

## Supported transform_processors
The following `transform_processor` tag values are supported by default:

| Tag | Type | Description|
| --- | --- | --- |
|[`strsub`](#strsub)|Transform|String substitution transformation.
|[`interpolation`](#interpolation)|Transform|Performs a string interpolation.
|[`cat`](#cat)|Transform|Concatenates multiple files.

## Transform Processor details
Each _transform processor_ performs a transformation over the input stream(s). The transform processors may alter 
specific fields of the input (e.g., `strsub`) while others alter the entire contents and format of the input (e.g., 
`interpolation`).

<a name="strsub"></a>
### `strsub`
This `transform_processor` processor performs a string substitution on a designated property of the input stream. The 
input must be `json-stream`. The spec allows multiple `substitutions` where `pattern` is given as a regular expresison
following Python `re` conventions, `reply` is the replacement string to substitute for each matched pattern, `input` is
the name of the object attribute to process, and `output` is the name of the object attribute to set with the result.
The following example would strip off the version suffix (`...:version-id`) from Hatrac versioned URLs.

```json
{
  "transform_processors": [
    {
      "processor":"strsub",
      "processor_params": {
        "input_path": "track-metadata.json",
        "output_path": "track-metadata-unversioned.json",
        "substitutions": [
          {
            "pattern": ":[^/]*$",
            "repl": "",
            "input": "url",
            "output": "url"
          }
        ]
      }
    }
  ]
}
```

<a name="interpolation"></a>
### `interpolation`
This `transform_processor` processor performs a string interpolation on each line of the input stream. The input must
be `json-stream` format. Each row of the input is passed as the environment for the string interpolation parameters. 
The following example would take metadata for genomic annotation tracks and create a line for the "custom tracks" 
specification used by UCSC and other Genome Browsers.

```json
    {
      "processor":"interpolation",
      "processor_params": {
        "input_path": "track-metadata-unversioned.json",
        "output_path": "customtracks.txt",
        "template": "track type=$type name=\"$RID\" description=\"$filename\" bigDataUrl=https://www.facebase.org$url\n"
      }
    }
```

<a name="cat"></a>
### `cat`
This `transform_processor` processor performs a concatenation of multiple input streams into a single output stream. In
the following example, 2 input files are concatenated into one. (More than 2 input files are allow.)

```json
    {
      "processor":"cat",
      "processor_params": {
        "input_paths": ["super-track.txt", "track.txt"],
        "output_path": "trackDb.txt"
      }
    }
```
