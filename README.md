<!-- Logo/image -->
![lightbeam](lightbeam/resources/lightbeam.png)

`lightbeam` transmits payloads from JSONL files into an [Ed-Fi API](https://techdocs.ed-fi.org/display/ETKB/Ed-Fi+Operational+Data+Store+and+API).
<!-- GIF or screenshot? -->


# Table of Contents  
* [Requirements](#requirements)
* [Installation](#installation)
* [Setup](#setup)
* [Usage](#usage)
* [Features](#features)
* [Design](#design)
* [Performance](#performance)
* [Limitations](#limitations)
* [Changelog](#changelog)
* [Contributing](#contributing)<!--
Guides and Resources -->
* [License](#license)
<!-- References -->


# Requirements
[Python 3](https://www.python.org/), [pip](https://pypi.org/project/pip/), and connectivity to an Ed-Fi API.


# Installation
```
pip install git+https://github.com/edanalytics/lightbeam.git#egg=lightbeam
```


# Setup
Running the tool requires
1. a folder of JSONL files, one for each Ed-Fi Resource and Descriptor to populate
1. a YAML configuration file
An example YAML configuration is below, followed by documentation of each option.

```yaml
state_dir: ~/.lighbeam/
data_dir: ./
edfi_api:
  base_url: https://api.schooldistrict.org/v5.3/api
  version: 3
  mode: year_specific
  year: 2021
  client_id: yourID
  client_secret: yourSecret
connection:
  pool_size: 8
  timeout: 60
  num_retries: 10
  backoff_factor: 1.5
  retry_statuses: [429, 500, 502, 503, 504]
  verify_ssl: True
force_delete: True
log_level: INFO
show_stacktrace: True
```
* (optional) `state_dir` is where [state](#state) is stored. The default is `~/.lightbeam/` on *nix systems, `C:/Users/USER/.lightbeam/` on Windows systems.
* (optional) Specify the `data_dir` which contains JSONL files to send to Ed-Fi. The default is `./`. The tool will look for files like `{Resource}.jsonl` or `{Descriptor}.jsonl` in this location, as well as directory-based files like `{Resource}/*.jsonl` or `{Descriptor}/*.jsonl`. Files with `.ndjson` or simply `.json` extensions will also be processed. (More info at the [`ndjson` standard page](http://dataprotocols.org/ndjson/).)
* Specify the details of the `edfi_api` to which to connect including
  * (optional) The `base_url` The default is `https://localhost/api` (the address of an Ed-Fi API [running locally in Docker](https://techdocs.ed-fi.org/display/EDFITOOLS/Docker+Deployment)).
  * The `version` as one of `3` or `2` (`2` is currently unsupported).
  * (optional) The `mode` as one of `shared_instance`, `sandbox`, `district_specific`, `year_specific`, or `instance_year_specific`.
  * (required if `mode` is `year_specific` or `instance_year_specific`) The `year` used to build the resource URL. The default is the current year.
  * (required if `mode` is `instance_year_specific`) The `instance_code` used to build the resource URL. The default is none.
  * (required) Specify the `client_id` to use when connecting to the Ed-Fi API.
  * (required) Specify the `client_secret` to use when connecting to the Ed-Fi API.
* Specify the `connection` parameters to use when making requests to the API including
  * (optional) The `pool_size`. The default is 8. The optimal setting depends on the Ed-Fi API's capabilities.
  * (optional) The `timeout` (in seconds) to wait for each connection attempt. The default is `60` seconds.
  * (optional) The `num_retries` to do in case of request failures. The default is `10`.
  * (optional) The `backoff_factor` to use for the exponential backoff. The default is `1.5`.
  * (optional) The `retry_statuses`, that is, the HTTPS response codes to consider as failures to retry. The default is `[429, 500, 501, 503, 504]`.
  * (optional) Whether to `verify_ssl`. The default is `True`. Set to `False` when working with `localhost` APIs or to live dangerously.
* (optional) Skip the interactive confirmation prompt (for programmatic use) when using the [`delete`](#delete) command. The default is `False` (prompt).
* (optional) Specify a `log_level` for output. Possible values are
  - `ERROR`: only output errors like missing required sources, invalid references, invalid [YAML configuration](#yaml-configuration), etc.
  - `WARNING`: output errors and warnings like when the run log is getting long
  - `INFO`: all errors and warnings plus basic information about what `earthmover` is doing: start and stop, how many rows were removed by a `distinct_rows` or `filter_rows` operation, etc. (This is the default `log_level`.)
  - `DEBUG`: all output above, plus verbose details about each transformation step, timing, memory usage, and more. (This `log_level` is recommended for [debugging](#debugging-practices) transformations.)
* (optional) Specify whether to show a stacktrace for runtime errors. The default is `False`.



# Usage
`lightbeam` recognizes several commands:

## `validate`
```bash
lightbeam validate path/to/config.yaml
```
You may `validate` your JSONL before transmitting it. This checks that the payloads
* are valid JSON
* conform to the structure described in the Swagger documents for [resources](https://api.ed-fi.org/v5.3/api/metadata/data/v3/resourcess/swagger.json) and [descriptors](https://api.ed-fi.org/v5.3/api/metadata/data/v3/descriptors/swagger.json) fetched from your API
* contain valid descriptor values (fetched from your API)
* contain unique values for any natural key

This command will not find invalid reference errors, but is helpful for finding payloads that are invalid JSON, are missing required fields, or have other structural issues.


## `send`
```bash
lightbeam send path/to/config.yaml
```
Sends your JSONL payloads to your Ed-Fi API.

## `validate+send`
```bash
lightbeam validate+send path/to/config.yaml
```
This is a shorthand for sequentially running [validate](#validate) and then [send](#send). It can be useful to catching errors in automated pipelines earlier in the `validate` step before you actually `send` problematic data to your Ed-Fi API.

## `delete`
```bash
lightbeam delete path/to/config.yaml
```
Delete payloads by
1. determing the natural key (set of required fields) for each endpoint
1. iterating through your payloads and looking up each one via a `GET` request to the API filtering for the natural key values
1. if exactly one result is returned, `DELETE`ing it by `id`

Payload hashes are also deleted from [saved state](#state). Endpoints are processed in reverse-dependency order to prevent delete failures due to data dependencies.

Note that `student` resource payloads *cannot be deleted* since other resources may reference them. (This is an Ed-Fi API restriction.)

Running the `delete` command will prompt you to type "yes" to confirm. This confirmation prompt can be disabled (for programmatic use) by specifying `force_delete: True` in your YAML.

## Other options
See a help message with
```bash
lightbeam -h
lightbeam --help
```

See the tool version with
```bash
lightbeam -v
lightbeam --version
```



# Features
This tool includes several special features:

## Selectors
Send only a subset of resources or descriptors in your `data_dir` using a selector:
```bash
lightbeam path/to/config.yaml -s schools,students,studentSchoolAssociations
```

## Environment variable references
In your [YAML configuration](#setup), you may reference environment variables with `${ENV_VAR}`. This can be useful for passing sensitive data like credentials to `lightbeam`, such as
```yaml
...
edfi_api:
  client_id: ${EDFI_API_CLIENT_ID}
  client_secret: ${EDFI_API_CLIENT_SECRET}
...
```

## Command-line parameters
Similarly, you can specify parameters via the command line with
```bash
lightbeam path/to/config.yaml -p '{"CLIENT_ID":"populated", "CLIENT_SECRET":"populatedSecret"}'
lightbeam path/to/config.yaml --params '{"CLIENT_ID":"populated", "CLIENT_SECRET":"populatedSecret"}'
```
Command-line parameters override any environment variables of the same name.

## State
This tool *maintains state about payloads previously dispatched to the Ed-Fi API* to avoid repeatedly resending the same payloads. This is done by maintaining a [pickled](https://docs.python.org/3/library/pickle.html) Python dictionary of payload hashes for each Ed-Fi resource and descriptor, together with a timestamp and HTTP status code of the last response. The files are located in the [config](#setup) file's `state_dir` and have names like `{resource}.dat` or `{descriptor}.dat`.

By default, only new, never-before-seen payloads are sent.

You may choose to resend payloads last sent before *timestamp* using the `-t` or `--older-than` command-line flag:
```bash
lightbeam path/to/config.yaml -t 2020-12-25T00:00:00
lightbeam path/to/config.yaml --older-than 2020-12-25T00:00:00
```
Or you may choose to resend payloads last sent after *timestamp* using the `-n` or `--newer-than` command-line flag:
```bash
lightbeam path/to/config.yaml -n 2020-12-25T00:00:00
lightbeam path/to/config.yaml --newer-than 2020-12-25T00:00:00
```
Or you may choose to resend payloads that returned a certain HTTP status code(s) on the last send using the `-r` or `--retry-status-codes` command-line flag:
```bash
lightbeam path/to/config.yaml -r 200,201
lightbeam path/to/config.yaml --retry-status-codes 200,201
```
These three options may be composed; `lightbeam` will resend payloads that match any conditions (logical OR).

Finally, you can ignore prior state and resend all payloads using the `-f` or `--force` flag:
```bash
lightbeam path/to/config.yaml -f
lightbeam path/to/config.yaml --force
```


# Design
Some details of the design of this tool are discussed below.

## Resource-dependency ordering
JSONL files are sent to the Ed-Fi API in resource-dependency order, which avoids "missing reference" API errors when populating multiple endpoints.


# Performance & Limitations
Tool performance depends on primarily on the performance of the Ed-Fi API, which in turn depends on the compute resources which back it. Typically the bottleneck is write performance to the database backend (SQL server or Postgres). If you use `lightbeam` to ingest a large amount of data into an Ed-Fi API (not the recommended use-case, by the way), consider temporarily scaling up your database backend.

For reference, we have achieved throughput rates in excess of 100 requests/second against an Ed-Fi ODS & API running in Docker on a laptop.


# Change log
[2022-06-??] Version 0.0.1 released


# Contributing
Bugfixes and new features (such as additional transformation operations) are gratefully accepted via pull requests here on GitHub.

## Contributions
* Cover image created with [DALL &bull; E mini](https://huggingface.co/spaces/dalle-mini/dalle-mini)


# License
See [License](LICENSE).
