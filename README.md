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
pip install lightbeam
```


# Setup
Running the tool requires
1. a folder of JSONL files, one for each Ed-Fi Resource and Descriptor to populate
1. a YAML configuration file
An example YAML configuration is below, followed by documentation of each option.

```yaml
verbose: True
state_dir: ~/.lighbeam/
data_dir: ./
edfi_api_base_url: https://api.schooldistrict.org/v5.3/api/
edfi_api_version: 3
edfi_api_mode: year_specific
edfi_api_year: 2021
edfi_api_client_id: yourID
edfi_api_client_secret: yourSecret
connection_pool_size: 8
connection_timeout: 60
num_retries: 10
backoff_factor: 1.5
retry_statuses: [429, 500, 502, 503, 504]
verbose: True
show_stacktrace: True
```
* (optional) Turn on `verbose` output. The default is `False`.
* (optional) `state_dir` is where [state](#state) is stored. The default is `~/.lightbeam/` on *nix systems, `C:/Users/USER/.lightbeam/` on Windows systems.
* (optional) Specify the `data_dir` which contains JSONL files to send to Ed-Fi. The default is `./`. The tool will look for files like `{Resource}.jsonl` or `{Descriptor}.jsonl` in this location.
* (optional) Specify the `edfi_api_base_url` to which to connect. The default is `https://localhost/api` (the address of an Ed-Fi API [running locally in Docker](https://techdocs.ed-fi.org/display/EDFITOOLS/Docker+Deployment)).
* (optional) Specify the `edfi_api_mode` as one of `shared_instance`, `sandbox`, `district_specific`, `year_specific`, or `instance_year_specific`.
* (required if `edfi_api_mode` is `year_specific` or `instance_year_specific`) Specify the `edfi_api_year` used to build the resource URL. The default is the current year.
* (required if `edfi_api_mode` is `instance_year_specific`) Specify the `edfi_api_instance_code` used to build the resource URL.
* (required) Specify the `edfi_api_client_id` to use when connecting to the Ed-Fi API.
* (required) Specify the `edfi_api_client_secret` to use when connecting to the Ed-Fi API.
* (optional) Specify the `connection_pool_size` to use when making requests to the API. The default is 8. The optimal setting will depend on how powerful your Ed-Fi API is.
* (optional) Specify the `num_retries` to do in case of request failures.
* (optional) Specify the `backoff_factor` to use for the exponential backoff.
* (optional) Specify the `retry_statuses`, that is, the HTTPS response codes to consider as failures to retry.
* (optional) Specify whether or not to show `verbose` output. The default is `False`.
* (optional) Specify whether to show a stacktrace for runtime errors. The default is `False`.


# Usage
Once you have the requierd [setup](#setup), send the JSONL payloads with
```bash
lightbeam path/to/config.yaml
```

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
In your [YAML configuration](#setup), you may reference environment variables with `${ENV_VAR}`. This can be useful for making references to source file locations dynamic, such as
```yaml
...
edfi_api_client_id: ${CLIENT_ID}
edfi_api_client_secret: ${CLIENT_SECRET}
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
Or you may choose to resend payloads that returned a certain HTTP status code(s) on the last send using the `-u` or `--retry-status-codes` command-line flag:
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
JSONL files are sent to the Ed-Fi API in resource-dependency order, which avoids "missing reference" API errors.


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