<!-- Logo/image -->
![lightbeam](https://raw.githubusercontent.com/edanalytics/lightbeam/main/lightbeam/resources/lightbeam.png)

`lightbeam` transmits payloads from JSONL files into an [Ed-Fi API](https://docs.ed-fi.org/reference/ods-api-platform).
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
state_dir: ~/.lightbeam/
data_dir: ./
namespace: ed-fi
edfi_api:
  base_url: https://api.schooldistrict.org/v5.3/api
  oauth_url: https://api.schooldistrict.org/v5.3/api/oauth/token 
  dependencies_url: https://api.schooldistrict.org/v5.3/api/metadata/data/v3/2024/dependencies
  open_api_metadata_url: https://api.schooldistrict.org/v5.3/api/metadata/
  descriptors_swagger_url: https://api.schooldistrict.org/v5.3/api/metadata/data/v3/2024/descriptors/swagger.json
  resources_swagger_url: https://api.schooldistrict.org/v5.3/api/metadata/data/v3/2024/resources/swagger.json
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
count:
  separator: ","
fetch:
  page_size: 100
validate:
  methods:
    - schema # checks that payloads conform to the Swagger definitions from the API
    - descriptors # checks that descriptor values are either locally-defined or exist in the remote API
    - uniqueness # checks that local payloads are unique by the required property values
    - references # checks that references resolve, either locally or in the remote API
  # or `methods: "*"`
  references:
    selector:
      - studentAssessments.studentReference
      - studentSchoolAssociations.schoolReference
    behavior: exclude # or `include`
    remote: False # default=True
force_delete: True
log_level: INFO
show_stacktrace: True
```
* (optional) `state_dir` is where [state](#state) is stored. The default is `~/.lightbeam/` on *nix systems, `C:/Users/USER/.lightbeam/` on Windows systems.
* (optional) Specify the `data_dir` which contains JSONL files to send to Ed-Fi. The default is `./`. The tool will look for files like `{Resource}.jsonl` or `{Descriptor}.jsonl` in this location, as well as directory-based files like `{Resource}/*.jsonl` or `{Descriptor}/*.jsonl`. Files with `.ndjson` or simply `.json` extensions will also be processed. (More info at the [`ndjson` standard page](http://dataprotocols.org/ndjson/).)
* (optional) Specify the `namespace` to use when accessing the Ed-Fi API. The default is `ed-fi` but others include `tpdm` or custom values. To send data to multiple namespaces, you must use a YAML configuration file and `lightbeam send` for each.
* Specify the details of the `edfi_api` to which to connect including
  * (optional) The `base_url` which serves a JSON object specifying the paths to data endpoints, Swagger, and dependencies. The default is `https://localhost/api` (the address of an Ed-Fi API [running locally in Docker](https://docs.ed-fi.org/reference/docker/)), but the location varies depending on how Ed-Fi is deployed.
  * Most Ed-Fi APIs publish other endpoints they provide in a JSON document at the base URL - `lighbteam` attempts to discover these. If, however, your API does not publish these URLs at the base, or you want to manually override any of them for any reason, you can specify the following:
    * (optional) `oauth_url` (usually [base_url]/oauth/token)
    * (optional) `dependencies_url` (usually [base_url]/metadata/data/v3/dependencies)
    * (optional) `descriptors_swagger_url` (usually [base_url]/metadata/data/v3/descriptors/swagger.json)
    * (optional) `resources_swagger_url` (usually [base_url]/metadata/data/v3/resources/swagger.json)
    * (optional) `open_api_metadata_url` (usually [base_url]/metadata/) - this is ignored if both `descriptors_swagger_url` and `resources_swagger_url` are specified
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
* (optional) for [`lightbeam count`](#count), optionally change the `separator` between `Records` and `Endpoint`. The default is a "tab" character.
* (optional) for [`lightbeam fetch`](#fetch), optionally specify the number of records (`page_size`) to GET at a time. The default is 100, but if you're trying to extract lots of data from an API increase this to the largest allowed (which depends on the API, but is often 500 or even 5000).
* (optional) for [`lightbeam validate`](#validate), optionally specify the list of validation `methods` to run (from `schema`, `descriptors`, `uniqueness`, and `references`). If validating `references`, specify a list of `selector`s to either `include` or `exclude` (`behavior`) when validating. Also optionally disable `remote` referece validation (enabled by default).
* (optional) Skip the interactive confirmation prompt (for programmatic use) when using the [`delete`](#delete) command. The default is `False` (prompt).
* (optional) Specify a `log_level` for output. Possible values are
  - `ERROR`: only output errors like missing required sources, invalid references, invalid [YAML configuration](#yaml-configuration), etc.
  - `WARNING`: output errors and warnings like when the run log is getting long
  - `INFO`: all errors and warnings plus basic information about what `earthmover` is doing: start and stop, how many rows were removed by a `distinct_rows` or `filter_rows` operation, etc. (This is the default `log_level`.)
  - `DEBUG`: all output above, plus verbose details about each transformation step, timing, memory usage, and more. (This `log_level` is recommended for [debugging](#debugging-practices) transformations.)
* (optional) Specify whether to show a stacktrace for runtime errors. The default is `False`.



# Usage
`lightbeam` recognizes several commands:

## `count`
```bash
lightbeam count -c path/to/config.yaml
```
Prints to the console (or to your `--results-file`, if specified) a record count for each endpoint in your Ed-Fi API.
* By default, resources *and descriptors* (all endpoints) are counted. You can change this by using [selectors](#selectors), such as `-e *Descriptors`.
* Endpoint counts printed to the console (if you don't specify a `--results-file`) include only endpoints with more than zero records. Endpoint counts saved in a `--results-file` include all available endpoints, even those with zero records.
* Whether printed to the console or a `--results-file`, output will include columns `Records` and `Endpoint` separated by a separator specified as `count.separator` in your [YAML configuration](#setup) (default is a "tab" character).

## `fetch`
```bash
lightbeam fetch -c path/to/config.yaml
```
Fetches the payloads of selected endpoints from your Ed-Fi API and saves them, each on their own line, to JSONL files in your `data_dir`.

Optionally specify `--query '{"studentUniqueId": 12345}'` or `-q '{"key": "value"}'` to add query parameters to every GET request. This can be useful if you want to `fetch` data for just a specific record (and related data). For example:
```bash
lightbeam fetch -s student* -e *Descriptors -q '{"studentUniqueId":12345}' -d id,_etag,_lastModifiedDate
```

Optionally specify `--keep-keys id` or `-k id` to keep only specific keys from every payload. This can be useful to reduce the amount of data stored if you only need certain fields. It is used internally by `truncate` to only `fetch` the `id`s or payloads to then `delete` by `id`.

Optionally specify `--drop-keys id,_etag,_lastModified` or `-d id` to remove specific keys from every payload. This can be useful if you want to `fetch` data from one Ed-Fi API and then turn around and `send` it to another.

Like [selectors](#selectors), `keep-keys` and `drop-keys` are comma-separated lists of values, each of which may begin or end with an asterisk (`*`) for wildcard matching. Example: `-d _*` would remove properties beginning with an underscore (`_`) character from any `fetch`ed payloads.

## `validate`
```bash
lightbeam validate -c path/to/config.yaml
```
You may `validate` your JSONL before transmitting it. Configuration for `validate` goes in its own section of `lightbeam.yaml`:
```yaml
validate:
  methods:
    - schema # checks that payloads conform to the Swagger definitions from the API
    - descriptors # checks that descriptor values are either locally-defined or exist in the remote API
    - uniqueness # checks that local payloads are unique by the required property values
    - references # checks that references resolve, either locally or in the remote API
  # or
  # methods: "*"
  references:
    selector:
      - studentAssessments.studentReference
      - studentSchoolAssociations.schoolReference
    behavior: exclude # or `include`
    remote: False # default=True
```
Default `validate`.`methods` are `["schema", "descriptors", "uniqueness"]` (not `references`; see below). In addition to the above methods, `lighteam validate` will also (first) check that each payload is valid JSON.

The `references` `method` can be slow, as a separate `GET` request may be made to your API for each reference. (Therefore the validation method is disabled by default.) `lightbeam` tries to improve efficiency by:
* batching requests and sending several concurrently (based on `connection`.`pool_size` of `lightbeam.yaml`)
* caching responses and first checking the cache before making another (potentially identical) request

Even with these optimizations, checking `references` can easily take minutes for even relatively small amounts of data. Therefore `lightbeam.yaml` also accepts a further configuration option:
```yaml
validate:
  references:
    max_failures: 10 # stop testing after X failed payloads ("fail fast")
```
This is optional; if absent, references in every payload are checked, no matter how many fail.

**Note:** Reference validation efficiency may be improved by first `lightbeam fetch`ing certain resources to have a local copy. `lightbeam validate` checks local JSONL files to resolve references before trying the remote API, and `fetch` retrieves many records per  `GET`, so total runtime can be faster in this scenario. The downsides include
* more data movement
* `fetch`ed data becoming stale over time
* needing to track which data is your own vs. was `fetch`ed (all the data must coexist in the `config.data_dir` to be discoverable by `lightbeam validate`)

You may specify a `selector` list of the form `someEndpoint.path.to.someReference` to include or exclude (according to `behavior`) specific references from reference validation. You may also specity `remote: False` to only validate references against local data in your JSONL files.


## `send`
```bash
lightbeam send -c path/to/config.yaml
```
Sends your JSONL payloads to your Ed-Fi API.

## `validate+send`
```bash
lightbeam validate+send -c path/to/config.yaml
```
This is a shorthand for sequentially running [validate](#validate) and then [send](#send). It can be useful to catching errors in automated pipelines earlier in the `validate` step before you actually `send` problematic data to your Ed-Fi API.

## `delete`
```bash
lightbeam delete -c path/to/config.yaml
```
Delete payloads by
1. determing the natural key (set of required fields) for each endpoint
1. iterating through your JSONL payloads and looking up each one via a `GET` request to the API filtering for the natural key values
1. if exactly one result is returned, `DELETE`ing it by `id`

Payload hashes are also deleted from [saved state](#state). Endpoints are processed in reverse-dependency order to prevent delete failures due to data dependencies.

Note that the default profile for most Ed-Fi API credentials prevents deletion of certain core resources (`student`, `school`, etc.), even if your credentials were used to create the records. If you get API errors trying to delete records, you may need "no further auth" API credentials.

Running the `delete` command will prompt you to type "yes" to confirm. This confirmation prompt can be disabled (for programmatic use) by specifying `force_delete: True` in your YAML.

## `truncate`
```bash
lightbeam truncate -c path/to/config.yaml
```
Truncates (empties) your Ed-Fi API for selected endpoints, in dependency-order. **USE WITH CAUTION!** `truncate` works by fetching the `id` of every record for a given endpoint and then deleting all records by ID.

`Truncate`ing a resource will also clear out the [saved state](#state) for it.

Note that the default profile for most Ed-Fi API credentials prevents deletion of certain core resources (`student`, `school`, etc.), even if your credentials were used to create the records. If you get API errors trying to delete records, you may need "no further auth" API credentials.

Running the `truncate` command will prompt you to type "yes" to confirm. This confirmation prompt can be disabled (for programmatic use) by specifying `force_delete: True` in your YAML.

`truncate` is a convenience command which should be used sparingly, as it can generate large numbers of `deletes` records and cause performance issues when pulling from `deletes` endpoints. If you want to wipe an entire Ed-Fi ODS, a better approach may be to drop and recreate the database (and re-send Descriptors and other default resources as needed).

## `create`
```bash
lightbeam create -s students,schools,studentSchoolAssociations -c path/to/config.yaml
```
Creates a skeleton of an [`earthmover`](https://edanalytics.github.io/earthmover/) project for _creating_ JSONL Ed-Fi data which one can then `lightbeam send` to an Ed-Fi API. It uses the Ed-Fi API's [OpenAPI specification](https://spec.openapis.org/oas/latest.html) to determine the schema of the endpoints you select (`lightbeam create` should usually be used with the `-s` [selector](#selectors)). Then, in the current directory, it:
* creates (if it doesn't already exist, otherwise adds/overwrites) a partial `earthmover.yml` configuration file with empty `sources` and `transformations` but `destinations` for each selected endpoint, plus comments indicating what column names and data types/values are required, and the required grain of the table (based on `isIdentity` flags in the OpenAPI definitions)
* creates (or overwrites) `templates/*.jsont` for each selected endpoint, with skeleton Jinja-JSON that includes all the required fields (including nested ones), optional fields wrapped in conditionals, and comments with some of the property metadata from OpenAPI definitions, such as `type`, `description`, `isIdentity`, etc.

The purpose of `lightbeam create` is to save developers time if they want to use `earthmover` to create Ed-Fi-shaped data from other data sources. See the [`earthmover` documentation](https://edanalytics.github.io/earthmover/) for more information.


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

Override specific configurations in `lightbeam.yml` from the command-line using the `--set` flag
```
lightbeam fetch --set fetch.page_size 1000
lightbeam validate --set log_level WARN show_stacktrace True
lightbeam send --set connection.timeout 15
```
(`--set` must be followed by a set of key-value pairs.)


# Features
This tool includes several special features:

## Selectors
Send only a subset of resources or descriptors in your `data_dir` using `-s` or `--selector`:
```bash
lightbeam send -c path/to/config.yaml -s schools,students,studentSchoolAssociations
```
or, similarly, exclude some resources or descriptors using `-e` or `--exclude`:
```bash
lightbeam send -c path/to/config.yaml -e *Descriptors
```
Selection and exclusion may be a single or comma-separated list of strings or a wildcards (beginning or ending with `*`). For example:
```bash
lightbeam send -c path/to/config.yaml -s student*,parent* -e *Associations,*Descriptors
```
would process resources like `studentSchoolAttendanceEvents` and `parents`, but not `studentSchoolAssociations`, `studentParentAssociations`, or any Descriptors.

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
lightbeam send -c path/to/config.yaml -p '{"CLIENT_ID":"populated", "CLIENT_SECRET":"populatedSecret"}'
lightbeam send -c path/to/config.yaml --params '{"CLIENT_ID":"populated", "CLIENT_SECRET":"populatedSecret"}'
```
Command-line parameters override any environment variables of the same name.

## State
This tool *maintains state about payloads previously dispatched to the Ed-Fi API* to avoid repeatedly resending the same payloads. This is done by maintaining a [pickled](https://docs.python.org/3/library/pickle.html) Python dictionary of payload hashes for each Ed-Fi resource and descriptor, together with a timestamp and HTTP status code of the last response. The files are located in the [config](#setup) file's `state_dir` and have names like `{resource}.dat` or `{descriptor}.dat`.

By default, only new, never-before-seen payloads are `sent` or `deleted`.

You may choose to resend payloads last sent before *timestamp* using the `-t` or `--older-than` command-line flag:
```bash
lightbeam send -c path/to/lightbeam.yaml -t 2020-12-25T00:00:00
lightbeam send -c path/to/lightbeam.yaml --older-than 2020-12-25T00:00:00
```
Or you may choose to resend payloads last sent after *timestamp* using the `-n` or `--newer-than` command-line flag:
```bash
lightbeam send -c path/to/lightbeam.yaml -n 2020-12-25T00:00:00
lightbeam send -c path/to/lightbeam.yaml --newer-than 2020-12-25T00:00:00
```
Or you may choose to resend payloads that returned a certain HTTP status code(s) on the last send using the `-r` or `--retry-status-codes` command-line flag:
```bash
lightbeam send -c path/to/lightbeam.yaml -r 200,201
lightbeam send -c path/to/lightbeam.yaml --retry-status-codes 200,201
```
These three options may be composed; `lightbeam` will resend payloads that match any conditions (logical OR).

Finally, you can ignore prior state and resend all payloads using the `-f` or `--force` flag:
```bash
lightbeam send -c path/to/lightbeam.yaml -f
lightbeam send -c path/to/lightbeam.yaml --force
```

## Cache
To reduce runtime, `lightbeam` caches the resource and descriptor Swagger docs it fetches from your Ed-Fi API as well as the descriptor values for up to a month. This way, the data does not have to be re-loaded from your API on every run. The cached files are stored in the `cache` directory within your `state_dir`. You may run `lightbeam` with the `-w` or `--wipe` flag to clear this cached data and force re-fetching the API metadata:
```bash
lightbeam send -c path/to/config.yaml -w
lightbeam send -c path/to/config.yaml --wipe
```

## Structured output of run results
To produce a JSON file with metadata about the run, invoke lightbeam with
```bash
lightbeam send -c path/to/config.yaml --results-file ./results.json
```
A sample results file could be:

```json
{
    "started_at": "2023-06-08T17:18:25.053207",
    "working_dir": "/home/someuser/",
    "config_file": "/home/someuser/lightbeam.yml",
    "data_dir": "/home/someuser/data/",
    "api_url": "https://some-ed-fi-api.edu/api",
    "namespace": "ed-fi",
    "resources": {
        "studentSchoolAssociations": {
            "records_processed": 50,
            "records_skipped": 0,
            "records_failed": 22,
            "failures": [
                {
                    "status_code": 400,
                    "message": "The request is invalid. \"request.schoolReference.schoolId\": [ \"JSON integer 1234567899999 is too large or small for an Int32. Path 'schoolReference.schoolId', line 1, position 328.\" ]",
                    "file": "/home/someuser/data/studentSchoolAssociations.jsonl",
                    "line_numbers": "6,4,5,7,8",
                    "count": 5
                },
                {
                    "status_code": 400,
                    "message": "Validation of 'StudentSchoolAssociation' failed. Student reference could not be resolved.",
                    "file": "/home/someuser/data/studentSchoolAssociations.jsonl",
                    "line_numbers": "1,3,2",
                    "count": 3

                },
                {
                    "status_code": 409,
                    "message": "The value supplied for the related 'studentschoolassociation' resource does not exist."
                    "file": "/home/someuser/data/studentSchoolAssociations.jsonl",
                    "line_numbers": "9,10,12,14,16,13,11,15,17,18,19,21,22,20",
                    "count": 14
                }
            ],
            "successes": [
                {
                    "status_code": 201,
                    "count": 28
                }
            ],
            "records_processed": 50,
            "records_skipped": 0,
            "records_failed": 22
        }
    },
    "completed_at": "2023-06-08T17:18:26.724699",
    "runtime_sec": 1.671492,
    "total_records_processed": 50,
    "total_records_skipped": 0,
    "total_records_failed": 22
}
```


# Design
Some details of the design of this tool are discussed below.

## Resource-dependency ordering
JSONL files are sent to the Ed-Fi API in resource-dependency order, which avoids "missing reference" API errors when populating multiple endpoints.

## Asynchronous requests
`lightbeam` achieves exceptional performance by making _asynchronous_ requests to the Ed-Fi API - up to `connection.pool_size` (in your [YAML configuration](#setup)) at a time.


# Performance & Limitations
Tool performance depends on primarily on the performance of the Ed-Fi API, which in turn depends on the compute resources which back it. Typically the bottleneck is write performance to the database backend (SQL server or Postgres). If you use `lightbeam` to ingest a large amount of data into an Ed-Fi API (not a recommended use-case), consider temporarily scaling up your database backend.

For reference, we have achieved throughput rates in excess of 100 requests/second against an Ed-Fi ODS & API running in Docker on a laptop.


# Changelog
See [CHANGELOG](CHANGELOG.md).



# Contributing
Bugfixes and new features (such as additional transformation operations) are gratefully accepted via pull requests here on GitHub.

## Contributions
* Cover image created with [DALL &bull; E mini](https://huggingface.co/spaces/dalle-mini/dalle-mini)


# License
See [License](LICENSE).
