### Unreleased
<details>

* bugfix: [Ensure command list in help menu and log output is always consistent](https://github.com/edanalytics/lightbeam/pull/27)
* bugfix: Fix how hashlog entries are removed during `lightbeam delete`
</details>

### v0.1.2
<details>
<summary>Released 2024-04-19</summary>

* feature: [Add ability for fetch `--keep-keys` and `--drop-keys` flags to allow wildcard matching](https://github.com/edanalytics/lightbeam/pull/23)
* feature: [Update structured logging to be flatter, per recent team discussion](https://github.com/edanalytics/lightbeam/pull/24)
* bugfix: [Support for `definitions`being renamed to `components.schemas` in Ed-Fi 7.1 Swagger](https://github.com/edanalytics/lightbeam/pull/25)
</details>

### v0.1.1
<details>
<summary>Released 2024-02-16</summary>

* bugfix: [replace single quotes in logging message with backticks](https://github.com/edanalytics/lightbeam/pull/18)
* bugfix: [fetching resources without read permission](https://github.com/edanalytics/lightbeam/pull/20)
</details>

### v0.1.0
<details>
<summary>Released 2023-10-16</summary>

* feature: [adding `lightbeam count` and `lightbeam fetch`, with other bugfixes and improvements](https://github.com/edanalytics/lightbeam/pull/17)
* bugfix: [typo in descriptor CSV header](https://github.com/edanalytics/lightbeam/pull/16)
</details>

### v0.0.8
<details>
<summary>Released 2023-07-11</summary>

* bugfix: [fixing a bug to create the results_file directory if needed](https://github.com/edanalytics/lightbeam/pull/14)
</details>

### v0.0.7
<details>
<summary>Released 2023-06-13</summary>

* bugfix: [fixing a bug with Ed-Fi 6.1 API's dependencies](https://github.com/edanalytics/lightbeam/pull/9)
* bugfix: [fixing a bug with per-request timeout](https://github.com/edanalytics/lightbeam/pull/11)
* feature: [adding an option to produce structured output](https://github.com/edanalytics/lightbeam/pull/10)
* feature: [adding skip exit code](https://github.com/edanalytics/lightbeam/pull/12)
</details>

### v0.0.6
<details>
<summary>Released 2023-04-07</summary>

* bugfix: resolve error fetching Swagger docs
</details>

### v0.0.5
<details>
<summary>Released 2023-04-07</summary>

* bugfix: better error logging (file name and line number) for erroring payloads
* bugfix: better error handling in cases where the Ed-Fi API dependencies and Swagger URLs return error status codes
</details>

### v0.0.4
<details>
<summary>Released 2023-01-25</summary>

* bugfix: fetching descriptor values for all namespaces, not just `ed-fi`
</details>

### v0.0.3
<details>
<summary>Released 2023-01-12</summary>

* bugfix: add pagination when fetching descriptor values
</details>

### v0.0.2
<details>
<summary>Released 2022-12-16</summary>

* un-pin requirements.txt dependencies from fixed versions
</details>

### v0.0.1
<details>
<summary>Released 2022-09-22</summary>

* initial release
</details>