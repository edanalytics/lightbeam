### v0.1.11
<details>
<summary>Released 2026-02-17</summary>
* feature: `namespace_overrides` to allow sending resources to different namespaces in one `lightbeam send`
</details>

### v0.1.10
<details>
<summary>Released 2025-11-21</summary>
* bugfix: `lightbeam send` not writing >=7.2 API error messages when error payload does not contain `errors` nor `validationErrors`
* fix: invalid YAML for `count.separator` in docs example `lightbeam.yml`
</details>

### v0.1.9
<details>
<summary>Released 2025-09-26</summary>
* feature: support new and improved error messages [introduced in Ed-Fi API 7.2](https://docs.ed-fi.org/reference/ods-api/7.2/whats-new/whats-new-in-this-release/#enhanced-api-error-handling) by @tomreitz in https://github.com/edanalytics/lightbeam/pull/80
* feature: warn and skip fetching if `--query` keys are not valid parameters for an endpoint by @tomreitz in https://github.com/edanalytics/lightbeam/pull/81
* bugfix: fix a `KeyError` if `connection.verify_ssl` is not set in `lightbeam.yml` by @tomreitz in https://github.com/edanalytics/lightbeam/pull/79
</details>

### v0.1.8
<details>
<summary>Released 2025-07-11</summary>
* bugfix: subarray uniqueness bug by @tomreitz in https://github.com/edanalytics/lightbeam/pull/76
* bugfix: GET deletes by identity, not required params by @tomreitz in https://github.com/edanalytics/lightbeam/pull/68
* feature: implement case flexibility for data discovery by @tomreitz in https://github.com/edanalytics/lightbeam/pull/31 and https://github.com/edanalytics/lightbeam/pull/77
* feature: implementation of new `lightbeam create` functionality by @tomreitz in https://github.com/edanalytics/lightbeam/pull/56
* feature: implementing validate references selector, behavior and remote switch, plus update docs by @tomreitz in https://github.com/edanalytics/lightbeam/pull/61
</details>

### v0.1.7
<details>
<summary>Released 2025-05-08</summary>

* bugfix: discovery of data in subfolders of `data_dir` by @tomreitz in https://github.com/edanalytics/lightbeam/pull/66
* bugfix: structured results record counts by @tomreitz in https://github.com/edanalytics/lightbeam/pull/60
* bugfix: validation error message shows the wrong number of records by @johncmerfeld in https://github.com/edanalytics/lightbeam/pull/65
* feature: adds a `--set` flag to the CLI to enable overriding config values in `lightbeam.yml` by @tomreitz in https://github.com/edanalytics/lightbeam/pull/63
* bugfix: `validate` uniqueness fixes and recurse into array elements, other improvements by @tomreitz in https://github.com/edanalytics/lightbeam/pull/67
* bugfix: eplace obsolete links to Ed-Fi Tech Docs by @stephenfuqua in https://github.com/edanalytics/lightbeam/pull/70
* bugfix: self-reference validation by @tomreitz in https://github.com/edanalytics/lightbeam/pull/71
* feature: protocol (http/https) fall-back for discovery URLs if not found by @ryanaguilar in https://github.com/edanalytics/lightbeam/pull/74
</details>

### v0.1.6
<details>
<summary>Released 2024-11-15</summary>

* feature: [implement structured results file output for validate](https://github.com/edanalytics/lightbeam/pull/52)
* bugfix: [`--results-file` required a directory prefix](https://github.com/edanalytics/lightbeam/pull/57)
* bugfix: [`validate` of descriptor values without a hash char (`#`) failed](https://github.com/edanalytics/lightbeam/pull/53)
* bugfix: [validate uniqueness by identity instead of required](https://github.com/edanalytics/lightbeam/pull/54)
* bugfix: [allow deletion of students (if permitted by credential claimset and API auth strategy)](https://github.com/edanalytics/lightbeam/pull/55)
* bugfix: [make `max_failures` truly optional](https://github.com/edanalytics/lightbeam/pull/58)
</details>

### v0.1.5
<details>
<summary>Released 2024-07-23</summary>

* feature: [add a `successes` section to the structured log results file to report how many payloads resulted in a 200, 201, etc.](https://github.com/edanalytics/lightbeam/pull/47)
* feature: [add a test suite](https://github.com/edanalytics/lightbeam/pull/45)
* bugfix: [lightbeam send resulted in an error about `lightbeam.get_endpoints_with_data()`](https://github.com/edanalytics/lightbeam/pull/48)
* bugfix: [restore endpoint dependency ordering to `lightbeam.get_endpoints_with_data()`](https://github.com/edanalytics/lightbeam/pull/49)
* bugfix: [custom API URLs not picked up when some, but not all, are specified](https://github.com/edanalytics/lightbeam/pull/46)
</details>

### v0.1.4
<details>
<summary>Released 2024-07-12</summary>

* feature: [reference validation](https://github.com/edanalytics/lightbeam/pull/30)
* feature: [notify user of endpoints to be `delete`d](https://github.com/edanalytics/lightbeam/pull/37)
* bugfix: [handle descriptors with missing (optional) description](https://github.com/edanalytics/lightbeam/pull/38)
</details>

### v0.1.3
<details>
<summary>Released 2024-06-03</summary>

* feature: [Support custom Ed-Fi URLs](https://github.com/edanalytics/lightbeam/pull/33)
* bugfix: [Fix `--drop-keys` and `--keep-keys` filtering](https://github.com/edanalytics/lightbeam/pull/32)
* bugfix: [Ensure command list in help menu and log output is always consistent](https://github.com/edanalytics/lightbeam/pull/27)
* bugfix: [Fix how hashlog entries are removed during `lightbeam delete`](https://github.com/edanalytics/lightbeam/pull/34)
* bugfix: [Fix validation of local descriptor values within nested payloads](https://github.com/edanalytics/lightbeam/pull/29)
* bugfix: [No descriptor payload keys were kept after being downloaded by `lightbeam validate`](https://github.com/edanalytics/lightbeam/pull/28)
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
