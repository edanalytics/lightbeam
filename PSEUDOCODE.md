# Lightbeam commands

  - `API` is an abstract Ed-Fi client
  - Other values in `ALL_CAPS` are runtime or configuration parameters
  - Most interactions with the ODS include the following retry logic:
```python
if resp.status == "401":
    if auth_token_is_stale():
        API.authenticate()
    else:
        sleep(1)
    retry()
```

**`validate`**

Uses Swagger and the ODS's descriptor endpoints to validate the structure and contents of local data. The validator ensures that each local entry:
  1. Is valid JSON
  2. Adheres to the Swagger schema for the entry's endpoint
  3. Contains descriptor values that appear in the ODS's descriptor endpoints
  4. Is unique in its natural key

```python
API.authenticate()

# local_descriptors are used for caching but the swagger is still source of truth
local_descriptors = []
remote_descriptors = []
for endpoint in SELECTED_ENDPOINTS:
    if endpoint.is_descriptor():
        data_files = get_local_data(endpoint)
        for file in data_files:
            for entry in file:
                local_descriptors.add(entry)

        resp = API.GET(endpoint)
        remote_descriptors.add(*resp.data)

for endpoint in SELECTED_ENDPOINTS:
    # a bit hand-wavey but I think this is a fair abstraciton
    schema = SWAGGER[endpoint]
    identifying_cols = get_required_params_from_swagger(endpoint)
    data_files = get_local_data(endpoint)
    unique_entrys = {}

    for file in data_files:
        for entry in file:
            assert valid_structure(entry, schema)
            try: 
                assert valid_descriptors(entry, local_descriptors)
            except:
                assert valid_descriptors(entry, remote_descriptors)

            h = hash(entry[[identifying_cols]])
            assert h not in unique_entries
            unique_entries.add(h)
```

**`send`**

POSTs local data to the ODS. Uses a local log of data hashes to ensure uniqueness in the destination data but this can be overriden

```python
API.authenticate()

for endpoint in SELECTED_ENDPOINTS:
    data_files = get_local_data(endpoint)
    for file in data_files:
        for entry in file:
            h = hash(entry)
            # local checks to guarantee uniqueness
            if reasons_not_to_post(h):
                continue

            API.POST(endpoint, entry)
            # warn of failures
            # retry() if necessary

```

**`(validate+send)`**

Validates and then sends local data

**`delete`**

DELETEs local data from the ODS. Uses the local data's natural keys to query the ODS for their surrogate keys. Then uses the surrogate keys to delete those records from the ODS

```python
API.authenticate()

for endpoint in SELECTED_ENDPOINTS:
    if endpoint.is_descriptor():
        descriptor_vals = fetch(endpoint)
    
    identifying_cols = get_required_params_from_swagger(endpoint)
    data_files = get_local_data(endpoint)
    for file in data_files:
        for entry in file:
            h = hash(entry)
            # local checks, mostly depends on when data was posted
            if reasons_not_to_delete(h):
                continue

            params = entry[[identifying_cols]]

            if endpoint.is_descriptor():
                if params in descriptor_vals:
                    delete_with_retry(
                        endpoint, 
                        id=descriptor_vals[[identifying_cols]]["id"]
                    )
                # retry() if necessary
                continue

            resp = API.GET(endpoint, query=params)
            # retry() if necessary
            for record in resp.data:
                API.DELETE(endpoint, id=record.id)
                # retry() if necessary

```

**`truncate`**

DELETEs all records from ODS endpoints. Works the same way as `delete`, but queries for all records in the ODS instead of just those that exist locally

```python
API.authenticate()
# contains a map from endpoint -> records based on SELECTED_ENDPOINTS
for endpoint in SELECTED_ENDPOINTS:
    selected_data = fetch()
    for record in selected_data:
        API.DELETE(endpoint, id=record.id)
    # retry() if necessary
```

**`count`**

Returns a count of records in the ODS

```python
API.authenticate()

def count():
    results = {}
    for endpoint in SELECTED_ENDPOINTS:
        resp = API.GET(
                endpoint,
                params={"limit": "0", "totalCount": "true"}
            )
        results[endpoint] = resp['Total-Count']

    return results

results = count()
write(results)
```

**`fetch`**

GETs data from the ODS. Can be filtered to keep or drop specific keys.

```python
API.authenticate()

def fetch():
    results = []
    record_counts = count()

    for endpoint in SELECTED_ENDPOINTS:
        records = []
        num_pages = record_counts[endpoint] / PAGE_SIZE

        for i in range(num_pages):
            resp = API.GET(page=i, query=QUERY_PARAMS)
            # retry() if necessary

            records.add(*resp.data)

        records.keys() = KEYS_TO_KEEP # or `not KEYS_TO_DROP`

        results.add({
                endpoint,
                records
            })
    return results

results = fetch()
write(results)
```