# Lightbeam commands

Values in `ALL_CAPS` are runtime parameters

**`validate`**

1. Checks that what you’re sending matches the structure as defined by the swagger docs
    1. Makes a get request to the API’s swagger docs to get a list of endpoints
    2. And then possibly makes requests for the swagger doc from each of those endpoints? Don’t totally understand how this works though
2. Then also checks that what you’re sending doesn’t use any invalid descriptor values
    1. Tries to use what’s cached, but otherwise hits al the descriptor endpoints and pulls out all the codes
    
**`send`**

1. Sends data one line at a time(?), using hashing to try to guarantee that data is not re-posted
2. Boils down to a bunch of post requests to all the endpoints for which there is local data saved

**`(validate+send)`**

**`delete`**

1. Deletes records found in local data from their corresponding endpoints
2. Needs to key those remote records on something, traverses swagger to get this natural key (how?)…
3. **come back to this, need to draw it out or something

**`truncate`**

```python
# {endpoint, records}
selected_data = fetch() # based on SELECTED_ENDPOINTS
for record in selected_data:
    DELETE(record.endpoint, id = record.id)
```

**`count`**

```python
def count():
    results = []
    for endpoint in SELECTED_ENDPOINTS:
        resp = GET(endpoint)
        results.add({
                endpoint,
                resp['Total-Count']
            })
    return results
```

**`fetch`**

```python
def fetch():
    results = []
    for endpoint in SELECTED_ENDPOINTS:
        records = []
        for page in endpoint.pages:
            resp = GET(page, query = QUERY_PARAMS)
            records.add(*resp.data)

        records.keys() = KEYS_TO_KEEP # or NOT KEYS_TO_DROP

        results.add({
                endpoint,
                records
            })
    return results
```

Seems to map to get() in edfi_api_client