import re
import sys
import json
import asyncio
import aiohttp
from requests.adapters import HTTPAdapter, Retry
from aiohttp_retry import RetryClient, ExponentialRetry

# Strips newlines from a string
def linearize(string: str) -> str:
    exp = re.compile(r"\s+")
    return exp.sub(" ", string).strip()

# Merges two (potentially nested) dict structures, such as a default + custom config
def merge_dicts(user, default):
    if isinstance(user, dict) and isinstance(default, dict):
        for k, v in default.items():
            if k not in user:
                user[k] = v
            else:
                user[k] = merge_dicts(user[k], v)
    return user

# Converts (for example) `LocalEducationAgencies` to `LocalEducationAgency`; `students` to `student`; etc.
def singularize_endpoint(endpoint):
    if endpoint[-3:]=="ies": return endpoint[0:-3] + "y"
    else: return endpoint[0:-1]

# Takes a params structure and interpolates values from a (string) JSON payload
def interpolate_params(params_structure, payload):
    params = {}
    for k,v in params_structure.items():
        value = json.loads(payload)
        for key in v.split('.'):
            value = value[key]
        params[k] = value
    return params

# Returns a client object with exponential retry and other parameters per configs
def get_retry_client(connection_config, token):
    return RetryClient(
        timeout=aiohttp.ClientTimeout(total=connection_config["timeout"]),
        retry_options=ExponentialRetry(
            attempts=connection_config["num_retries"],
            factor=connection_config["backoff_factor"],
            statuses=connection_config["retry_statuses"]
            ),
        connector=aiohttp.connector.TCPConnector(limit=connection_config["pool_size"]),
        headers={
                "accept": "application/json",
                "Content-Type": "application/json",
                "authorization": "Bearer " + token
            }
        )
