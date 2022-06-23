import os
import json
import time
import yaml
import pickle
import dateutil.parser
from datetime import datetime
from yaml.loader import SafeLoader
import hashlib
from glob import glob

import requests
from requests.adapters import HTTPAdapter, Retry
import logging
import hashlib
import asyncio
import aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry
from urllib3 import PoolManager

from edfi_api_client import EdFiBase, EdFiClient


parameters = {}

# This allows us to determine the YAML file line number for any element loaded from YAML
# (very useful for debugging and giving meaningful error messages)
# (derived from https://stackoverflow.com/a/53647080)
# Also added env var interpolation based on
# https://stackoverflow.com/questions/52412297/how-to-replace-environment-variable-value-in-yaml-file-to-be-parsed-using-python#answer-55301129
class SafeLineEnvVarLoader(SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = super(SafeLineEnvVarLoader, self).construct_mapping(node, deep=deep)

        # swap in and expand vars:
        global env_copy
        global env_saved
        os.environ = env_copy
        for k,v in mapping.items():
            if isinstance(v, str):
                mapping[k] = os.path.expandvars(v)
        # return environment to original
        os.environ = env_saved

        # Add 1 so line numbering starts at 1
        mapping['__line__'] = node.start_mark.line + 1
        return mapping


# from https://gist.github.com/miku/dc6d06ed894bc23dfd5a364b7def5ed8
class dotdict(dict):
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in self.items():
            if isinstance(v, dict): self[k] = dotdict(v)
    def lookup(self, dotkey):
        path = list(reversed(dotkey.split(".")))
        v = self
        while path:
            key = path.pop()
            if isinstance(v, dict): v = v[key]
            elif isinstance(v, list): v = v[int(key)]
            else: raise KeyError(key)
        return v


class Lightbeam:

    version = "0.0.1"
    config_defaults = {
        "state_dir": os.path.join(os.path.expanduser("~"), ".lightbeam", ""),
        "source_dir": "./",
        "edfi_api_base": "https://localhost/api",
        "edfi_api_mode": "year_specific",
        "edfi_api_year": datetime.today().year,
        "edfi_api_client_id": "populated",
        "edfi_api_client_secret": "populatedSecret",
        "connection_pool_size": 8,
        "num_retries": 10,
        "backoff_factor": 1.5,
        "status_forcelist": [429, 500, 501, 503, 504],
        "verbose": False,
        "show_stacktrace": False
    }
    
    def __init__(self, config_file, params="", older_than="", newer_than="", retry_status_codes=""):
        self.config_file = config_file
        self.t0 = time.time()
        self.memory_usage = 0
        self.dispatcher_status_counts = {}
        self.dispatcher_errors = 0
        self.params = params
        self.older_than=older_than
        self.newer_than=newer_than
        self.retry_status_codes=retry_status_codes

        parameters = {}
        if params!="": parameters = json.loads(params)
        global env_copy
        env_copy = os.environ.copy() # make a copy of environment vars
        if isinstance(parameters, dict): # add in any CLI params
            for k,v in parameters.items():
                env_copy[k] = v
        global env_saved
        env_saved = os.environ # save original copy of environment vars

        # load & parse config YAML:
        with open(config_file, "r") as stream:
            try:
                user_config = yaml.load(stream, Loader=SafeLineEnvVarLoader)
            except yaml.YAMLError as e:
                raise Exception(self.error_handler.ctx + "YAML could not be parsed: {0}".format(e))
        
        if isinstance(user_config, dict):
            self.config = dotdict(self.merge_config(user_config, self.config_defaults))
        else: self.config = dotdict(self.config_defaults)

    def merge_config(self, user, default):
        if isinstance(user, dict) and isinstance(default, dict):
            for k, v in default.items():
                if k not in user:
                    user[k] = v
                else:
                    user[k] = self.merge_config(user[k], v)
        return user
    
    # sort destinations by Ed-Fi dependency-order:
    def get_sorted_endpoints(self, endpoints):
        with open(f"lightbeam/resources/ed-fi-ordered-dependencies.txt", 'r') as file:
            deps = file.readlines()
            deps = [k.strip() for k in deps]
            sorted = [dep for dep in deps if dep in endpoints]
            return sorted

    def dispatch(self, selector="*"):
        if not os.path.isdir(self.config.data_dir):
            print("FATAL: `data_dir` {0} is not a directory".format(self.config.data_dir))
            exit(1)
        endpoints = []
        for f_name in glob(os.path.join(self.config.data_dir, '*.jsonl')):
            endpoints.append(f_name.replace(".jsonl", "").split("/")[-1:][0])
        if len(endpoints)==0:
            print("FATAL: `data_dir` {0} has no *.jsonl files".format(self.config.data_dir))
            exit(1)
        endpoints = self.get_sorted_endpoints(endpoints) # make sure we process destinations in Ed-Fi dependency order
        if len(endpoints)==0:
            print("FATAL: `data_dir` {0} has no *.jsonl files that match an Ed-Fi resource or descriptor name".format(self.config.data_dir))
            exit(1)
        # filter down to only selected endpoints
        if selector!="*" and selector!="":
            selected_endpoints = selector.split(",")
            endpoints = [e for e in endpoints if e in selected_endpoints]
        
        logging.captureWarnings(True) # turn off annoying SSL warnings (is this DANGEROUSSSS??)

        if self.older_than!='': self.older_than = dateutil.parser.parse(self.older_than).timestamp()
        if self.newer_than!='': self.newer_than = dateutil.parser.parse(self.newer_than).timestamp()
        if self.retry_status_codes!='': self.retry_status_codes = [int(code) for code in self.retry_status_codes.split(",")]

        # using the EA's edfi_api_client library:
        # edfi_client = EdFiClient(
        #     self.config.dispatch.edfi_api.base_url,
        #     self.config.dispatch.edfi_api.client_id,
        #     self.config.dispatch.edfi_api.client_secret,
        #     api_year=2022,
        #     verbose=True
        # )
        # # print(edfi_client.get_info())
        # self.edfi_client = edfi_client
        
        # print(edfi.total_rows)

        # we need an API token first:
        token_response = requests.post(
                        self.config.edfi_api_base_url,
                        data={"grant_type":"client_credentials"},
                        auth=(self.config.edfi_api_client_id, self.config.edfi_api_client_secret),
                        verify=False)
        print(token_response)
        token = token_response.json()["access_token"]
        self.profile("(using OAuth token {0})".format(token))

        # these headers are sent with every subsequent API POST request:
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "authorization": "Bearer " + token
        }

        for endpoint in endpoints:
            self.profile("sending endpoint {0} ...".format(endpoint))
            asyncio.run(self.do_dispatch(endpoint, headers, older_than, newer_than, retry_status_codes))
            self.profile("finished endpoint {0}! (status counts: {1}) ".format(endpoint, str(self.dispatcher_status_counts)))

    async def do_dispatch(self, destination, headers, older_than, newer_than, retry_status_codes):
        timeout = aiohttp.ClientTimeout(total=600)
        connector = aiohttp.connector.TCPConnector(limit=self.config.dispatch.connection_pool_size)
        retry_options = ExponentialRetry(
            attempts=self.config.dispatch.num_retries,
            factor=self.config.dispatch.backoff_factor,
            statuses=self.config.dispatch.status_forcelist
        )
        async with RetryClient(timeout=timeout, retry_options=retry_options, connector=connector, headers=headers) as client:
        
            # We try to be smart and avoid re-POSTing JSON we've already (successfully) sent.
            # This is done by storing a few things in a file we call a hashlog:
            # - the hash of the JSON (so we can recognize it in the future)
            # - the timestamp of the last send of this JSON
            # - the returned status code for the last send
            # Using these logs, we can do things like retry JSON that previously failed, resend JSON older than a certain age, etc.
            hashlog_file = os.path.join(self.config.state_dir, f"{destination}.dat")
            self.hashlog = self.load_hashlog(hashlog_file)
            
            file_name = self.config.generate.output_dir + destination + ".jsonl"
            tasks = []
            with open(file_name) as file:
                num_skipped = 0
                counter = 0
                for line in file:
                    data = line.strip()
                    hash = 0
                    # compute hash of current row:
                    hash = hashlib.md5(data.encode()).digest()
                    # check if we've posted this data before:
                    counter += 1
                    if hash in self.hashlog.keys():
                        # check if the last post meets criteria for a resend:
                        if (
                            (older_than!='' and self.hashlog[hash][0]<older_than)
                            or (newer_than!="" and self.hashlog[hash][0]>newer_than)
                            or (len(retry_status_codes)>0 and self.hashlog[hash][1] in retry_status_codes)
                        ):
                            tasks.append(asyncio.ensure_future(self.do_post(destination, data, self.edfi_client, counter, hash)))
                        else:
                            num_skipped += 1
                            continue
                if num_skipped>0:
                    self.profile("skipped {0} of {1} payloads because they were previously processed and did not match any resend criteria".format(num_skipped, counter))
            await self.gather_with_concurrency(self.config.dispatch.connection_pool_size, *tasks) # execute them concurrently
            self.save_hashlog(hashlog_file, self.hashlog)
    
        if self.dispatcher_errors > 10:
            raise Exception("more than 10 errors, terminating. Please review the errors, fix data errors or network conditions, and dispatch again.")

    async def do_post(self, endpoint, data, client, line, hash):
        try:
            async with client.post(self.config.dispatch.edfi_api.base_url + "/" + endpoint, data=data, ssl=False) as response:
                body = await response.text()
                status = str(response.status)
                if status not in self.dispatcher_status_counts: self.dispatcher_status_counts[status] = 1
                else: self.dispatcher_status_counts[status] += 1
                file_name = self.config.generate.output_dir + endpoint + ".jsonl"
                if response.status not in [ 200, 201 ]:
                    self.profile("  error with line {0} of {1}; PAYLOAD: {2}; RESPONSE: {3}".format(line, file_name, data, body))
                    self.dispatcher_errors += 1
                if not self.skip_change_check:
                    self.hashlog[hash] = (round(time.time()), response.status)
            # async with client.post(destination,data) as response:
            #     print(response)
            #     body = await response.text()
            #     status = str(response.status)
            #     if status not in self.dispatcher_status_counts: self.dispatcher_status_counts[status] = 1
            #     else: self.dispatcher_status_counts[status] += 1
            #     file_name = self.config.generate.output_dir + destination + ".jsonl"
            #     if response.status not in [ 200, 201 ]:
            #         self.profile("  error with line {0} of {1}; PAYLOAD: {2}; RESPONSE: {3}".format(line, file_name, data, body))
            #         self.dispatcher_errors += 1
            #     if not self.skip_change_check:
            #         self.hashlog[hash] = (round(time.time()), response.status)
        except Exception as e:
            self.dispatcher_errors += 1
            print(e)
            self.profile("  (at line {0} of {1}; PAYLOAD: {2} )".format(line, file_name, data))


    async def gather_with_concurrency(self, n, *tasks):
        semaphore = asyncio.Semaphore(n)
        async def sem_task(task):
            async with semaphore:
                return await task
        return await asyncio.gather(*(sem_task(task) for task in tasks))
    
    def load_hashlog(self, hashlog_file):
        hashlog = {}
        if os.path.isfile(hashlog_file):
            with open(hashlog_file, 'rb') as f:
                hashlog = pickle.load(f)
        return hashlog

    def save_hashlog(self, hashlog_file, hashlog):
        if not os.path.isdir(self.config.state_dir):
            os.mkdir(self.config.state_dir)
        with open(hashlog_file, 'wb') as f:
            pickle.dump(hashlog, f)