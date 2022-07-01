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

from jsonschema import RefResolver
from jsonschema import Draft4Validator

import requests
from requests.adapters import HTTPAdapter, Retry
import logging
import hashlib
import asyncio
import aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry
from urllib3 import PoolManager

# from edfi_api_client import EdFiBase, EdFiClient


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
        "validate": False,
        "swagger": "",
        "edfi_api": {
            "base_url": "https://localhost/api",
            "version": 3,
            "mode": "year_specific",
            "year": datetime.today().year,
            "client_id": "populated",
            "client_secret": "populatedSecret"
        },
        "connection": {
            "pool_size": 8,
            "timeout": 60,
            "num_retries": 10,
            "backoff_factor": 1.5,
            "retry_statuses": [429, 500, 501, 503, 504],
        },
        "verbose": False,
        "show_stacktrace": False
    }
    
    def __init__(self, config_file, params="", force=False, older_than="", newer_than="", resend_status_codes=""):
        self.config_file = config_file
        self.t0 = time.time()
        self.memory_usage = 0
        self.status_counts = {}
        self.errors = 0
        self.params = params
        self.force = force
        self.older_than=older_than
        self.newer_than=newer_than
        self.resend_status_codes=resend_status_codes

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
    
    def profile(self, msg, force=False):
        t = time.time()
        if self.config.verbose or force: print(str(t-self.t0) + "\t" + msg)
    
    # sort destinations by Ed-Fi dependency-order:
    def get_sorted_endpoints(self, endpoints):
        response = requests.get(
            self.config.edfi_api.dependencies_url,
            verify=self.config.connection.verify_ssl)
        ordered_endpoints = []
        for e in response.json():
            ordered_endpoints.append(e["resource"].replace("/ed-fi/", ""))

        return [e for e in ordered_endpoints if e in endpoints]
    
    def validate(self, swagger, endpoint):
        if endpoint[-3:]=="ies": definition = "edFi_" + endpoint[0:-3] + "y"
        else: definition = "edFi_" + endpoint[0:-1]
        resource_schema = swagger["definitions"][definition]

        resolver = RefResolver("test", swagger, swagger)
        validator = Draft4Validator(resource_schema, resolver=resolver)

        jsonl_file_name = self.config.data_dir + endpoint + ".jsonl"
        self.profile(f"validating {jsonl_file_name} against {definition} schema...")
        with open(jsonl_file_name) as f:
            counter = 0
            errors = 0
            for line in f:
                counter += 1
                try:
                    instance = json.loads(line)
                except Exception as e:
                    self.profile(f"... VALIDATION ERROR (line {counter}): invalid JSON" + str(e).replace(" line 1",""), True)
                    errors += 1
                    continue

                try:
                    validator.validate(instance)
                except Exception as e:
                    if errors < 10:
                        e_path = [str(x) for x in list(e.path)]
                        context = ""
                        if len(e_path)>0: context = " in " + " -> ".join(e_path)
                        self.profile(f"... VALIDATION ERROR (line {counter}): " + str(e.message) + context, True)
                    errors += 1
                    continue
            
            if errors==0: self.profile(f"... all lines validate ok!")
            else:
                num = errors - 10
                if errors > 10: self.profile(f"... and {num} others!", True)
                self.profile(f"... VALIDATION ERRORS on {errors} of {counter} lines in {jsonl_file_name}; see details above.", True)
                exit(1)


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
        
        logging.captureWarnings(True) # turn off annoying SSL warnings (is this DANGEROUSSSS??)

        if self.older_than!='': self.older_than = dateutil.parser.parse(self.older_than).timestamp()
        if self.newer_than!='': self.newer_than = dateutil.parser.parse(self.newer_than).timestamp()
        if self.resend_status_codes!='': self.resend_status_codes = [int(code) for code in self.resend_status_codes.split(",")]

        # using the EA's edfi_api_client library:
        # edfi_client = EdFiBase(
        #     base_url=self.config.edfi_api_base_url,
        #     client_key=self.config.edfi_api_client_id,
        #     client_secret=self.config.edfi_api_client_secret,
        #     api_version=self.config.edfi_api_version,
        #     api_year=self.config.edfi_api_year,
        #     api_mode=self.config.edfi_api_mode,
        #     instance_code=self.config.edfi_api_instance_code or None
        # )
        # print(edfi_client.get_info())
        # session = edfi_client.get_conn()
        # print(session)
        # print(session.headers)
        # self.edfi_client = edfi_client

        # filter down to only selected endpoints
        if selector!="*" and selector!="":
            if "," in selector:
                selected_endpoints = selector.split(",")
                endpoints = [e for e in endpoints if e in selected_endpoints]
            else: endpoints = [ selector ]

        # do validation:
        if self.config.validate:
            swagger_file_name = self.config.swagger
            if "http://" in self.config.swagger or "https://" in self.config.swagger:
                swagger = json.loads(requests.get(self.config.swagger).text)
            else:
                swagger = json.load(open(swagger_file_name))
            for endpoint in endpoints:
                self.validate(swagger, endpoint)

        api_base = requests.get(self.config.edfi_api.base_url, verify=self.config.connection.verify_ssl).json()
        self.config.edfi_api.oauth_url = api_base["urls"]["oauth"]
        self.config.edfi_api.dependencies_url = api_base["urls"]["dependencies"]
        self.config.edfi_api.data_url = api_base["urls"]["dataManagementApi"] + 'ed-fi/'
        
        # make sure we process destinations in Ed-Fi dependency order
        endpoints = self.get_sorted_endpoints(endpoints)
        if len(endpoints)==0:
            print("FATAL: `data_dir` {0} has no *.jsonl files that match an Ed-Fi resource or descriptor name".format(self.config.data_dir))
            exit(1)

        # get token with which to send requests
        self.do_oauth()

        # create state_dir if it doesn't exist:
        state_dir = os.path.expanduser(self.config.state_dir)
        if not os.path.isdir(state_dir):
            os.mkdir(state_dir)

        for endpoint in endpoints:
            self.profile("sending endpoint {0} ...".format(endpoint))
            asyncio.run(self.do_dispatch(endpoint))
            self.profile("finished endpoint {0}! (status counts: {1}) ".format(endpoint, str(self.status_counts)))

    def do_oauth(self):
        token_response = requests.post(
            self.config.edfi_api.oauth_url,
            data={"grant_type":"client_credentials"},
            auth=(
                self.config.edfi_api.client_id,
                self.config.edfi_api.client_secret
                ),
            verify=self.config.connection.verify_ssl)
        self.token = token_response.json()["access_token"]
        # self.profile("(using OAuth token {0})".format(token))

        # these headers are sent with every subsequent API POST request:
        self.config.edfi_api.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "authorization": "Bearer " + self.token
        }

    async def do_dispatch(self, endpoint):
        async with RetryClient(
            timeout=aiohttp.ClientTimeout(total=self.config.connection.timeout),
            retry_options=ExponentialRetry(
                attempts=self.config.connection.num_retries,
                factor=self.config.connection.backoff_factor,
                statuses=self.config.connection.retry_statuses
                ),
            connector=aiohttp.connector.TCPConnector(limit=self.config.connection.pool_size),
            headers=self.config.edfi_api.headers
            ) as client:
        
            # We try to be smart and avoid re-POSTing JSON we've already (successfully) sent.
            # This is done by storing a few things in a file we call a hashlog:
            # - the hash of the JSON (so we can recognize it in the future)
            # - the timestamp of the last send of this JSON
            # - the returned status code for the last send
            # Using these logs, we can do things like retry JSON that previously failed, resend JSON older than a certain age, etc.
            hashlog_file = os.path.join(os.path.expanduser(self.config.state_dir), f"{endpoint}.dat")
            self.hashlog = self.load_hashlog(hashlog_file)
            
            file_name = self.config.data_dir + endpoint + ".jsonl"
            self.num_finished = 0
            self.status_counts = {}
            tasks = []
            with open(file_name) as file:
                self.num_skipped = 0
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
                        if ( self.force
                            or (self.older_than!='' and self.hashlog[hash][0]<self.older_than)
                            or (self.newer_than!="" and self.hashlog[hash][0]>self.newer_than)
                            or (len(self.resend_status_codes)>0 and self.hashlog[hash][1] in self.resend_status_codes)
                        ):
                            tasks.append(asyncio.ensure_future(self.do_post(endpoint, data, client, counter, hash)))
                        else:
                            self.num_skipped += 1
                            continue
                    else: # never before seen! send
                        tasks.append(asyncio.ensure_future(self.do_post(endpoint, data, client, counter, hash)))
                if self.num_skipped>0:
                    self.profile("skipped {0} of {1} payloads because they were previously processed and did not match any resend criteria".format(self.num_skipped, counter))
            tasks.append(asyncio.ensure_future(self.update_every_second_until_done(counter)))
            await self.gather_with_concurrency(self.config.connection.pool_size, *tasks) # execute them concurrently
            self.save_hashlog(hashlog_file, self.hashlog)
    
        if self.errors > 10:
            raise Exception("more than 10 errors, terminating. Please review the errors, fix data errors or network conditions, and dispatch again.")

    async def update_every_second_until_done(self, counter):
        while self.num_finished + self.num_skipped < counter:
            if len(self.status_counts.keys())>0:
                self.profile("                       (status counts: {0}) ".format(str(self.status_counts)))
            await asyncio.sleep(1)

    async def do_post(self, endpoint, data, client, line, hash):
        file_name = self.config.data_dir + endpoint + ".jsonl"
        try:
            async with client.post(self.config.edfi_api.data_url + endpoint, data=data, ssl=self.config.connection.verify_ssl) as response:
                body = await response.text()
                status = str(response.status)
                self.num_finished += 1
                if status not in self.status_counts: self.status_counts[status] = 1
                else: self.status_counts[status] += 1
                if response.status not in [ 200, 201 ]:
                    self.profile("  ERROR with line {0} of {1}; ENDPOINT: {2}{3}; PAYLOAD: {4}; STATUS: {5}; RESPONSE: {6}".format(line, file_name, self.config.edfi_api.data_url, endpoint, data, status, body))
                    self.errors += 1
                if not self.force:
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
            self.errors += 1
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
        if not os.path.isdir(os.path.expanduser(self.config.state_dir)):
            os.mkdir(os.path.expanduser(self.config.state_dir))
        with open(hashlog_file, 'wb') as f:
            pickle.dump(hashlog, f)