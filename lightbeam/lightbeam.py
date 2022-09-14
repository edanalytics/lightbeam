import os
import re
import csv
import json
import time
import yaml
import copy
import pickle
import hashlib
import logging
import hashlib
import asyncio
import aiohttp
import dateutil.parser
import requests
from requests.adapters import HTTPAdapter, Retry
from aiohttp_retry import RetryClient, ExponentialRetry
from urllib3 import PoolManager
from glob import glob
from datetime import datetime
from yaml.loader import SafeLoader
from jsonschema import RefResolver
from jsonschema import Draft4Validator


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


class Lightbeam:

    version = "0.0.1"
    config_defaults = {
        "state_dir": os.path.join(os.path.expanduser("~"), ".lightbeam", ""),
        "data_dir": "./",
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
        "log_level": "INFO",
        "show_stacktrace": False
    }
    SWAGGER_CACHE_TTL = 2629800 # one month in seconds
    DESCRIPTORS_CACHE_TTL = 2629800 # one month in seconds
    TASK_QUEUE_SIZE = 1000
    NUM_VALIDATION_ERRORS_TO_DISPLAY = 10
    NUM_VALIDATION_REASONS_TO_DISPLAY = 10
    
    def __init__(self, config_file, logger=None, selector="*", params="", force=False, older_than="", newer_than="", resend_status_codes=""):
        self.config_file = config_file
        self.logger = logger
        self.t0 = time.time()
        self.memory_usage = 0
        self.status_counts = {}
        self.status_reasons = {}
        self.errors = 0
        self.params = params
        self.force = force
        self.older_than=older_than
        self.newer_than=newer_than
        self.resend_status_codes=resend_status_codes
        self.endpoints = []

        # load params and/or env vars for config YAML interpolation
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
                self.logger.critical(self.error_handler.ctx + "YAML could not be parsed: {0}".format(e))
            if not isinstance(user_config, dict):
                self.logger.critical("YAML does not seem to be a dictionary. See documentation for expected structure.")
        
        self.config = self.merge_config(user_config, self.config_defaults)
        self.config["state_dir"] = os.path.expanduser(self.config["state_dir"])
        self.config["data_dir"] = os.path.expanduser(self.config["data_dir"])

        # configure log level
        self.logger.setLevel(logging.getLevelName(self.config["log_level"].upper()))

        # check data_dir exists
        if not os.path.isdir(self.config["data_dir"]):
            self.logger.critical("`data_dir` {0} is not a directory".format(self.config["data_dir"]))
        
        # turn off annoying SSL warnings (is this necessary? is this dangerous?)
        logging.captureWarnings(True)

        # fetch/set up Ed-Fi API URLs
        try:
            self.logger.debug("fetching base_url...")
            api_base = requests.get(self.config["edfi_api"]["base_url"],
                                    verify=self.config["connection"]["verify_ssl"])
        except Exception as e:
            self.logger.critical("could not connect to {0} ({1})".format(self.config["edfi_api"]["base_url"], str(e)))
        try:
            api_base = api_base.json()
        except Exception as e:
            self.logger.critical("could not connect to {0} ({1})".format(self.config["edfi_api"]["base_url"], str(e)))

        self.config["edfi_api"]["oauth_url"] = api_base["urls"]["oauth"]
        self.config["edfi_api"]["dependencies_url"] = api_base["urls"]["dependencies"]
        self.config["edfi_api"]["data_url"] = self.get_data_url() + '/ed-fi/'
        self.config["edfi_api"]["open_api_metadata_url"] = api_base["urls"]["openApiMetadata"]

        # load all endpoints in dependency-order
        all_endpoints = self.get_sorted_endpoints()

        # filter down to only selected endpoints
        if selector!="*" and selector!="":
            if "," in selector:
                selected_endpoints = selector.split(",")
                selected_endpoints = [e for e in all_endpoints if e in selected_endpoints]
            else: selected_endpoints = [ selector ]
        else: selected_endpoints = all_endpoints
        if len(selected_endpoints)==0:
            self.logger.critical("selector filtering left no endpoints to process; check your selector for typos?")

        # filter down to selected endpoints that actually have .jsonl in config.data_dir
        selected_endpoints_with_data = self.get_endpoints_with_data(selected_endpoints)
        if len(selected_endpoints_with_data)==0:
            self.logger.critical("`data_dir` {0} has no *.jsonl files".format(self.config["data_dir"]) + (" for selected endpoints" if selector!="*" and selector!="" else ""))
        
        # set endpoints for this run
        self.endpoints = selected_endpoints_with_data
        
        # parse timestamps and/or status codes for state-based filtering
        if self.older_than!='': self.older_than = dateutil.parser.parse(self.older_than).timestamp()
        if self.newer_than!='': self.newer_than = dateutil.parser.parse(self.newer_than).timestamp()
        if self.resend_status_codes!='': self.resend_status_codes = [int(code) for code in self.resend_status_codes.split(",")]

        # create state_dir if it doesn't exist
        if not os.path.isdir(self.config["state_dir"]):
            self.logger.debug("creating state dir {0}".format(self.config["state_dir"]))
            os.mkdir(self.config["state_dir"])

        # load Descriptors and Resources swagger URLs
        try:
            self.logger.debug("fetching swagger docs...")
            response = requests.get(self.config["edfi_api"]["open_api_metadata_url"],
                                    verify=self.config["connection"]["verify_ssl"]).json()
        except Exception as e:
            self.logger.critical("Unable to load Swagger docs from API... terminating. Check API connectivity.")

        # load (or re-use cached) Descriptors and Resources swagger
        self.descriptors_swagger = None
        self.resources_swagger = None
        cache_dir = os.path.join(self.config["state_dir"], "cache")
        if not os.path.isdir(cache_dir):
            self.logger.debug("creating cache dir {0}".format(cache_dir))
            os.mkdir(cache_dir)
        for endpoint in response:
            endpoint_type = endpoint["name"].lower()
            if endpoint_type=="descriptors" or endpoint_type=="resources":
                swagger_url = endpoint["endpointUri"]
                hash = hashlib.md5(swagger_url.encode('utf-8')).hexdigest()
                file = os.path.join(cache_dir, f"swagger-{endpoint_type}-{hash}.json")
                if os.path.isfile(file) and time.time()-os.path.getmtime(file)<self.SWAGGER_CACHE_TTL:
                    self.logger.debug(f"re-using cached {endpoint_type} swagger doc (from {file})...")
                    with open(file) as f:
                        swagger = json.load(f)
                else:
                    self.logger.debug(f"fetching {endpoint_type} swagger doc...")
                    try:
                        swagger = requests.get(swagger_url,
                                                    verify=self.config["connection"]["verify_ssl"]
                                                    ).json()
                        self.logger.debug(f"(saving to {file})")
                    except Exception as e:
                        self.logger.critical(f"Unable to load {endpoint_type} Swagger from API... terminating. Check API connectivity.")

                    with open(file, 'w') as f:
                        json.dump(swagger, f)
                
            if endpoint_type=="descriptors":
                self.config["edfi_api"]["descriptors_swagger_url"] = swagger_url
                self.descriptors_swagger = swagger
                  
            if endpoint_type=="resources":
                self.config["edfi_api"]["resources_swagger_url"] = swagger_url
                self.resources_swagger = swagger

    # Merges two (potentially nested) dict structures, such as a default + custom config
    def merge_config(self, user, default):
        if isinstance(user, dict) and isinstance(default, dict):
            for k, v in default.items():
                if k not in user:
                    user[k] = v
                else:
                    user[k] = self.merge_config(user[k], v)
        return user
    
    # Constructs a base data URL (based on config params) to which we will post data
    def get_data_url(self):
        if self.config["edfi_api"]["version"]!=3:
            self.logger.critical("Sorry, lightbeam only supports connections to v3+ Ed-Fi APIs.")
        if self.config["edfi_api"]["base_url"][-1]!="/": url = self.config["edfi_api"]["base_url"] + "/" + "data/v3"
        else: url = self.config["edfi_api"]["base_url"] + "data/v3"

        if self.config["edfi_api"]["mode"] is None: pass
        elif self.config["edfi_api"]["mode"] in ('shared_instance', 'sandbox', 'district_specific',): pass
        elif self.config["edfi_api"]["mode"] in ('year_specific',):
            if "year" not in self.config["edfi_api"].keys():
                self.logger.critical("`year` required for 'year_specific' mode.")
            url += "/" + str(self.config["edfi_api"]["year"])
        elif self.config["edfi_api"]["mode"] in ('instance_year_specific',):
            if "year" not in self.config["edfi_api"].keys() or "instance_code" not in self.config["edfi_api"].keys():
                self.logger.critical("`instance_code` and `year` required for 'instance_year_specific' mode.")
            url += "/" + self.config["edfi_api"]["instance_code"] + "/" + str(self.config["edfi_api"]["year"])
        else:
            self.logger.critical(f"Invalid `api_mode` - must be one of: [shared_instance, sandbox, "
                "district_specific, year_specific, or instance_year_specific]. See {base_url} to find out your apiMode.")
        return url
    
    # Sorts endpoints by Ed-Fi dependency-order
    def get_sorted_endpoints(self):
        self.logger.debug("fetching resource dependencies...")
        try:
            response = requests.get(self.config["edfi_api"]["dependencies_url"],
                                    verify=self.config["connection"]["verify_ssl"]).json()
        except Exception as e:
            print(e)
            self.logger.critical("Unable to load dependencies from API... terminating. Check API connectivity.")
        
        ordered_endpoints = []
        for e in response:
            ordered_endpoints.append(e["resource"].replace("/ed-fi/", ""))
        return ordered_endpoints
    
    # Prunes the list of endpoints down to those for which .jsonl files exist in the config.data_dir
    def get_endpoints_with_data(self, endpoints):
        self.logger.debug("discovering data...")
        endpoints_with_data = []
        for endpoint in endpoints:
            possible_file = os.path.join(self.config["data_dir"], endpoint + ".jsonl")
            if os.path.isfile(possible_file) and endpoint not in endpoints_with_data:
                endpoints_with_data.append(endpoint)
            possible_dir = os.path.join(self.config["data_dir"] + endpoint)
            if os.path.isdir(possible_dir):
                for file in os.listdir(os.path.join(self.config["data_dir"] + endpoint)):
                    if file.endswith('.jsonl'):
                        if endpoint not in endpoints_with_data:
                            endpoints_with_data.append(endpoint)
                            break # one file is enough to know we need to process this endpoint
        return endpoints_with_data
    
    # For the specified endpoint, returns a list of all files in config.data_dir which end in .jsonl
    def get_data_files_for_endpoint(self, endpoint):
        file_list = []
        possible_file = os.path.join(self.config["data_dir"], endpoint + ".jsonl")
        if os.path.isfile(possible_file):
            file_list.append(possible_file)
        possible_dir = os.path.join(self.config["data_dir"] + endpoint)
        if os.path.isdir(possible_dir):
            for file in os.listdir(possible_dir):
                if file.endswith('.jsonl'):
                    file_list.append(os.path.join(self.config["data_dir"], endpoint, file))
        return file_list

    # Returns a generator which produces json lines for a given endpoint based on relevant files in config.data_dir
    def get_jsonl_for_endpoint(self, endpoint):
        file_list = self.get_data_files_for_endpoint(endpoint)
        for f in file_list:
            with open(f) as fd:
                for line in fd:
                    yield line.strip()
    
    # Converts (for example) `LocalEducationAgencies` to `LocalEducationAgency`; `students` to `student`; etc.
    def singularize_endpoint(self, endpoint):
        if endpoint[-3:]=="ies": return endpoint[0:-3] + "y"
        else: return endpoint[0:-1]

    # Returns a client object with exponential retry and other parameters per configs
    def get_retry_client(self):
        self.num_finished = 0
        self.num_skipped = 0
        self.status_counts = {}
        self.status_reasons = {}
        return RetryClient(
            timeout=aiohttp.ClientTimeout(total=self.config["connection"]["timeout"]),
            retry_options=ExponentialRetry(
                attempts=self.config["connection"]["num_retries"],
                factor=self.config["connection"]["backoff_factor"],
                statuses=self.config["connection"]["retry_statuses"]
                ),
            connector=aiohttp.connector.TCPConnector(limit=self.config["connection"]["pool_size"]),
            headers=self.config["edfi_api"]["headers"]
            )

    # Obtains an OAuth token from the API and sets the client headers accordingly
    def do_oauth(self):
        try:
            token_response = requests.post(
                self.config["edfi_api"]["oauth_url"],
                data={"grant_type":"client_credentials"},
                auth=(
                    self.config["edfi_api"]["client_id"],
                    self.config["edfi_api"]["client_secret"]
                    ),
                verify=self.config["connection"]["verify_ssl"])
            self.token = token_response.json()["access_token"]

            # these headers are sent with every subsequent API POST request:
            self.config["edfi_api"]["headers"] = {
                "accept": "application/json",
                "Content-Type": "application/json",
                "authorization": "Bearer " + self.token
            }
        except Exception as e:
            self.logger.error(f"OAuth token could not be obtained; check your API credentials?")

    # Loops over all descriptor endpoints (from Descriptors swagger) and fetches all valid
    # Descriptor values for each Descriptor. These values can then be used by
    # `validate_endpoint()` to check for invalid descriptor values before `send`ing.
    async def load_descriptors_values(self):
        self.logger.info("loading descriptor values...")
        cache_dir = os.path.join(self.config["state_dir"], "cache")
        if not os.path.isdir(cache_dir):
            self.logger.debug("creating cache dir {0}".format(cache_dir))
            os.mkdir(cache_dir)
        
        # get token with which to send requests
        self.do_oauth()

        # check for cached descriptor values
        hash = hashlib.md5(self.config["edfi_api"]["base_url"].encode('utf-8')).hexdigest()
        cache_file = os.path.join(cache_dir, f"descriptor-values-{hash}.csv")

        if os.path.isfile(cache_file) and time.time()-os.path.getmtime(cache_file)<self.DESCRIPTORS_CACHE_TTL:
            # cache file exists and we can use it!
            self.logger.debug(f"re-using cached descriptor values (from {cache_file})...")
            with open(cache_file, 'r') as csvfile:
                csvreader = csv.reader(csvfile)
                fields = next(csvreader)
                self.descriptor_values = []
                for row in csvreader:
                    self.descriptor_values.append(row)
        else:
            # load descriptor values from API
            self.logger.debug(f"fetching descriptor values...")
            tasks = []
            counter = 0
            async with self.get_retry_client() as client:
                for descriptor in self.descriptors_swagger["definitions"]:
                    if descriptor in ["link", "deletedResource"]: continue

                    counter += 1
                    descriptor = descriptor.replace("edFi_", "").replace("tpdm_", "")
                    #descriptor = descriptor[0].upper() + descriptor[1:]
                    tasks.append(asyncio.ensure_future(self.get_descriptor_values(client, descriptor, counter)))
                
                await self.do_tasks(tasks, counter)

            # save
            self.logger.debug(f"saving descriptor values to {cache_file}...")
            header = ['desriptor', 'namespace', 'codeValue', 'shortDescription', 'description']
            with open(cache_file, 'w', encoding='UTF8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(self.descriptor_values)

    # Fetches valid descriptor values for a specific descriptor endpoint
    async def get_descriptor_values(self, client, descriptor, counter):
        self.descriptor_values = []
        try:
            async with client.get(self.config["edfi_api"]["data_url"] + descriptor + "s",
                                    ssl=self.config["connection"]["verify_ssl"],
                                    headers=self.config["edfi_api"]["headers"]
                                    ) as response:
                body = await response.text()
                self.num_finished += 1
                if response.content_type == "application/json":
                    # status = str(response.status)
                    values = json.loads(body)
                    for v in values:
                        self.descriptor_values.append([descriptor, v["namespace"], v["codeValue"], v["shortDescription"], v["description"]])

        except Exception as e:
            self.num_errors += 1
            self.logger.critical("Unable to load descriptor values from API... terminating. Check API connectivity.")

    # Tells you if a specified descriptor value is valid or not
    def is_valid_descriptor_value(self, namespace, codeValue):
        for row in self.descriptor_values:
            if row[1]==namespace and row[2]==codeValue:
                return True
        return False

    # Validates (selected) endpoints
    def validate(self):
        asyncio.run(self.load_descriptors_values())
        for endpoint in self.endpoints:
            if "Descriptor" in endpoint:
                self.validate_endpoint(self.descriptors_swagger, endpoint)
            else:
                self.validate_endpoint(self.resources_swagger, endpoint)

    # Validates a single endpoint based on the Swagger docs
    def validate_endpoint(self, swagger, endpoint):
        definition = "edFi_" + self.singularize_endpoint(endpoint)
        resource_schema = swagger["definitions"][definition]

        resolver = RefResolver("test", swagger, swagger)
        validator = Draft4Validator(resource_schema, resolver=resolver)
        params_structure = self.get_params_for_endpoint(endpoint)
        distinct_params = []

        endpoint_data_files = self.get_data_files_for_endpoint(endpoint)
        for file in endpoint_data_files:
            self.logger.info(f"validating {file} against {definition} schema...")
            with open(file) as f:
                counter = 0
                num_errors = 0
                for line in f:
                    counter += 1
                    # check payload is valid JSON
                    try:
                        instance = json.loads(line)
                    except Exception as e:
                        if num_errors < self.NUM_VALIDATION_ERRORS_TO_DISPLAY:
                            self.logger.warning(f"... VALIDATION ERROR (line {counter}): invalid JSON" + str(e).replace(" line 1",""))
                        num_errors += 1
                        continue

                    # check payload obeys Swagger schema
                    try:
                        validator.validate(instance)
                    except Exception as e:
                        if num_errors < self.NUM_VALIDATION_ERRORS_TO_DISPLAY:
                            e_path = [str(x) for x in list(e.path)]
                            context = ""
                            if len(e_path)>0: context = " in " + " -> ".join(e_path)
                            self.logger.warning(f"... VALIDATION ERROR (line {counter}): " + str(e.message) + context)
                        num_errors += 1
                        continue

                    # check descriptor values are valid
                    error_message = self.invalid_descriptor_values(instance)
                    if error_message != "":
                        if num_errors < self.NUM_VALIDATION_ERRORS_TO_DISPLAY:
                            self.logger.warning(f"... VALIDATION ERROR (line {counter}): " + error_message)
                        num_errors += 1
                        continue

                    # check natural keys are unique
                    params = json.dumps(self.interpolate_params(params_structure, line))
                    hash = hashlib.md5(params.encode()).digest()
                    if hash in distinct_params:
                        if num_errors < self.NUM_VALIDATION_ERRORS_TO_DISPLAY:
                            self.logger.warning(f"... VALIDATION ERROR (line {counter}): duplicate value(s) for natural key(s): {params}")
                        num_errors += 1
                        continue
                    else: distinct_params.append(hash)
                
                if num_errors==0: self.logger.info(f"... all lines validate ok!")
                else:
                    num = num_errors - self.NUM_VALIDATION_ERRORS_TO_DISPLAY
                    if num_errors > self.NUM_VALIDATION_ERRORS_TO_DISPLAY:
                        self.logger.warning(f"... and {num} others!")
                    self.logger.warning(f"... VALIDATION ERRORS on {num_errors} of {counter} lines in {file}; see details above.")
                    exit(1)
    
    # Validates descriptor values for a single payload (returns an error message or empty string)
    def invalid_descriptor_values(self, payload, path=""):
        for k in payload.keys():
            if isinstance(payload[k], dict):
                value = self.invalid_descriptor_values(payload[k], path+("." if path!="" else "")+k)
                if value!="": return value
            elif isinstance(payload[k], list):
                for i in range(0, len(payload[k])):
                    value = self.invalid_descriptor_values(payload[k][i], path+("." if path!="" else "")+k+"["+str(i)+"]")
                    if value!="": return value
            elif isinstance(payload[k], str) and "Descriptor" in k:
                namespace = payload[k].split("#")[0]
                codeValue = payload[k].split("#")[1]
                if not self.is_valid_descriptor_value(namespace, codeValue):
                    return payload[k] + f" is not a valid descriptor value for {k}" + (" (at " + path + ")" if path!="" else "")
        return ""

    # Sends all (selected) endpoints
    def send(self):
        # get token with which to send requests
        self.do_oauth()

        # send each endpoint
        for endpoint in self.endpoints:
            self.logger.info("sending endpoint {0} ...".format(endpoint))
            asyncio.run(self.do_send(endpoint))
            self.logger.info("finished sending data for endpoint {0}!".format(endpoint))
            self.logger.info("  (status counts: {0}) ".format(str(self.status_counts)))
            if len(self.status_reasons.keys())>0:
                counter = 0
                for k,v in self.status_reasons.items():
                    counter += 1
                    if counter>self.NUM_VALIDATION_REASONS_TO_DISPLAY: break
                    self.logger.info("  (reason: [{0}]; instances: {1})".format(k, str(v)))
                if len(self.status_reasons.keys())>self.NUM_VALIDATION_REASONS_TO_DISPLAY:
                    self.logger.info("  (... and {0} others)".format(str(len(self.status_reasons.keys())-self.NUM_VALIDATION_REASONS_TO_DISPLAY)))

    # Sends a single endpoint
    async def do_send(self, endpoint):
        # here we set up a smart retry client with exponential backoff and a connection pool
        async with self.get_retry_client() as client:
            # We try to  avoid re-POSTing JSON we've already (successfully) sent.
            # This is done by storing a few things in a file we call a hashlog:
            # - the hash of the JSON (so we can recognize it in the future)
            # - the timestamp of the last send of this JSON
            # - the returned status code for the last send
            # Using these hashlogs, we can do things like retry JSON that previously
            # failed, resend JSON older than a certain age, etc.
            hashlog_file = os.path.join(self.config["state_dir"], f"{endpoint}.dat")
            self.hashlog = self.load_hashlog(hashlog_file)
            
            # process each file
            data_files = self.get_data_files_for_endpoint(endpoint)
            tasks = []
            total_counter = 0
            todo_counter = 0
            for file_name in data_files:
                with open(file_name) as file:
                    # process each line
                    for line in file:
                        total_counter += 1
                        data = line.strip()
                        # compute hash of current row
                        hash = hashlib.md5(data.encode()).digest()
                        # check if we've posted this data before
                        if hash in self.hashlog.keys():
                            # check if the last post meets criteria for a resend
                            if ( self.force
                                or (self.older_than!='' and self.hashlog[hash][0]<self.older_than)
                                or (self.newer_than!="" and self.hashlog[hash][0]>self.newer_than)
                                or (len(self.resend_status_codes)>0 and self.hashlog[hash][1] in self.resend_status_codes)
                            ):
                                # yes, we need to (re)post it; append to task queue
                                todo_counter += 1
                                tasks.append(asyncio.ensure_future(
                                    self.do_post(endpoint, file_name, data, client, total_counter, hash)))
                            else:
                                # no, do not (re)post
                                self.num_skipped += 1
                                continue
                        else:
                            # new, never-before-seen payload! append it to task queue
                            todo_counter += 1
                            tasks.append(asyncio.ensure_future(
                                self.do_post(endpoint, file_name, data, client, total_counter, hash)))
                    
                        if total_counter%self.TASK_QUEUE_SIZE==0:
                            await self.do_tasks(tasks, total_counter)
                            tasks = []
                        
                    if self.num_skipped>0:
                        self.logger.info("skipped {0} of {1} payloads because they were previously processed and did not match any resend criteria".format(self.num_skipped, total_counter))
                        
                await self.do_tasks(tasks, total_counter)

            # any task may have updated the hashlog, so we need to re-save it out to disk
            self.save_hashlog(hashlog_file, self.hashlog)
    
    # Posts a single data payload to a single endpoint using the client
    async def do_post(self, endpoint, file_name, data, client, line, hash):
        try:
            async with client.post(self.config["edfi_api"]["data_url"] + endpoint, data=data,
                                    ssl=self.config["connection"]["verify_ssl"]) as response:
                body = await response.text()
                status = str(response.status)
                self.num_finished += 1

                # update status_counts (for every-second status update)
                if status not in self.status_counts: self.status_counts[status] = 1
                else: self.status_counts[status] += 1
                
                # warn about errors
                if response.status not in [ 200, 201 ]:
                    message = str(response.status) + ": " + self.linearize(body)
                    if message not in self.status_reasons: self.status_reasons[message] = 1
                    else: self.status_reasons[message] += 1
                    self.errors += 1
                
                # update hashlog
                if not self.force:
                    self.hashlog[hash] = (round(time.time()), response.status)
        
        except Exception as e:
            self.errors += 1
            self.logger.error(str(e), "  (at line {0} of {1} )".format(line, file_name))

    # Logs a status update every second
    async def update_every_second_until_done(self, counter):
        while self.num_finished + self.num_skipped < counter:
            if hasattr(self, "status_counts") and len(self.status_counts.keys())>0:
                self.logger.info("     (status counts: {0}) ".format(str(self.status_counts)))
            await asyncio.sleep(1)
    
    # Waits for an entire task queue to finish processing
    async def gather_with_concurrency(self, n, *tasks):
        semaphore = asyncio.Semaphore(n)
        async def sem_task(task):
            async with semaphore:
                return await task
        return await asyncio.gather(*(sem_task(task) for task in tasks))
    
    def linearize(self, string):
        exp = re.compile(r"\s+")
        string = string.replace("\r\n", "")
        string = string.replace("\n", "")
        string = string.replace("\r", "")
        string = string.strip()
        string = exp.sub(" ", string).strip()
        return string

    # Deletes data matching payloads in config.data_dir for selected endpoints
    def delete(self):
        # prompt to confirm this destructive operation
        if not self.config["force_delete"]:
            if input('Type "yes" to confirm you want to delete payloads for the selected endpoints? ')!="yes":
                exit('You did not type "yes" - exiting.')
            
        # get token with which to send requests
        self.do_oauth()

        # process endpoints in reverse-dependency order, so we don't get dependency errors
        endpoints = copy.deepcopy(self.endpoints)
        endpoints.reverse()
        for endpoint in endpoints:
            if endpoint=='students':
                self.logger.warn("data for {0} endpoint cannot be deleted (this is an Ed-Fi limitation); skipping".format(endpoint))
                continue
            self.logger.info("deleting data from endpoint {0} ...".format(endpoint))
            asyncio.run(self.do_deletes(endpoint))
            self.logger.info("finished deleting data from endpoint {0}!".format(endpoint))
            self.logger.info("  (status counts: {0})".format(str(self.status_counts)))
            if len(self.status_reasons.keys())>0:
                counter = 0
                for k,v in self.status_reasons.items():
                    counter += 1
                    if counter>self.NUM_VALIDATION_REASONS_TO_DISPLAY: break
                    self.logger.info("  (reason: [{0}]; instances: {1})".format(k, str(v)))
                if len(self.status_reasons.keys())>self.NUM_VALIDATION_REASONS_TO_DISPLAY:
                    self.logger.info("  (... and {0} others)".format(str(len(self.status_reasons.keys())-self.NUM_VALIDATION_REASONS_TO_DISPLAY)))

    # Deletes data matching payloads in config.data_dir for single endpoint
    async def do_deletes(self, endpoint):
        # here we set up a smart retry client with exponential backoff and a connection pool
        async with self.get_retry_client() as client:
            # load the hashlog, since we delete previously-seen payloads from it after deleting them
            hashlog_file = os.path.join(self.config["state_dir"], f"{endpoint}.dat")
            self.hashlog = self.load_hashlog(hashlog_file)
        
            data_files = self.get_data_files_for_endpoint(endpoint)
            tasks = []

            # determine the fields that uniquely define a record for this endpoint
            params_structure = self.get_params_for_endpoint(endpoint)

            # process each file
            counter = 0
            for file_name in data_files:
                with open(file_name) as file:
                    # process each payload
                    for line in file:
                        counter += 1
                        data = line.strip()
                        # fill out the required fields from the data payload
                        # (so we can search for matching records in the API)
                        params = self.interpolate_params(params_structure, data)

                        hash = hashlib.md5(data.encode()).digest()
                        if hash in self.hashlog.keys():
                            # remove the payload from the hashlog
                            del self.hashlog[hash]
                        
                        # append a delete task to the queue
                        tasks.append(asyncio.ensure_future(
                            self.do_delete(endpoint, file_name, params, client, counter)))

                        if counter%self.TASK_QUEUE_SIZE==0:
                            await self.do_tasks(tasks, counter)
                            tasks = []
                        
                await self.do_tasks(tasks, counter)

            # any task may have updated the hashlog, so we need to re-save it out to disk
            self.save_hashlog(hashlog_file, self.hashlog)

    async def do_tasks(self, tasks, counter):
        # we also append a task to the queue that logs a status update every second
        tasks.append(asyncio.ensure_future(self.update_every_second_until_done(counter)))

        # now process the task queue (concurrently)
        await self.gather_with_concurrency(self.config["connection"]["pool_size"], *tasks)

    # Deletes a single payload for a single endpoint
    async def do_delete(self, endpoint, file_name, params, client, line):
        try:
            # we have to get the `id` for a particular resource by first searching for its natural keys
            async with client.get(self.config["edfi_api"]["data_url"] + endpoint, params=params,
                                    ssl=self.config["connection"]["verify_ssl"]) as response:
                body = await response.text()
                status = str(response.status)
                skip_reason = ""
                if status in ['200', '201']:
                    j = json.loads(body)
                    if type(j)==list and len(j)==1:
                        the_id = j[0]['id']
                        # now we can delete by `id`
                        async with client.delete(self.config["edfi_api"]["data_url"] + endpoint + '/' + the_id,
                                                    ssl=self.config["connection"]["verify_ssl"]) as response:
                            body = await response.text()
                            status = str(response.status)
                            self.num_finished += 1
                            if status not in self.status_counts: self.status_counts[status] = 1
                            else: self.status_counts[status] += 1
                            if response.status not in [ 204 ]:
                                message = str(response.status) + ": " + self.linearize(body)
                                if message not in self.status_reasons: self.status_reasons[message] = 1
                                else: self.status_reasons[message] += 1
                                self.errors += 1
                    elif type(j)==list and len(j)==0:
                        self.num_skipped += 1
                        skip_reason = "payload not found in API"
                    elif type(j)==list and len(j)>1:
                        self.num_skipped += 1
                        skip_reason = "multiple matching payloads found in API"
                    else:
                        self.num_skipped += 1
                        skip_reason = "searching API for payload returned a response that is not a list"
                else:
                    self.num_skipped += 1
                    skip_reason = f"searching API for payload returned a {status} response"
                if skip_reason != "":
                    if skip_reason not in self.status_reasons: self.status_reasons[skip_reason] = 1
                    else: self.status_reasons[skip_reason] += 1
                    
        except Exception as e:
            self.errors += 1
            self.logger.exception(e, exc_info=self.config["show_stacktrace"])
            self.logger.error("  (at line {0} of {1}; ID: {2} )".format(line, file_name, id))

    # This function (and the helper below) walks through the swagger for a resource, following references,
    #  grabs all the required (nested) fields, and constructs a structure like this (for assessmentItem):
    # {
    #    "identificationCode": "identificationCode",
    #    "assessmentIdentifier": "assessmentReference.assessmentIdentifier",
    #    "namespace": "assessmentReference.namespace"
    # }
    # (The first element is a required attribute of the assessmentItem; the other two are required elements
    # of the required nested assessmentReference.)
    def get_params_for_endpoint(self, endpoint):
        if "Descriptor" in endpoint: swagger = self.descriptors_swagger
        else: swagger = self.resources_swagger
        definition = "edFi_" + self.singularize_endpoint(endpoint)
        return self.get_required_params_from_swagger(swagger, definition)

    def get_required_params_from_swagger(self, swagger, definition, prefix=""):
        params = {}
        for requiredProperty in swagger["definitions"][definition]["required"]:
            if "$ref" in swagger["definitions"][definition]["properties"][requiredProperty].keys():
                sub_definition = swagger["definitions"][definition]["properties"][requiredProperty]["$ref"].replace("#/definitions/", "")
                sub_params = self.get_required_params_from_swagger(swagger, sub_definition, prefix=requiredProperty+".")
                for k,v in sub_params.items():
                    params[k] = v
            elif swagger["definitions"][definition]["properties"][requiredProperty]["type"]!="array":
                params[requiredProperty] = prefix + requiredProperty
        return params

    # Takes a params structure (from above two functions) and interpolates values from a payload
    def interpolate_params(self, params_structure, payload):
        params = {}
        for k,v in params_structure.items():
            value = json.loads(payload)
            for key in v.split('.'):
                value = value[key]
            params[k] = value
        return params

    # Loads (unpickles) a hashlog file
    def load_hashlog(self, hashlog_file):
        hashlog = {}
        if os.path.isfile(hashlog_file):
            with open(hashlog_file, 'rb') as f:
                hashlog = pickle.load(f)
        return hashlog

    # Saves (pickles) a hashlog file
    def save_hashlog(self, hashlog_file, hashlog):
        if not os.path.isdir(self.config["state_dir"]):
            os.mkdir(self.config["state_dir"])
        with open(hashlog_file, 'wb') as f:
            pickle.dump(hashlog, f)
