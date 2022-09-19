import os
import csv
import json
import time
import asyncio
import aiohttp
import requests
from requests.adapters import HTTPAdapter, Retry
from aiohttp_retry import RetryClient, ExponentialRetry

from lightbeam import util
from lightbeam import hashlog


class EdFiAPI:

    SWAGGER_CACHE_TTL = 2629800 # one month in seconds
    DESCRIPTORS_CACHE_TTL = 2629800 # one month in seconds
    
    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        self.logger = self.lightbeam.logger
        self.config = None
    
    # prepares this API object by fetching some of its metadata and
    # setting up data and objects for further use
    def prepare(self, selector="*"):
        self.config = self.lightbeam.config["edfi_api"]

        # fetch/set up Ed-Fi API URLs
        try:
            self.logger.debug("fetching base_url...")
            api_base = requests.get(self.config["base_url"],
                                    verify=self.lightbeam.config["connection"]["verify_ssl"])
        except Exception as e:
            self.logger.critical("could not connect to {0} ({1})".format(self.config["base_url"], str(e)))
        
        try:
            api_base = api_base.json()
        except Exception as e:
            self.logger.critical("could not parse response from {0} ({1})".format(self.config["base_url"], str(e)))

        self.config["oauth_url"] = api_base["urls"]["oauth"]
        self.config["dependencies_url"] = api_base["urls"]["dependencies"]
        self.config["data_url"] = self.get_data_url() + '/' + self.lightbeam.config["namespace"] + '/'
        self.config["open_api_metadata_url"] = api_base["urls"]["openApiMetadata"]

        # load all endpoints in dependency-order
        all_endpoints = self.get_sorted_endpoints()

        # filter down to only selected endpoints
        if selector!="*" and selector!="":
            if "," in selector:
                selected_endpoints = selector.split(",")
                selected_endpoints = [e for e in all_endpoints if e in selected_endpoints]
            else: selected_endpoints = [ selector ]
        else: selected_endpoints = all_endpoints
        unknown_endpoints = list(set(selected_endpoints).difference(set(all_endpoints)))
        # make sure all selectors resolve to an endpoint
        if len(unknown_endpoints)>0:
            self.logger.critical("no match for selector(s) [{0}] to any endpoint in your API; check for typos?".format(", ".join(unknown_endpoints)))
        # make sure we have some endpoints to process
        if len(selected_endpoints)==0:
            self.logger.critical("selector filtering left no endpoints to process; check your selector for typos?")

        self.lightbeam.endpoints = selected_endpoints

    # Returns a client object with exponential retry and other parameters per configs
    def get_retry_client(self):
        return RetryClient(
            timeout=aiohttp.ClientTimeout(total=self.lightbeam.config['connection']["timeout"]),
            retry_options=ExponentialRetry(
                attempts=self.lightbeam.config['connection']["num_retries"],
                factor=self.lightbeam.config['connection']["backoff_factor"],
                statuses=self.lightbeam.config['connection']["retry_statuses"]
                ),
            connector=aiohttp.connector.TCPConnector(limit=self.lightbeam.config['connection']["pool_size"]),
            headers={
                    "accept": "application/json",
                    "Content-Type": "application/json",
                    "authorization": "Bearer " + self.token
                }
            )
    
    # Obtains an OAuth token from the API and sets the client headers accordingly
    def do_oauth(self):
        try:
            token_response = requests.post(
                self.config["oauth_url"],
                data={"grant_type":"client_credentials"},
                auth=(
                    self.config["client_id"],
                    self.config["client_secret"]
                    ),
                verify=self.lightbeam.config["connection"]["verify_ssl"])
            self.token = token_response.json()["access_token"]

            # these headers are sent with every subsequent API POST request:
            self.token = self.token
        except Exception as e:
            self.logger.error(f"OAuth token could not be obtained; check your API credentials?")

    def update_oauth(self, client):
        self.lightbeam.is_locked = True
        self.do_oauth()
        client.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "authorization": "Bearer " + self.token
        }
        self.lightbeam.is_locked = False

    # Constructs a base data URL (based on config params) to which we will post data
    def get_data_url(self):
        if self.config["version"]!=3:
            self.logger.critical("Sorry, lightbeam only supports connections to v3+ Ed-Fi APIs.")
        if self.config["base_url"][-1]!="/": url = self.config["base_url"] + "/" + "data/v3"
        else: url = self.config["base_url"] + "data/v3"

        if self.config["mode"] is None: pass
        elif self.config["mode"] in ('shared_instance', 'sandbox', 'district_specific',): pass
        elif self.config["mode"] in ('year_specific',):
            if "year" not in self.config.keys():
                self.logger.critical("`year` required for 'year_specific' mode.")
            url += "/" + str(self.config["year"])
        elif self.config["mode"] in ('instance_year_specific',):
            if "year" not in self.config.keys() or "instance_code" not in self.config.keys():
                self.logger.critical("`instance_code` and `year` required for 'instance_year_specific' mode.")
            url += "/" + self.config["instance_code"] + "/" + str(self.config["year"])
        else:
            self.logger.critical(f"Invalid `api_mode` - must be one of: [shared_instance, sandbox, "
                "district_specific, year_specific, or instance_year_specific]. See {base_url} to find out your apiMode.")
        return url
    
    # Sorts endpoints by Ed-Fi dependency-order
    def get_sorted_endpoints(self):
        self.logger.debug("fetching resource dependencies...")
        try:
            response = requests.get(self.config["dependencies_url"],
                                    verify=self.lightbeam.config["connection"]["verify_ssl"]).json()
        except Exception as e:
            self.logger.critical("Unable to load dependencies from API... terminating. Check API connectivity. ({0})".format(str(e)))
        
        ordered_endpoints = []
        for e in response:
            ordered_endpoints.append(e["resource"].replace('/' + self.lightbeam.config["namespace"] + '/', ""))
        return ordered_endpoints
    
    # Loads the Swagger JSON from the Ed-Fi API
    def load_swagger_docs(self):
        # grab Descriptors and Resources swagger URLs
        try:
            self.logger.debug("fetching swagger docs...")
            response = requests.get(self.config["open_api_metadata_url"],
                                    verify=self.lightbeam.config["connection"]["verify_ssl"]).json()
        except Exception as e:
            self.logger.critical("Unable to load Swagger docs from API... terminating. Check API connectivity.")

        # load (or re-use cached) Descriptors and Resources swagger
        self.descriptors_swagger = None
        self.resources_swagger = None
        
        cache_dir = os.path.join(self.lightbeam.config["state_dir"], "cache")
        if not os.path.isdir(cache_dir):
            self.logger.debug("creating cache dir {0}".format(cache_dir))
            os.mkdir(cache_dir)

        for endpoint in response:
            endpoint_type = endpoint["name"].lower()
            if endpoint_type=="descriptors" or endpoint_type=="resources":
                swagger_url = endpoint["endpointUri"]
                hash = hashlog.get_hash_string(swagger_url)
                file = os.path.join(cache_dir, f"swagger-{endpoint_type}-{hash}.json")
                if os.path.isfile(file) and time.time()-os.path.getmtime(file)<self.SWAGGER_CACHE_TTL:
                    self.logger.debug(f"re-using cached {endpoint_type} swagger doc (from {file})...")
                    with open(file) as f:
                        swagger = json.load(f)
                else:
                    self.logger.debug(f"fetching {endpoint_type} swagger doc...")
                    try:
                        swagger = requests.get(swagger_url,
                                                    verify=self.lightbeam.config["connection"]["verify_ssl"]
                                                    ).json()
                        self.logger.debug(f"(saving to {file})")
                    except Exception as e:
                        self.logger.critical(f"Unable to load {endpoint_type} Swagger from API... terminating. Check API connectivity.")

                    with open(file, 'w') as f:
                        json.dump(swagger, f)
                
            if endpoint_type=="descriptors": self.descriptors_swagger = swagger
            if endpoint_type=="resources": self.resources_swagger = swagger

    # Loops over all descriptor endpoints (from Descriptors swagger) and fetches all valid
    # Descriptor values for each Descriptor. These values can then be used by
    # `validate_endpoint()` to check for invalid descriptor values before `send`ing.
    async def load_descriptors_values(self):
        self.logger.debug("loading descriptor values...")
        cache_dir = os.path.join(self.lightbeam.config["state_dir"], "cache")
        if not os.path.isdir(cache_dir):
            self.logger.debug("creating cache dir {0}".format(cache_dir))
            os.mkdir(cache_dir)
        
        # get token with which to send requests
        self.do_oauth()

        # check for cached descriptor values
        hash = hashlog.get_hash_string(self.config["base_url"])
        cache_file = os.path.join(cache_dir, f"descriptor-values-{hash}.csv")

        self.lightbeam.reset_counters()
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
                    descriptor = descriptor.replace(util.camel_case(self.lightbeam.config["namespace"]) + "_", "")
                    #descriptor = descriptor[0].upper() + descriptor[1:]
                    tasks.append(asyncio.ensure_future(self.get_descriptor_values(client, descriptor)))
                
                await self.lightbeam.do_tasks(tasks, counter)

            # save
            self.logger.debug(f"saving descriptor values to {cache_file}...")
            header = ['desriptor', 'namespace', 'codeValue', 'shortDescription', 'description']
            with open(cache_file, 'w', encoding='UTF8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(self.descriptor_values)

    # Fetches valid descriptor values for a specific descriptor endpoint
    async def get_descriptor_values(self, client, descriptor):
        self.descriptor_values = []
        try:
            # wait if another process has locked lightbeam while we refresh the oauth token:
            while self.lightbeam.is_locked:
                await asyncio.sleep(1)
            
            async with client.get(self.config["data_url"] + descriptor + "s",
                                    ssl=self.lightbeam.config["connection"]["verify_ssl"]) as response:
                body = await response.text()
                status = str(response.status)
                if status=='400': self.lightbeam.api.update_oauth(client)
                self.lightbeam.num_finished += 1
                if response.content_type == "application/json":
                    values = json.loads(body)
                    for v in values:
                        self.descriptor_values.append([descriptor, v["namespace"], v["codeValue"], v["shortDescription"], v["description"]])

        except Exception as e:
            self.logger.critical(f"Unable to load descriptor values for {descriptor} from API... terminating. Check API connectivity.")


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
        definition = util.camel_case(self.lightbeam.config["namespace"]) + "_" + util.singularize_endpoint(endpoint)
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


    