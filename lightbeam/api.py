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
    def prepare(self):
        self.config = self.lightbeam.config["edfi_api"]

        # fetch/set up Ed-Fi API URLs
        try:
            self.logger.debug("fetching base_url...")
            api_base = self.get_with_protocol_fallback(self.config["base_url"], 'base_url')
        except Exception as e:
            self.logger.critical("could not connect to {0} ({1})".format(self.config["base_url"], str(e)))
        
        # Data URL doesn't rely on metadata connection
        self.config["data_url"] = self.get_data_url()

        # If ALL urls are set in config (probably from source/destination file),
        # then they don't need to be pulled from api metadata, so this section can be skipped.
        # This is most common if the api metadata json files are not in the "default" location.
        # Otherwise, pull urls from api metadata.
        if (
            self.config.get("oauth_url", "")==""
            or self.config.get("dependencies_url", "")==""
            or self.config.get("open_api_metadata_url", "")==""
        ):
            try:
                api_base = api_base.json()
                if self.config.get("oauth_url", "")=="":
                    self.config["oauth_url"] = api_base["urls"]["oauth"]
                if self.config.get("dependencies_url", "")=="":
                    self.config["dependencies_url"] = api_base["urls"]["dependencies"]
                if self.config.get("open_api_metadata_url", "")=="":
                    self.config["open_api_metadata_url"] = api_base["urls"]["openApiMetadata"]
            except Exception as e:
                self.logger.critical("could not parse response from {0} ({1})".format(self.config["base_url"], str(e)))

        # load all endpoints in dependency-order
        self.lightbeam.all_endpoints = self.get_sorted_endpoints()

        # filter down to only selected endpoints
        self.lightbeam.endpoints = self.apply_filters(self.lightbeam.all_endpoints)


    def apply_filters(self, endpoints=[]):
        # apply filters
        my_endpoints = util.apply_selections(endpoints, self.lightbeam.selector, self.lightbeam.exclude)
        
        # make sure we have some endpoints to process
        if not my_endpoints:
            self.logger.critical("selector filtering left no endpoints to process; check your selector for typos?")

        # make sure all selectors resolve to an endpoint
        unknown_endpoints = set(my_endpoints).difference(endpoints)
        if unknown_endpoints:
            self.logger.critical("no match for selector(s) [{0}] to any endpoint in your API; check for typos?".format(", ".join(unknown_endpoints)))

        # all the list(set()) stuff above can mess up the ordering of the endpoints (which must be in dependency-order)... this puts them back in dependency-order
        final_endpoints = [x for x in endpoints if x in my_endpoints]
        
        return final_endpoints


    # Returns a client object with exponential retry and other parameters per configs
    def get_retry_client(self):
        return RetryClient(
            timeout=aiohttp.ClientTimeout(sock_connect=self.lightbeam.config['connection']["timeout"]),
            retry_options=ExponentialRetry(
                attempts=self.lightbeam.config['connection']["num_retries"],
                factor=self.lightbeam.config['connection']["backoff_factor"],
                statuses=self.lightbeam.config['connection']["retry_statuses"].append(401)
                ),
            connector=aiohttp.connector.TCPConnector(limit=self.lightbeam.config['connection']["pool_size"])
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
            self.headers = {
                    "accept": "application/json",
                    "Content-Type": "application/json",
                    "authorization": "Bearer " + self.token
                }
        except Exception as e:
            self.logger.error(f"OAuth token could not be obtained; check your API credentials?")

    def update_oauth(self):
        self.logger.debug("fetching new OAuth token due to a 401 response...")
        self.lightbeam.token_version += 1
        self.do_oauth()
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "authorization": "Bearer " + self.token
        }


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
            response = self.get_with_protocol_fallback(self.config["dependencies_url"], 'dependencies_url')
            if response.status_code not in [ 200, 201 ]:
                raise Exception("Dependencies URL returned status {0} ({1})".format(response.status_code, (response.content[:75] + "...") if len(response.content)>75 else response.content))
            data = response.json()
        except Exception as e:
            self.logger.critical("Unable to load dependencies from API... terminating. Check API connectivity. ({0})".format(str(e)))
        
        # Sort `data` by order (not sorted by default in Ed-Fi 6.1)
        data = list(filter(lambda x: "Create" in x['operations'], data))
        data = sorted(data, key=lambda x: x['order'])

        ordered_endpoints = []
        for e in data:
            if e["resource"].startswith("/" + self.lightbeam.config["namespace"] + "/"):
                ordered_endpoints.append(e["resource"].replace('/' + self.lightbeam.config["namespace"] + '/', ""))
        return ordered_endpoints
    
    # Loads the Swagger JSON from the Ed-Fi API
    def load_swagger_docs(self):

        # If Swagger URLs are explicitly set, use them
        if self.config.get("descriptors_swagger_url", "")!="" and self.config.get("resources_swagger_url", ""):
            openapi = [
                {
                    "name": "descriptors",
                    "endpointUri": self.config["descriptors_swagger_url"],
                }, 
                {
                    "name": "resources",
                    "endpointUri": self.config["resources_swagger_url"],
                }
            ]
        # If the metadata URL is set (pulled from root metadata file earlier), then pull endpoint urls from metadata
        elif self.config.get("open_api_metadata_url", "")!="":
            # grab Descriptors and Resources swagger URLs
            try:
                self.logger.debug("fetching swagger docs...")
                response = self.get_with_protocol_fallback(self.config["open_api_metadata_url"], 'open_api_metadata_url')
                if not response.ok:
                    raise Exception("OpenAPI metadata URL returned status {0} ({1})".format(response.status_code, (response.content[:75] + "...") if len(response.content)>75 else response.content))
                openapi = response.json()

            except Exception as e:
                self.logger.critical("Unable to load Swagger docs from API... terminating. Check your `edfi_api.open_api_metadata_url` and/or manually specify `edfi_api.descriptors_swagger_url` and `edfi_api.resources_swagger_url`.")
        # We can only get here if the API doesn't publish its metadata AND the user didn't configure Swagger URLs
        else:
            self.logger.critical("Swagger docs for the API were not discoverable... please manually specify them in `lightbeam.yaml` as `edfi_api.descriptors_swagger_url` and `edfi_api.resources_swagger_url`.")
            

        # load (or re-use cached) Descriptors and Resources swagger
        self.descriptors_swagger = None
        self.resources_swagger = None
        
        if self.lightbeam.track_state:
            cache_dir = os.path.join(self.lightbeam.config["state_dir"], "cache")
            if not os.path.isdir(cache_dir):
                self.logger.debug("creating cache dir {0}".format(cache_dir))
                os.mkdir(cache_dir)

        for endpoint in openapi:
            endpoint_type = endpoint["name"].lower()
            if endpoint_type=="descriptors" or endpoint_type=="resources":
                swagger_url = endpoint["endpointUri"]
                if self.lightbeam.track_state:
                    url_hash = hashlog.get_hash_string(swagger_url)
                    file = os.path.join(cache_dir, f"swagger-{endpoint_type}-{url_hash}.json")
                if (
                    self.lightbeam.track_state  # we have a state_dir in which to store
                    and not self.lightbeam.wipe # we aren't clearing the cache
                    and os.path.isfile(file)    # the cache file exists
                    and time.time()-os.path.getmtime(file)<self.SWAGGER_CACHE_TTL # cache file isn't expired
                ):
                    self.logger.debug(f"re-using cached {endpoint_type} swagger doc (from {file})...")
                    with open(file) as f:
                        swagger = json.load(f)
                else:
                    self.logger.debug(f"fetching {endpoint_type} swagger doc...")
                    try:
                        response = self.get_with_protocol_fallback(self.config[endpoint_type], endpoint_type)
                        if not response.ok:
                            raise Exception("OpenAPI metadata URL returned status {0} ({1})".format(response.status_code, (response.content[:75] + "...") if len(response.content)>75 else response.content))
                        swagger = response.json()

                    except Exception as e:
                        self.logger.critical(f"Unable to load {endpoint_type} Swagger from API... terminating. Check API connectivity.")

                    if self.lightbeam.track_state:
                        self.logger.debug(f"(saving to {file})")
                        with open(file, 'w') as f:
                            json.dump(swagger, f)
                
            if endpoint_type=="descriptors": self.descriptors_swagger = swagger
            if endpoint_type=="resources": self.resources_swagger = swagger

    # Loops over all descriptor endpoints (from Descriptors swagger) and fetches all valid
    # Descriptor values for each Descriptor. These values can then be used by
    # `validate_endpoint()` to check for invalid descriptor values before `send`ing.
    async def load_descriptors_values(self):
        # get token with which to send requests
        self.do_oauth()

        self.logger.debug("loading descriptor values...")
        if self.lightbeam.track_state:
            cache_dir = os.path.join(self.lightbeam.config["state_dir"], "cache")
            if not os.path.isdir(cache_dir):
                self.logger.debug("creating cache dir {0}".format(cache_dir))
                os.mkdir(cache_dir)
        
            # check for cached descriptor values
            url_hash = hashlog.get_hash_string(self.config["base_url"])
            cache_file = os.path.join(cache_dir, f"descriptor-values-{url_hash}.csv")

        self.lightbeam.reset_counters()
        if (
            self.lightbeam.track_state     # we have a state_dir, and therefore a cache dir in which to save
            and not self.lightbeam.wipe    # we're not wiping/rebuilding the cache
            and os.path.isfile(cache_file) # the cache file exists
            and time.time()-os.path.getmtime(cache_file)<self.DESCRIPTORS_CACHE_TTL # the cache file isn't expired
        ):
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
            selector_backup = self.lightbeam.selector
            exclude_backup = self.lightbeam.exclude
            keep_keys_backup = self.lightbeam.keep_keys
            drop_keys_backup = self.lightbeam.drop_keys
            self.lightbeam.selector = "*Descriptors"
            self.lightbeam.exclude = ""
            self.lightbeam.keep_keys = "*"
            self.lightbeam.drop_keys = ""
            self.logger.debug(f"fetching descriptor values...")
            all_endpoints = self.get_sorted_endpoints()
            self.lightbeam.endpoints = self.apply_filters(all_endpoints)
            await self.lightbeam.fetcher.get_records(do_write=False, log_status_counts=False)
            self.descriptor_values = []
            for v in self.lightbeam.results:
                descriptor = ""
                for key in v.keys():
                    if key.endswith("Id"): descriptor = key[0:-2]
                self.descriptor_values.append([descriptor, v["namespace"], v["codeValue"], v["shortDescription"], v.get("description", "")])

            # save
            if self.lightbeam.track_state:
                self.logger.debug(f"saving descriptor values to {cache_file}...")
                header = ['descriptor', 'namespace', 'codeValue', 'shortDescription', 'description']
                with open(cache_file, 'w', encoding='UTF8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(header)
                    writer.writerows(self.descriptor_values)

            self.lightbeam.results = []
            self.lightbeam.selector = selector_backup
            self.lightbeam.exclude = exclude_backup
            self.lightbeam.keep_keys = keep_keys_backup
            self.lightbeam.drop_keys = drop_keys_backup
            self.prepare()


    # This function (and the helper below) walks through the swagger for a resource, following references,
    #  grabs all the required (nested) fields, and constructs a structure like this (for assessmentItem):
    # {
    #    "identificationCode": "identificationCode",
    #    "assessmentIdentifier": "assessmentReference.assessmentIdentifier",
    #    "namespace": "assessmentReference.namespace"
    # }
    # (The first element is a required attribute of the assessmentItem; the other two are required elements
    # of the required nested assessmentReference.)
    def get_params_for_endpoint(self, endpoint, type='required'):
        if "Descriptor" in endpoint: swagger = self.descriptors_swagger
        else: swagger = self.resources_swagger
        definition = util.get_swagger_ref_for_endpoint(self.lightbeam.config["namespace"], swagger, endpoint)
        if type=='required':
            return self.get_required_params_from_swagger(swagger, definition)
        else:
            # descriptor endpoints all have the same structure and identity fields:
            if "Descriptor" in endpoint:
                return { 'namespace':'namespace', 'codeValue':'codeValue', 'shortDescription':'shortDescription'}
            else:
                return self.get_identity_params_from_swagger(swagger, definition)

    def get_required_params_from_swagger(self, swagger, definition, prefix=""):
        params = {}
        schema = util.resolve_swagger_ref(swagger, definition)
        if not schema:
            self.logger.critical(f"Swagger contains neither `definitions` nor `components.schemas` - check that the Swagger is valid.")
        
        for prop in schema["required"]:
            if "$ref" in schema["properties"][prop].keys():
                sub_definition = schema["properties"][prop]["$ref"]
                sub_params = self.get_required_params_from_swagger(swagger, sub_definition, prefix=prop+".")
                for k,v in sub_params.items():
                    params[k] = v
            elif schema["properties"][prop]["type"]!="array":
                params[prop] = prefix + prop
        return params

    def get_identity_params_from_swagger(self, swagger, definition, prefix=""):
        params = {}
        schema = util.resolve_swagger_ref(swagger, definition)
        if not schema:
            self.logger.critical(f"Swagger contains neither `definitions` nor `components.schemas` - check that the Swagger is valid.")
        
        for prop in schema["properties"]:
            if prop.endswith("Reference") and "required" in schema.keys() and prop in schema['required'] and "$ref" in schema["properties"][prop].keys():
                sub_definition = schema["properties"][prop]["$ref"]
                sub_params = self.get_identity_params_from_swagger(swagger, sub_definition, prefix=prop+".")
                for k,v in sub_params.items():
                    params[k] = v
            elif "type" in schema["properties"][prop].keys() and schema["properties"][prop]["type"]!="array" and "x-Ed-Fi-isIdentity" in schema["properties"][prop].keys():
                params[prop] = prefix + prop
        return params
    
    def get_with_protocol_fallback(self, url, url_type):
        self.logger.debug(f"fetching {url_type}...")
        try:
            return requests.get(url, verify=self.lightbeam.config["connection"]["verify_ssl"])
        except Exception as e:
            try:
                swapped_url = url.replace("http://", "https://") if "http://" in url else url.replace("https://", "http://")
                return requests.get(swapped_url, verify=self.lightbeam.config["connection"]["verify_ssl"])
            except Exception as e:
                self.logger.critical(f"could not reach {url_type} {url} ({str(e)})")