import json
import requests
import asyncio, concurrent.futures
from urllib.parse import urlencode
from jsonschema import RefResolver
from jsonschema import Draft4Validator

from lightbeam import util
from lightbeam import hashlog


class Validator:

    MAX_VALIDATION_ERRORS_TO_DISPLAY = 10
    MAX_VALIDATE_TASK_QUEUE_SIZE = 100
    DEFAULT_VALIDATION_METHODS = ["schema", "descriptors", "uniqueness"]

    EDFI_GENERICS_TO_RESOURCES_MAPPING = {
        "educationOrganizations": ["localEducationAgencies", "stateEducationAgencies", "schools"],
    }
    EDFI_GENERIC_REFS_TO_PROPERTIES_MAPPING = {
        "educationOrganizationId": {
            "localEducationAgencies": "localEducationAgencyId",
            "stateEducationAgencies": "stateEducationAgencyId",
            "schools": "schoolId",
        },
    }

    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        self.logger = self.lightbeam.logger
        
    # Validates (selected) endpoints
    def validate(self):

        # The below should go in __init__(), but rely on lightbeam.config which is not yet available there.
        self.fail_fast_threshold = self.lightbeam.config.get("validate",{}).get("references",{}).get("max_failures", None)
        self.validation_methods = self.lightbeam.config.get("validate",{}).get("methods",self.DEFAULT_VALIDATION_METHODS)
        if type(self.validation_methods)==str and (self.validation_methods=="*" or self.validation_methods.lower()=='all'):
            self.validation_methods = self.DEFAULT_VALIDATION_METHODS
            self.validation_methods.append("references")
        self.validation_references_selector = self.lightbeam.config.get("validate",{}).get("references",{}).get("selector", [])
        for selector in self.validation_references_selector:
            if "." not in selector:
                self.logger.error(f"`config.validate.references.selector` {selector} is incorrectly formatted (should be `someEndpoint.someReference`, such as `studentSchoolAssociation.schoolReference`)")
        self.validation_references_behavior = self.lightbeam.config.get("validate",{}).get("references",{}).get("behavior", "exclude")
        if self.validation_references_behavior not in ["exclude", "include"]:
            self.logger.error(f"`config.validate.references.behavior` must be either `exclude` (default) or `include`)")
        self.validation_references_remote = self.lightbeam.config.get("validate",{}).get("references",{}).get("remote", True)
        if "references" in self.validation_methods and not self.validation_references_remote:
            self.logger.info(f"(references will only be validated against local data, since `config.validate.references.remote: False`)")

        self.lightbeam.api.load_swagger_docs()
        self.logger.info(f"validating by methods {self.validation_methods}...")
        if "descriptors" in self.validation_methods:
            # load remote descriptors
            asyncio.run(self.lightbeam.api.load_descriptors_values())
            self.lightbeam.reset_counters()
            self.load_local_descriptors()
        
        endpoints_with_data = self.lightbeam.get_endpoints_with_data()
        self.lightbeam.endpoints = self.lightbeam.api.apply_filters(endpoints_with_data)

        # structures for local and remote reference lookups to prevent repeated lookups for the same thing
        self.remote_reference_cache = {}
        self.local_reference_cache = {}

        for endpoint in self.lightbeam.endpoints:
            if "references" in self.validation_methods and "Descriptor" not in endpoint: # Descriptors have no references:
                # We don't want every `do_validate_payload()` to separately have to open and scan
                # local files looking for a matching payload; this pre-loads local data that
                # might resolve references from within payloads of this endpoint.
                # We assume that the data fits in memory; the largest Ed-Fi endpoints
                # (studentSectionAssociations, studentSchoolAttendanceEvents, etc.) contain references
                # to comparatively small datasets (sections, schools, students).
                self.build_local_reference_cache(endpoint)
            asyncio.run(self.validate_endpoint(endpoint))
        
        # write structured output (if needed)
        self.lightbeam.write_structured_output("validate")

        if self.lightbeam.metadata["total_records_processed"] == self.lightbeam.metadata["total_records_failed"]:
            self.logger.info("all payloads failed")
            exit(1) # signal to downstream tasks (in Airflow) all payloads failed
    
    def build_local_reference_cache(self, endpoint):
        swagger = self.lightbeam.api.resources_swagger
        definition = self.get_swagger_definition_for_endpoint(endpoint)
        references_structure = self.load_references_structure(swagger, definition)
        references_structure = self.rebalance_local_references_structure(references_structure)
        # more memory-efficient to load local data and populate cache for one endpoint at a time:
        for original_endpoint in references_structure.keys():
            endpoints_to_check = self.EDFI_GENERICS_TO_RESOURCES_MAPPING.get(original_endpoint, [original_endpoint])
            for endpoint in endpoints_to_check:
                if endpoint in self.local_reference_cache.keys():
                    # already loaded (when validating another endpoint); no need to reload
                    continue
                self.logger.debug(f"(discovering any local data for {endpoint}...)")
                endpoint_data = self.load_references_data(endpoint, references_structure)
                self.local_reference_cache[endpoint] = self.references_data_to_cache(endpoint, endpoint_data, references_structure)

    # this is (unfortunately) necessary to allow lookup of nested references in local payload
    # (for remote reference lookup, a flat dict of keys is passed to the Ed-Fi API and it takes care of nesting)
    def rebalance_local_references_structure(self, references_structure):
        if "objectiveAssessments" in references_structure.keys():
            if "assessmentIdentifier" in references_structure["objectiveAssessments"]:
                references_structure["objectiveAssessments"].remove("assessmentIdentifier")
                references_structure["objectiveAssessments"].append("assessmentReference.assessmentIdentifier")
            if "namespace" in references_structure["objectiveAssessments"]:
                references_structure["objectiveAssessments"].remove("namespace")
                references_structure["objectiveAssessments"].append("assessmentReference.namespace")
        return references_structure

    def references_data_to_cache(self, endpoint, endpoint_data, references_structure):
        cache = []
        structure = references_structure[endpoint]
        for payload in endpoint_data:
            sorted_keys = structure.copy()
            sorted_keys.sort(key=lambda x: x.split(".")[-1])
            cache_key = ''
            for key in sorted_keys:
                cache_key += f"{payload[key]}~~~"
            cache.append(cache_key)
        return cache

    def load_references_data(self, endpoint, references_structure):
        data = []
        data_files = self.lightbeam.get_data_files_for_endpoint(endpoint)
        for file_name in data_files:
            with open(file_name) as file:
                for line_counter, line in enumerate(file):
                    line = line.strip()
                    try:
                        payload = json.loads(line)
                    except Exception as e:
                        self.logger.warning(f"... (ignoring invalid JSON payload at {line_counter} of {file_name})")
                    ref_payload = {}
                    for key in references_structure[endpoint]:
                        key = self.EDFI_GENERIC_REFS_TO_PROPERTIES_MAPPING.get(key, {}).get(endpoint, key)
                        tmpdata = payload
                        for subkey in key.split("."):
                            tmpdata = tmpdata[subkey]
                        ref_payload[key] = tmpdata
                    data.append(ref_payload)
        return data

    def load_references_structure(self, swagger, definition):
        if "definitions" in swagger.keys():
            schema = swagger["definitions"][definition]
        elif "components" in swagger.keys() and "schemas" in swagger["components"].keys():
            schema = swagger["components"]["schemas"][definition]
        else:
            self.logger.critical(f"Swagger contains neither `definitions` nor `components.schemas` - check that the Swagger is valid.")
        references = {}
        prefixes_to_remove = ["#/definitions/", "#/components/schemas/"]
        for k in schema["properties"].keys():
            if k.endswith("Reference"):
                original_endpoint = util.pluralize_endpoint(k.replace("Reference", ""))

                # this deals with the fact that an educationOrganizationReference may be to a school, LEA, etc.:
                endpoints_to_check = self.EDFI_GENERICS_TO_RESOURCES_MAPPING.get(original_endpoint, [original_endpoint])
                
                for endpoint in endpoints_to_check:
                    ref_definition = schema["properties"][k]["$ref"]
                    for prefix_to_remove in prefixes_to_remove:
                        ref_definition = ref_definition.replace(prefix_to_remove,"")
                    # look up (in swagger) the required fields for any reference
                    ref_properties = self.load_reference(swagger, ref_definition)
                    references[endpoint] = ref_properties
            elif "items" in schema["properties"][k].keys():
                # this deals with a property which is a list of items which themselves contain References
                # (example: studentAssessment.studentObjectiveAssessments contain an objectiveAssessmentReference)
                nested_definition = schema["properties"][k]["items"]["$ref"]
                for prefix_to_remove in prefixes_to_remove:
                    nested_definition = nested_definition.replace(prefix_to_remove,"")
                nested_references = self.load_references_structure(swagger, nested_definition)
                references.update(nested_references)
        return references

    def load_reference(self, swagger, definition):
        properties = []
        if "definitions" in swagger.keys():
            schema = swagger["definitions"][definition]
        elif "components" in swagger.keys() and "schemas" in swagger["components"].keys():
            schema = swagger["components"]["schemas"][definition]
        else:
            self.logger.critical(f"Swagger contains neither `definitions` nor `components.schemas` - check that the Swagger is valid.")
        for k in schema["properties"].keys():
            if k in schema.get("required",[]):
                properties.append(k)
        return properties

    def get_swagger_definition_for_endpoint(self, endpoint):
        return util.camel_case(self.lightbeam.config["namespace"]) + "_" + util.singularize_endpoint(endpoint)
    
    # Validates a single endpoint based on the Swagger docs
    async def validate_endpoint(self, endpoint):
        self.lightbeam.metadata["resources"].update({endpoint: {}})
        definition = self.get_swagger_definition_for_endpoint(endpoint)
        data_files = self.lightbeam.get_data_files_for_endpoint(endpoint)
        tasks = []
        total_counter = 0
        self.lightbeam.num_errors = 0
        self.lightbeam.metadata["resources"][endpoint].update({
            "records_processed": 0,
            "records_skipped": 0,
            "records_failed": 0
        })
        for file_name in data_files:
            self.logger.info(f"validating {file_name} against {definition} schema...")
            with open(file_name) as file:
                for line_counter, line in enumerate(file):
                    total_counter += 1
                    data = line.strip()
                        
                    tasks.append(asyncio.create_task(
                        self.do_validate_payload(endpoint, file_name, data, line_counter)))
                
                    if len(tasks) >= self.MAX_VALIDATE_TASK_QUEUE_SIZE:
                        await self.lightbeam.do_tasks(tasks, total_counter, log_status_counts=False)
                        tasks = []
                        if total_counter%1000==0:
                            self.logger.info(f"(processed {total_counter}...)")
                    
                    # update metadata counts
                    self.lightbeam.metadata["resources"][endpoint]["records_processed"] = total_counter
                    self.lightbeam.metadata["resources"][endpoint]["records_skipped"] = self.lightbeam.num_skipped
                    self.lightbeam.metadata["resources"][endpoint]["records_failed"] = self.lightbeam.num_errors
                    
                    # implement "fail fast" feature:
                    if self.fail_fast_threshold is not None and self.lightbeam.num_errors >= self.fail_fast_threshold:
                        self.lightbeam.shutdown("validate")
                        self.logger.critical(f"... STOPPING; found {self.lightbeam.num_errors} >= validate.references.max_failures={self.fail_fast_threshold} VALIDATION ERRORS.")
                        break

            if len(tasks)>0: await self.lightbeam.do_tasks(tasks, total_counter, log_status_counts=False)

            if self.lightbeam.num_errors==0: self.logger.info(f"... all lines validate ok!")
            else:
                num_others = self.lightbeam.num_errors - self.MAX_VALIDATION_ERRORS_TO_DISPLAY
                if self.lightbeam.num_errors > self.MAX_VALIDATION_ERRORS_TO_DISPLAY:
                    self.logger.warn(f"... and {num_others} others!")
                self.logger.warn(f"... VALIDATION ERRORS on {self.lightbeam.num_errors} of {line_counter} lines in {file_name}; see details above.")


    async def do_validate_payload(self, endpoint, file_name, data, line_counter):
        if self.fail_fast_threshold is not None and self.lightbeam.num_errors >= self.fail_fast_threshold: return
        definition = self.get_swagger_definition_for_endpoint(endpoint)
        if "Descriptor" in endpoint:
            swagger = self.lightbeam.api.descriptors_swagger
        else:
            swagger = self.lightbeam.api.resources_swagger
            
        if "definitions" in swagger.keys():
            resource_schema = swagger["definitions"][definition]
        elif "components" in swagger.keys() and "schemas" in swagger["components"].keys():
            resource_schema = swagger["components"]["schemas"][definition]
        else:
            self.logger.critical(f"Swagger contains neither `definitions` nor `components.schemas` - check that the Swagger is valid.")
        
        resolver = RefResolver("test", swagger, swagger)
        validator = Draft4Validator(resource_schema, resolver=resolver)
        identity_params_structure = self.lightbeam.api.get_params_for_endpoint(endpoint, type='identity')
        distinct_params = []

        # check payload is valid JSON
        try:
            payload = json.loads(data)
        except Exception as e:
            self.log_validation_error(endpoint, file_name, line_counter, "json", f"invalid JSON {str(e).replace(' line 1','')}")
            return

        # check payload obeys Swagger schema
        if "schema" in self.validation_methods:
            try:
                validator.validate(payload)
            except Exception as e:
                e_path = [str(x) for x in list(e.path)]
                context = ""
                if len(e_path)>0: context = " in " + " -> ".join(e_path)
                self.log_validation_error(endpoint, file_name, line_counter, "schema", f"{str(e.message)} {context}")
                return

        # check descriptor values are valid
        if "descriptors" in self.validation_methods:
            error_message = self.has_invalid_descriptor_values(payload, path="")
            if error_message != "":
                self.log_validation_error(endpoint, file_name, line_counter, "descriptors", error_message)
                return

        # check natural keys are unique
        if "uniqueness" in self.validation_methods:
            params = json.dumps(util.interpolate_params(identity_params_structure, payload))
            params_hash = hashlog.get_hash(params)
            if params_hash in distinct_params:
                self.log_validation_error(endpoint, file_name, line_counter, "uniqueness", "duplicate value(s) for natural key(s): {params}")
                return
            else: distinct_params.append(params_hash)

        # check references values are valid
        if "references" in self.validation_methods and "Descriptor" not in endpoint: # Descriptors have no references
            self.lightbeam.api.do_oauth()
            error_message = self.has_invalid_references(endpoint, payload, path="")
            if error_message != "":
                self.log_validation_error(endpoint, file_name, line_counter, "references", error_message)
                
                
    def log_validation_error(self, endpoint, file_name, line_number, method, message):
        if self.lightbeam.num_errors < self.MAX_VALIDATION_ERRORS_TO_DISPLAY:
            self.logger.warning(f"... VALIDATION ERROR (line {line_number}): {message}")
        self.lightbeam.num_errors += 1

        # update run metadata...
        failures = self.lightbeam.metadata["resources"][endpoint].get("failures", [])
        do_append = True
        for index, item in enumerate(failures):
            if item["method"]==method and item["message"]==message and item["file"]==file_name:
                failures[index]["line_numbers"].append(line_number)
                failures[index]["count"] += 1
                do_append = False
        if do_append:
            failure = {
                'method': method,
                'message': message,
                'file': file_name,
                'line_numbers': [line_number],
                'count': 1
            }
            failures.append(failure)
        self.lightbeam.metadata["resources"][endpoint]["failures"] = failures
    
    def load_local_descriptors(self):
        local_descriptors = []
        all_endpoints = self.lightbeam.api.get_sorted_endpoints()
        descriptor_endpoints = [x for x in all_endpoints if x.endswith("Descriptors")]
        for descriptor in descriptor_endpoints:
            data_files = self.lightbeam.get_data_files_for_endpoint(descriptor)
            for file_name in data_files:
                with open(file_name) as file:
                    # process each line
                    for line in file:
                        local_descriptors.append(json.loads(line.strip()))
        self.local_descriptors = local_descriptors
    
    # Validates descriptor values for a single payload (returns an error message or empty string)
    def has_invalid_descriptor_values(self, payload, path=""):
        for k in payload.keys():
            if isinstance(payload[k], dict):
                value = self.has_invalid_descriptor_values(payload[k], path+("." if path!="" else "")+k)
                if value!="": return value
            elif isinstance(payload[k], list):
                for i in range(0, len(payload[k])):
                    value = self.has_invalid_descriptor_values(payload[k][i], path+("." if path!="" else "")+k+"["+str(i)+"]")
                    if value!="": return value
            elif isinstance(payload[k], str) and k.endswith("Descriptor"):
                if "#" not in payload[k]:
                    return payload[k] + f" is not a valid descriptor value for {k}" + (" (at " + path + ")" if path!="" else "") + "; format should be like `uri://namespace.org/SomeDescriptor#SomeValue`"
                namespace = payload[k].split("#")[0]
                codeValue = payload[k].split("#")[1]
                # check if it's a local descriptor:
                matching_local_descriptors = list(filter(lambda descriptor:
                                                            type(descriptor)==dict
                                                            and descriptor.get("namespace", "")==namespace
                                                            and descriptor.get("codeValue", "")==codeValue
                                                         , self.local_descriptors or []))
                if len(matching_local_descriptors)>0: return ""
                # check if it's a remote descriptor:
                if not self.is_valid_descriptor_value(namespace, codeValue):
                    return payload[k] + f" is not a valid descriptor value for {k}" + (" (at " + path + ")" if path!="" else "")
        return ""

    # Validates descriptor values for a single payload (returns an error message or empty string)
    def has_invalid_references(self, endpoint, payload, path=""):
        for k in payload.keys():
            if isinstance(payload[k], dict) and not k.endswith("Reference"):
                value = self.has_invalid_references(endpoint, payload[k], path+("." if path!="" else "")+k)
                if value!="": return value
            elif isinstance(payload[k], list):
                for i in range(0, len(payload[k])):
                    value = self.has_invalid_references(endpoint, payload[k][i], path+("." if path!="" else "")+k+"["+str(i)+"]")
                    if value!="": return value
            elif isinstance(payload[k], dict) and k.endswith("Reference"):
                check_this_reference = (
                    (f"{endpoint}.{path}{k}" in self.validation_references_selector and self.validation_references_behavior=="include")
                    or (f"{endpoint}.{path}{k}" not in self.validation_references_selector and self.validation_references_behavior=="exclude")
                )
                if not check_this_reference: continue
                is_valid_reference = False
                original_endpoint = util.pluralize_endpoint(k.replace("Reference",""))

                params = payload[k].copy()
                if "link" in params.keys(): del params["link"]
                
                # this deals with the fact that an educationOrganizationReference may be to a school, LEA, etc.:
                endpoints_to_check = self.EDFI_GENERICS_TO_RESOURCES_MAPPING.get(original_endpoint, [original_endpoint])
                for endpt in endpoints_to_check:
                    # check if it's a local reference:
                    if endpt not in self.local_reference_cache.keys(): break
                    # construct cache_key for reference
                    cache_key = self.get_cache_key(params)
                    if cache_key in self.local_reference_cache[endpt]:
                        is_valid_reference = True
                        break
                if not is_valid_reference and self.validation_references_remote: # not found in local data...
                    for endpt in endpoints_to_check:
                        # check if it's a remote reference:
                        value = self.remote_reference_exists(endpt, params)
                        if value:
                            is_valid_reference = True
                            break
                if not is_valid_reference:
                    return f"payload contains an invalid {k} " + (" (at "+path+"): " if path!="" else ": ") + json.dumps(params)
        return ""

    # Tells you if a specified descriptor value is valid or not
    def is_valid_descriptor_value(self, namespace, codeValue):
        for row in self.lightbeam.api.descriptor_values:
            if row[1]==namespace and row[2]==codeValue:
                return True
        return False

    @staticmethod
    def get_cache_key(payload):
        cache_key = ''
        for k in payload.keys():
            cache_key += f"{payload[k]}~~~"
        return cache_key
    
    def remote_reference_exists(self, endpoint, params):
        # check cache:
        if endpoint not in self.remote_reference_cache.keys():
            self.remote_reference_cache[endpoint] = []
        cache_key = self.get_cache_key(params)
        if cache_key in self.remote_reference_cache[endpoint]:
            return True
        # print(f"remote reference lookup to {endpoint} for {params}")
        # do remote lookup
        curr_token_version = int(str(self.lightbeam.token_version))
        while True: # this is not great practice, but an effective way (along with the `break` below) to achieve a do:while loop
            try:
                # send GET request
                response = requests.get(
                    util.url_join(self.lightbeam.api.config["data_url"], self.lightbeam.config["namespace"], endpoint),
                    params=params,
                    verify=self.lightbeam.config["connection"]["verify_ssl"],
                    headers=self.lightbeam.api.headers
                    )
                body = response.text
                status = str(response.status_code)
                if status!='401':
                    self.lightbeam.increment_status_counts(status)
                if status=='401':
                    # this could be broken out to a separate function call,
                    # but not doing so should help keep the critical section small
                    if self.lightbeam.token_version == curr_token_version:
                        self.lightbeam.lock.acquire()
                        self.lightbeam.api.update_oauth()
                        self.lightbeam.lock.release()
                    else:
                        pass # await asyncio.sleep(1)
                    curr_token_version = int(str(self.lightbeam.token_version))
                elif status=='404' or status=='400':
                    return False
                elif status in ['200', '201']:
                    # 200 response might still return zero matching records...
                    if len(json.loads(body))>0:
                        # add to cache
                        if cache_key not in self.remote_reference_cache[endpoint]:
                            self.remote_reference_cache[endpoint].append(cache_key)
                        return True
                    else: return False
                else:
                    print(f"Status: {status}")
                    print(f"Body: {body}")
                    self.logger.warn(f"Unable to resolve reference for {endpoint}... API returned {status} status.")
                    return False

            except RuntimeError as e:
                pass # await asyncio.sleep(1)
            except Exception as e:
                self.logger.critical(f"Unable to resolve reference for {endpoint} from API... terminating. Check API connectivity.")

