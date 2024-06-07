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
    MAX_VALIDATE_TASK_QUEUE_SIZE = 50
    DEFAULT_VALIDATION_METHODS = ["schema", "descriptors", "uniqueness"]

    EDFI_GENERICS_TO_RESOURCES_MAPPING = {
        "educationOrganizations": ["localEducationAgencies", "stateEducationAgencies", "schools"],
        "objectiveAssessment": [""]
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
        self.lightbeam.api.load_swagger_docs()
        validation_methods = self.lightbeam.config.get("validate",{}).get("methods",self.DEFAULT_VALIDATION_METHODS)
        if type(validation_methods)==str and (validation_methods=="*" or validation_methods.lower()=='all'):
            validation_methods = self.DEFAULT_VALIDATION_METHODS
            validation_methods.append("references")
        self.logger.info(f"validating by methods {validation_methods}...")
        if "descriptors" in validation_methods:
            # load remote descriptors
            asyncio.run(self.lightbeam.api.load_descriptors_values())
            self.lightbeam.reset_counters()
            self.load_local_descriptors()
        
        endpoints = self.lightbeam.endpoints
        for endpoint in endpoints:
            if "references" in validation_methods and "Descriptor" not in endpoint: # Descriptors have no references:
                # We don't want every `do_validate_payload()` to separately have to open and scan
                # local files looking for a matching payload; this pre-loads local data that
                # might resolve references from within payloads of this endpoint.
                # We assume that the data fits in memory; the largest Ed-Fi endpoints
                # (studentSectionAssociations, studentSchoolAttendanceEvents, etc.) contain references
                # to comparatively datasets (sections, schools, students).
                self.load_local_reference_data(endpoint)
                # create a structure which remote reference lookups can populate to prevent repeated lookups for the same thing
                self.remote_reference_cache = {}
            asyncio.run(self.validate_endpoint(endpoint))
    
    def load_local_reference_data(self, endpoint):
        self.local_references = {}
        swagger = self.lightbeam.api.resources_swagger
        definition = self.get_swagger_definition_for_endpoint(endpoint)
        references_structure = self.load_references_structure(swagger, definition)
        references_structure = self.rebalance_local_references_structure(references_structure)
        references_data = self.load_references_data(references_structure)
        self.local_references.update(references_data)

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

    def load_references_data(self, references_structure):
        data = {}
        for original_endpoint in references_structure.keys():
            endpoints_to_check = self.EDFI_GENERICS_TO_RESOURCES_MAPPING.get(original_endpoint, [original_endpoint])
            for endpoint in endpoints_to_check:
                data[endpoint] = []
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
                            data[endpoint].append(ref_payload)
        return data

    def load_references_structure(self, swagger, definition):
        if "definitions" in swagger.keys():
            schema = swagger["definitions"][definition]
        elif "components" in swagger.keys() and "schemas" in swagger["components"].keys():
            schema = swagger["components"]["schemas"][definition]
        else:
            self.logger.critical(f"Swagger contains neither `definitions` nor `components.schemas` - check that the Swagger is valid.")
        references = {}
        for k in schema["properties"].keys():
            if k.endswith("Reference"):
                original_endpoint = util.pluralize_endpoint(k.replace("Reference", ""))

                # this deals with the fact that an educationOrganizationReference may be to a school, LEA, etc.:
                endpoints_to_check = self.EDFI_GENERICS_TO_RESOURCES_MAPPING.get(original_endpoint, [original_endpoint])
                
                for endpoint in endpoints_to_check:
                    ref_definition = schema["properties"][k]["$ref"].replace("#/definitions/", "")
                    # look up (in swagger) the required fields for any reference
                    ref_properties = self.load_reference(swagger, ref_definition)
                    references[endpoint] = ref_properties
            elif "items" in schema["properties"][k].keys():
                # this deals with a property which is a list of items which themselves contain References
                # (example: studentAssessment.studentObjectiveAssessments contain an objectiveAssessmentReference)
                nested_definition = schema["properties"][k]["items"]["$ref"].replace("#/definitions/", "")
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
        partial_threshold = self.lightbeam.config.get("validate",{}).get("references",{}).get("partial", False)
        fail_fast_threshold = self.lightbeam.config.get("validate",{}).get("references",{}).get("max_failures", 10)
        definition = self.get_swagger_definition_for_endpoint(endpoint)
        data_files = self.lightbeam.get_data_files_for_endpoint(endpoint)
        tasks = []
        total_counter = 0
        for file_name in data_files:
            self.logger.info(f"validating {file_name} against {definition} schema...")
            with open(file_name) as file:
                self.lightbeam.num_errors = 0
                for line_counter, line in enumerate(file):
                    total_counter += 1
                    data = line.strip()
                        
                    tasks.append(asyncio.create_task(
                        self.do_validate_payload(endpoint, file_name, data, line_counter)))
                
                    if total_counter%self.MAX_VALIDATE_TASK_QUEUE_SIZE==0:
                        await self.lightbeam.do_tasks(tasks, total_counter, log_status_counts=False)
                        tasks = []
                    
                    # implement "fail fast" feature:
                    if self.lightbeam.num_errors >= fail_fast_threshold:
                        self.logger.critical(f"... STOPPING; found {self.lightbeam.num_errors} >= validate.references.max_failures={fail_fast_threshold} VALIDATION ERRORS.")
                        break
                    # implement "succeed fast" feature:
                    if self.lightbeam.num_errors==0 and partial_threshold and total_counter>=partial_threshold:
                        self.logger.info(f"... STOPPING; all {total_counter} tested payloads >= validate.references.partial={partial_threshold} validated successfully.")
                        return

            if len(tasks)>0: await self.lightbeam.do_tasks(tasks, total_counter, log_status_counts=False)
            
            if self.lightbeam.num_errors==0: self.logger.info(f"... all lines validate ok!")
            else:
                num_others = self.lightbeam.num_errors - self.MAX_VALIDATION_ERRORS_TO_DISPLAY
                if self.lightbeam.num_errors > self.MAX_VALIDATION_ERRORS_TO_DISPLAY:
                    self.logger.critical(f"... and {num_others} others!")
                self.logger.critical(f"... VALIDATION ERRORS on {self.lightbeam.num_errors} of {line_counter} lines in {file_name}; see details above.")


    async def do_validate_payload(self, endpoint, file_name, data, line_counter):
        validation_methods = self.lightbeam.config.get("validate",{}).get("methods",self.DEFAULT_VALIDATION_METHODS)
        if type(validation_methods)==str and (validation_methods=="*" or validation_methods.lower()=='all'):
            validation_methods = self.DEFAULT_VALIDATION_METHODS
            validation_methods.append("references")
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
        params_structure = self.lightbeam.api.get_params_for_endpoint(endpoint)
        distinct_params = []

        # check payload is valid JSON
        try:
            payload = json.loads(data)
        except Exception as e:
            if self.lightbeam.num_errors < self.MAX_VALIDATION_ERRORS_TO_DISPLAY:
                self.logger.warning(f"... VALIDATION ERROR (line {line_counter}): invalid JSON" + str(e).replace(" line 1",""))
            self.lightbeam.num_errors += 1
            return

        # check payload obeys Swagger schema
        if "schema" in validation_methods:
            try:
                validator.validate(payload)
            except Exception as e:
                if self.lightbeam.num_errors < self.MAX_VALIDATION_ERRORS_TO_DISPLAY:
                    e_path = [str(x) for x in list(e.path)]
                    context = ""
                    if len(e_path)>0: context = " in " + " -> ".join(e_path)
                    self.logger.warning(f"... VALIDATION ERROR (line {line_counter}): " + str(e.message) + context)
                self.lightbeam.num_errors += 1
                return

        # check descriptor values are valid
        if "descriptors" in validation_methods:
            error_message = self.has_invalid_descriptor_values(payload, path="")
            if error_message != "":
                if self.lightbeam.num_errors < self.MAX_VALIDATION_ERRORS_TO_DISPLAY:
                    self.logger.warning(f"... VALIDATION ERROR (line {line_counter}): " + error_message)
                self.lightbeam.num_errors += 1
                return

        # check natural keys are unique
        if "uniqueness" in validation_methods:
            params = json.dumps(util.interpolate_params(params_structure, payload))
            params_hash = hashlog.get_hash(params)
            if params_hash in distinct_params:
                if self.lightbeam.num_errors < self.MAX_VALIDATION_ERRORS_TO_DISPLAY:
                    self.logger.warning(f"... VALIDATION ERROR (line {line_counter}): duplicate value(s) for natural key(s): {params}")
                self.lightbeam.num_errors += 1
                return
            else: distinct_params.append(params_hash)

        # check references values are valid
        if "references" in validation_methods and "Descriptor" not in endpoint: # Descriptors have no references
            self.lightbeam.api.do_oauth()
            error_message = self.has_invalid_references(payload, path="")
            if error_message != "":
                if self.lightbeam.num_errors < self.MAX_VALIDATION_ERRORS_TO_DISPLAY:
                    self.logger.warning(f"... VALIDATION ERROR (line {line_counter}): " + error_message)
                self.lightbeam.num_errors += 1
                
                
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
    def has_invalid_references(self, payload, path=""):
        for k in payload.keys():
            if isinstance(payload[k], dict) and not k.endswith("Reference"):
                value = self.has_invalid_references(payload[k], path+("." if path!="" else "")+k)
                if value!="": return value
            elif isinstance(payload[k], list):
                for i in range(0, len(payload[k])):
                    value = self.has_invalid_references(payload[k][i], path+("." if path!="" else "")+k+"["+str(i)+"]")
                    if value!="": return value
            elif isinstance(payload[k], dict) and k.endswith("Reference"):
                is_valid_reference = False
                original_endpoint = util.pluralize_endpoint(k.replace("Reference",""))

                # this deals with the fact that an educationOrganizationReference may be to a school, LEA, etc.:
                endpoints_to_check = self.EDFI_GENERICS_TO_RESOURCES_MAPPING.get(original_endpoint, [original_endpoint])
                for endpoint in endpoints_to_check:
                    # check if it's a local reference:
                    if endpoint not in self.local_references.keys(): break
                    for local_payload in self.local_references[endpoint]:
                        instance_matches = True
                        for key,value in local_payload.items():
                            key = self.EDFI_GENERIC_REFS_TO_PROPERTIES_MAPPING.get(key, {}).get(endpoint, key)
                            local_key = key
                            if "." in key:
                                local_key = key.split(".")[-1]
                            if payload[k][local_key]!=value:
                                instance_matches = False
                                break
                        if instance_matches:
                            is_valid_reference = True
                            break
                    if is_valid_reference:
                        break
                if not is_valid_reference: # not found in local data...
                    for endpoint in endpoints_to_check:
                        # check if it's a remote reference:
                        params = payload[k].copy()
                        if "link" in params.keys(): del params["link"]
                        value = self.remote_reference_exists(endpoint, params)
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
    
    def remote_reference_exists(self, endpoint, params):
        # check cache:
        if endpoint=='students' and 'studentUniqueId' in params.keys(): return True
        if endpoint not in self.remote_reference_cache.keys():
            self.remote_reference_cache[endpoint] = []
        cache_key = ''
        for k in sorted(params.keys()):
            cache_key += f"{params[k]}-"
        if cache_key in self.remote_reference_cache[endpoint]:
            return True
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
                elif status=='404':
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

