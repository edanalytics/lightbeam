import json
import asyncio
from jsonschema import RefResolver
from jsonschema import Draft4Validator

from lightbeam import util
from lightbeam import hashlog


class Validator:

    MAX_VALIDATION_ERRORS_TO_DISPLAY = 10

    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        self.logger = self.lightbeam.logger
    
    # Validates (selected) endpoints
    def validate(self):
        self.lightbeam.api.load_swagger_docs()
        asyncio.run(self.lightbeam.api.load_descriptors_values())

        for endpoint in self.lightbeam.endpoints:
            if "Descriptor" in endpoint:
                self.validate_endpoint(self.lightbeam.api.descriptors_swagger, endpoint)
            else:
                self.validate_endpoint(self.lightbeam.api.resources_swagger, endpoint)

    # Validates a single endpoint based on the Swagger docs
    def validate_endpoint(self, swagger, endpoint):
        definition = "edFi_" + util.singularize_endpoint(endpoint)
        resource_schema = swagger["definitions"][definition]

        resolver = RefResolver("test", swagger, swagger)
        validator = Draft4Validator(resource_schema, resolver=resolver)
        params_structure = self.lightbeam.api.get_params_for_endpoint(endpoint)
        distinct_params = []

        endpoint_data_files = self.lightbeam.get_data_files_for_endpoint(endpoint)
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
                        if num_errors < self.MAX_VALIDATION_ERRORS_TO_DISPLAY:
                            self.logger.warning(f"... VALIDATION ERROR (line {counter}): invalid JSON" + str(e).replace(" line 1",""))
                        num_errors += 1
                        continue

                    # check payload obeys Swagger schema
                    try:
                        validator.validate(instance)
                    except Exception as e:
                        if num_errors < self.MAX_VALIDATION_ERRORS_TO_DISPLAY:
                            e_path = [str(x) for x in list(e.path)]
                            context = ""
                            if len(e_path)>0: context = " in " + " -> ".join(e_path)
                            self.logger.warning(f"... VALIDATION ERROR (line {counter}): " + str(e.message) + context)
                        num_errors += 1
                        continue

                    # check descriptor values are valid
                    error_message = self.has_invalid_descriptor_values(instance)
                    if error_message != "":
                        if num_errors < self.MAX_VALIDATION_ERRORS_TO_DISPLAY:
                            self.logger.warning(f"... VALIDATION ERROR (line {counter}): " + error_message)
                        num_errors += 1
                        continue

                    # check natural keys are unique
                    params = json.dumps(util.interpolate_params(params_structure, line))
                    hash = hashlog.get_hash(params)
                    if hash in distinct_params:
                        if num_errors < self.MAX_VALIDATION_ERRORS_TO_DISPLAY:
                            self.logger.warning(f"... VALIDATION ERROR (line {counter}): duplicate value(s) for natural key(s): {params}")
                        num_errors += 1
                        continue
                    else: distinct_params.append(hash)
                
                if num_errors==0: self.logger.info(f"... all lines validate ok!")
                else:
                    num_others = num_errors - self.MAX_VALIDATION_ERRORS_TO_DISPLAY
                    if num_errors > self.MAX_VALIDATION_ERRORS_TO_DISPLAY:
                        self.logger.critical(f"... and {num_others} others!")
                    self.logger.critical(f"... VALIDATION ERRORS on {num_errors} of {counter} lines in {file}; see details above.")
    
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
            elif isinstance(payload[k], str) and "Descriptor" in k:
                namespace = payload[k].split("#")[0]
                codeValue = payload[k].split("#")[1]
                if not self.is_valid_descriptor_value(namespace, codeValue):
                    return payload[k] + f" is not a valid descriptor value for {k}" + (" (at " + path + ")" if path!="" else "")
        return ""

    # Tells you if a specified descriptor value is valid or not
    def is_valid_descriptor_value(self, namespace, codeValue):
        for row in self.lightbeam.api.descriptor_values:
            if row[1]==namespace and row[2]==codeValue:
                return True
        return False
    