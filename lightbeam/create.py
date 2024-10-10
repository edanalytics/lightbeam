import os
import json
import yaml
from lightbeam import util

class Creator:

    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        self.logger = self.lightbeam.logger
        self.template_folder = "templates/"
        self.earthmover_file = "earthmover.yml"
    
    def create(self):
        os.makedirs(self.template_folder, exist_ok=True)
        earthmover_yaml = {}
        # check if file exists!
        if os.path.isfile(self.earthmover_file):
            with open(self.earthmover_file) as file:
                earthmover_yaml = yaml.safe_load(file)
        for endpoint in self.lightbeam.endpoints:
            if endpoint in (earthmover_yaml.get("destinations", {}) or {}).keys():
                self.logger.critical(f"The file `{self.earthmover_file}` already exists in the current directory and contains `$destinations.{endpoint}`; to re-create it, please first manually remove it (and `{self.template_folder}{endpoint}.jsont`).")
            self.create_jsont(endpoint)
        # write out earthmover_yaml
        if not os.path.isfile(self.earthmover_file):
            self.logger.info(f"creating file `{self.earthmover_file}`...")
            with open(self.earthmover_file, 'w+') as file:
                file.write("""version: 2.0

# This is an earthmover.yml file, generated with `lightbeam create`, for creating Ed-Fi JSON payloads
# using earthmover. See https://github.com/edanalytics/earthmover for documentation.

# Define your source data here:
sources:
  # Example:
  # mysource:
  #   file: path/to/myfile.csv
  # ...

# (If needed, define your data transformations here:)
# transformations:

destinations:""")
                for endpoint in self.lightbeam.endpoints:
                    file.write(self.create_em_destination_node(endpoint))
        else:
            self.logger.info(f"appending to file `{self.earthmover_file}`...")
            with open(self.earthmover_file, 'a+') as file:
                for endpoint in self.lightbeam.endpoints:
                    file.write(self.create_em_destination_node(endpoint))

    def create_em_destination_node(self, endpoint):
        return f"""
  {endpoint}:
    source: $transformations.{endpoint}
    template: {self.template_folder}{endpoint}.jsont
    extension: jsonl
    linearize: True"""
        
    def create_jsont(self, endpoint):
        template_file = f"{self.template_folder}{endpoint}.jsont"
        # check if file exists!
        if os.path.isfile(template_file):
            self.logger.critical(f"The file `{template_file}` already exists in the current directory; to re-create it, please first manually delete it.")
        # write out json template
        self.logger.info(f"creating file `{template_file}`...")
        with open(template_file, 'w+') as file:
            # TODO: implement a function in `lightbeam/api.py` that constructs a "sample" payload for the endpoint
            # Example:
            # {
            #   "property_bool": true,
            #   "property_int": 1,
            #   "property_float": 1.0,
            #   "property_string": "string",
            #   "property_date": "date",
            #   "property_string_optional": "string",
            #   "property_descriptor": "uri://ed-fi.org/SomeDescriptor#SomeValue",
            #   "property_object": {
            #     "property_object_1": "string",
            #     "property_object_2": "string"
            #   },
            #   "property_array": [
            #     {
            #       "property_array_1": "string",
            #       "property_array_2": "string"
            #     }
            #   ]
            # }
            # TODO: turn the "sample" payload into a Jinja template
            # Example:
            # {
            #   "property_bool": {{property_bool}},
            #   "property_int": {{property_int}},
            #   "property_float": {{property_float}},
            #   "property_string": "{{property_string}}",
            #   "property_date": "{{property_date}}",
            #   {% if property_string_optional %}
            #   "property_string_optional": "{{property_string_optional}}",
            #   {% endif %}
            #   "property_descriptor": "uri://ed-fi.org/SomeDescriptor#{{property_descriptor}}",
            #   "property_object": {
            #     "property_object_1": "{{property_object_1}}",
            #     "property_object_2": "{{property_object_2}}"
            #   },
            #   "property_array": [
            #     {% for item in property_array %}
            #     {
            #       "property_array_1": "{{item.property_array_1}}",
            #       "property_array_2": "{{item.property_array_2}}"
            #     } {% if not loop.last %},{% endif %}
            #     {% endfor %}
            #   ]
            # }
            file.write("coming soon...") # (for now)
