import os
import re
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
        self.lightbeam.api.load_swagger_docs()
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

config:
  macros: >
    {% macro descriptor_namespace() -%}
      uri://ed-fi.org
    {%- endmacro %}

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

    def upper_repl(self, match):
        value = match.group(1)
        return "/" + value[0].upper() + value[1:] + "Descriptor#"
        
        
    def create_jsont(self, endpoint):
        template_file = f"{self.template_folder}{endpoint}.jsont"
        # check if file exists!
        if os.path.isfile(template_file):
            self.logger.critical(f"The file `{template_file}` already exists in the current directory; to re-create it, please first manually delete it.")
        # generate base JSON structure:
        content = self.lightbeam.api.get_params_for_endpoint(endpoint, type='all')
        # pretty-print it:
        content = json.dumps(content, indent=2)
        # annotate required/optional properties:
        content = content.replace('"[required]', '{# (required) #} "')
        content = content.replace('"[optional]', '{# (optional) #} "')
        # appropriate quoting based on property data type:
        content = re.sub('"\[string\](.*)Descriptor"', r'"{{descriptor_namespace()}}/\1Descriptor#{{\1Descriptor}}"', content)
        content = re.sub('/(.*)_(.*)Descriptor#', r'/\2Descriptor#', content)
        content = re.sub(r'/(.*)Descriptor#', self.upper_repl, content)
        content = re.sub('"\[string\](.*)"', r'"{{\1}}"', content)
        content = re.sub('"\[(integer|boolean)\](.*)"', r'{{\2}}', content)
        # for loops over arrays:
        content = re.sub('"(.*)": \[', r'"\1": [ {% for item in \1 %}', content)
        content = re.sub('\]', r'{% endfor %} ]', content)
        content = re.sub('{{(.*)-(.*)}}', r'{{item.\2}}', content)
        # add info header message:
        content = """{#
  This is an earthmover JSON template file, generated with `lightbeam create`, for creating Ed-Fi JSON `"""+endpoint+"""`
  payloads using earthmover. See https://github.com/edanalytics/earthmover for documentation.
#}
""" + content
        # write out json template
        self.logger.info(f"creating file `{template_file}`...")
        with open(template_file, 'w+') as file:
            file.write(content)
