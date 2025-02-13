import os
import re
import json
import yaml
import logging
import asyncio
import dateutil.parser
from datetime import datetime
from yaml.loader import SafeLoader

from lightbeam import util
from lightbeam.api import EdFiAPI
from lightbeam.count import Counter
from lightbeam.fetch import Fetcher
from lightbeam.validate import Validator
from lightbeam.send import Sender
from lightbeam.delete import Deleter
from lightbeam.truncate import Truncator


class Lightbeam:

    config_defaults = {
        "data_dir": "./",
        "namespace": "ed-fi",
        "edfi_api": {
            "base_url": "",
            "oauth_url": "", 
            "dependencies_url": "",
            "descriptors_swagger_url": "",
            "resources_swagger_url": "",
            "version": 3,
            "mode": "year_specific",
            "year": datetime.today().year,
            "client_id": "",
            "client_secret": ""
        },
        "connection": {
            "pool_size": 8,
            "timeout": 60,
            "num_retries": 10,
            "backoff_factor": 1.5,
            "retry_statuses": [429, 500, 501, 503, 504],
        },
        "count": {
            "separator": "\t"
        },
        "fetch": {
            "page_size": 100
        },
        "log_level": "INFO",
        "show_stacktrace": False
    }
    MAX_TASK_QUEUE_SIZE = 2000
    MAX_STATUS_REASONS_TO_DISPLAY = 10
    DATA_FILE_EXTENSIONS = ['json', 'jsonl', 'ndjson']

    EDFI_GENERICS_TO_RESOURCES_MAPPING = {
        "educationOrganizations": ["schools", "localEducationAgencies", "stateEducationAgencies"],
    }
    EDFI_GENERIC_REFS_TO_PROPERTIES_MAPPING = {
        "educationOrganizationId": {
            "localEducationAgencies": "localEducationAgencyId",
            "stateEducationAgencies": "stateEducationAgencyId",
            "schools": "schoolId",
        },
    }
    
    def __init__(self, config_file, logger=None, selector="*", exclude="", keep_keys="*", drop_keys="", query="{}", params="", wipe=False, force=False, older_than="", newer_than="", resend_status_codes="", results_file="", overrides={}):
        self.config_file = config_file
        self.logger = logger
        self.errors = 0
        self.params = params
        self.force = force
        self.selector = selector
        self.exclude = exclude
        self.keep_keys = keep_keys
        self.drop_keys = drop_keys
        self.query = query
        self.wipe = wipe
        self.older_than=older_than
        self.newer_than=newer_than
        self.resend_status_codes=resend_status_codes
        self.endpoints = []
        self.results = []
        self.counter = Counter(self)
        self.fetcher = Fetcher(self)
        self.validator = Validator(self)
        self.sender = Sender(self)
        self.deleter = Deleter(self)
        self.truncator = Truncator(self)
        self.api = EdFiAPI(self)
        self.token_version = 0        
        self.results_file = os.path.abspath(results_file) if results_file else None
        self.start_timestamp = datetime.now()
        self.overrides = overrides

        # load params and/or env vars for config YAML interpolation
        self.params = json.loads(params) if params else {}
        user_config = self.load_config_file()
        self.config = util.merge_dicts(user_config, self.config_defaults)
        
        # inject overrides into config
        if self.overrides: self.inject_cli_overrides()

        if "state_dir" in self.config:
            self.track_state = True
            self.config["state_dir"] = os.path.expanduser(self.config["state_dir"])
        else:
            self.track_state = False
            self.logger.warning("`state_dir` not specified in config; continuing without state-tracking")
        self.config["data_dir"] = os.path.expanduser(self.config["data_dir"])

        # configure log level
        self.logger.setLevel(logging.getLevelName(self.config["log_level"].upper()))

        # check data_dir exists
        if not os.path.isdir(self.config["data_dir"]):
            self.logger.critical("`data_dir` {0} is not a directory".format(self.config["data_dir"]))
        
        # turn off annoying SSL warnings (is this necessary? is this dangerous?)
        logging.captureWarnings(True)

        self.api.prepare()

        # parse timestamps and/or status codes for state-based filtering
        if self.older_than!='': self.older_than = dateutil.parser.parse(self.older_than).timestamp()
        if self.newer_than!='': self.newer_than = dateutil.parser.parse(self.newer_than).timestamp()
        if self.resend_status_codes!='': self.resend_status_codes = [int(code) for code in self.resend_status_codes.split(",")]

        # create state_dir if it doesn't exist
        if self.track_state and not os.path.isdir(self.config["state_dir"]):
            self.logger.debug("creating state dir {0}".format(self.config["state_dir"]))
            os.mkdir(self.config["state_dir"])

        # Initialize a dictionary for tracking run metadata (for structured output)
        self.metadata = {
            "started_at": self.start_timestamp.isoformat(timespec='microseconds'),
            "working_dir": os.getcwd(),
            "config_file": self.config_file,
            "data_dir": self.config["data_dir"],
            "api_url": self.config["edfi_api"]["base_url"],
            "namespace": self.config["namespace"],
            "resources": {}
        }
    
    def inject_cli_overrides(self):
        # parse self.overrides into configs:
        for key, value in self.overrides.items():
            self.config = Lightbeam.set_path(self.config, key, value)

    @staticmethod
    def set_path(my_dict, path, value):
        path_pieces = path.split(".")
        current = my_dict
        for path_piece in path_pieces[:-1]:
            if path_piece not in current.keys():
                current[path_piece] = {}
            current = current[path_piece]
        current[path_pieces[-1]] = Lightbeam.autocast(value)
        return my_dict
    
    @staticmethod
    def autocast(value):
        if value.lower() in ['true', 'yes', 'on', 't', 'y']:
            return True
        elif value.lower() in ['false', 'no', 'off', 'f', 'n']:
            return False
        elif '.' in value:
            try:
                return float(value)
            except ValueError:
                return value
        else:
            try:
                return int(value)
            except ValueError:
                return value
    
    # this is intended to be called before any CRITICAL errors;
    # any cleanup tasks should go here:
    def shutdown(self, method):
        self.write_structured_output(method)

    # helper function used below
    def replace_linebreaks(self, m):
        return re.sub(r"\s+", '', m.group(0))

    def write_structured_output(self, command):
        ### Create structured output results_file if necessary
        self.end_timestamp = datetime.now()
        self.metadata.update({
            "command": command,
            "completed_at": self.end_timestamp.isoformat(timespec='microseconds'),
            "runtime_sec": (self.end_timestamp - self.start_timestamp).total_seconds(),
            "total_records_processed": sum(item['records_processed'] for item in self.metadata["resources"].values()),
            "total_records_skipped": sum(item['records_skipped'] for item in self.metadata["resources"].values()),
            "total_records_failed": sum(item['records_failed'] for item in self.metadata["resources"].values())
        })
        # sort failing line numbers
        for resource in self.metadata["resources"].keys():
            if "failures" in self.metadata["resources"][resource].keys():
                for idx, _ in enumerate(self.metadata["resources"][resource]["failures"]):
                    self.metadata["resources"][resource]["failures"][idx]["line_numbers"].sort()

        ### Create structured output results_file if necessary
        if self.results_file:

            # create directory if not exists
            os.makedirs(os.path.dirname(self.results_file), exist_ok=True)

            with open(self.results_file, 'w') as fp:
                content = json.dumps(self.metadata, indent=4)
                # failures.line_numbers are split each on their own line; here we remove those line breaks
                content = re.sub(r'"line_numbers": \[(\d|,|\s|\n)*\]', self.replace_linebreaks, content)
                fp.write(content)
    
            self.logger.info(f"results written to {self.results_file}")
        
    
    def load_config_file(self) -> dict:
        _env_backup = os.environ.copy()

        # Load & parse config YAML (using modified environment vars)
        os.environ.update(self.params)

        with open(self.config_file, "r") as stream:
            try:
                configs = yaml.load(stream, Loader=SafeLineEnvVarLoader)
            except yaml.YAMLError as err:
                raise Exception(self.error_handler.ctx + f"YAML could not be parsed: {err}")
            if not isinstance(configs, dict):
                self.logger.critical("YAML does not seem to be a dictionary. See documentation for expected structure.")

        # Return environment to original backup
        os.environ = _env_backup

        return configs
    
    def meets_process_criteria(self, tuple):
        return ( self.force
                    or (self.older_than and tuple[0]<self.older_than)
                    or (self.newer_than and tuple[0]>self.newer_than)
                    or (len(self.resend_status_codes)>0 and tuple[1] in self.resend_status_codes)
                )
    
    def _confirm_delete_op(self, endpoints, verbiage):
        if self.config.get("force_delete", False):
            return

        endpoint_list = "\n\t - ".join(endpoints)
        print(f'Preparing to delete the following endpoints:\n\t • {endpoint_list}')
        if input(f'Type "yes" to confirm you want to {verbiage} payloads for the selected endpoints? ')!="yes":
            exit('You did not type "yes" - exiting.')

    def confirm_delete(self, endpoints):
        self._confirm_delete_op(endpoints, "delete")

    def confirm_truncate(self, endpoints):
        self._confirm_delete_op(endpoints, "TRUNCATE ALL DATA")

    ################### Data discovery and loading methods ####################
    
    # For the specified endpoint, returns a list of all files in config.data_dir which end in .jsonl
    def get_data_files_for_endpoint(self, endpoint):
        file_list = []
        for ext in self.DATA_FILE_EXTENSIONS:
            possible_file = os.path.join(self.config["data_dir"], endpoint + "." + ext)
            if os.path.isfile(possible_file):
                file_list.append(possible_file)
            possible_dir = os.path.join(self.config["data_dir"] + endpoint)
            if os.path.isdir(possible_dir):
                for file in os.listdir(possible_dir):
                    if file.endswith("." + ext):
                        file_list.append(os.path.join(self.config["data_dir"], endpoint, file))
        return file_list

    # Prunes the list of endpoints down to those for which .jsonl files exist in the config.data_dir
    def get_endpoints_with_data(self, filter_endpoints=None):
        if not filter_endpoints:
            filter_endpoints = self.all_endpoints
        self.logger.debug("discovering data...")
        endpoints_with_data = []
        data_dir_list = os.listdir(self.config["data_dir"])
        for data_dir_item in data_dir_list:
            data_dir_item_path = os.path.join(self.config["data_dir"], data_dir_item)
            if os.path.isfile(data_dir_item_path):
                filename = os.path.basename(data_dir_item)
                extension = filename.rsplit(".", 1)[-1]
                filename_without_extension = filename.rsplit(".", 1)[0]
                if (
                    extension in self.DATA_FILE_EXTENSIONS # valid file extension
                    and filename_without_extension in self.all_endpoints # valid endpoint
                    and filename_without_extension in filter_endpoints # selected endpoint
                ):
                    endpoints_with_data.append(filename_without_extension)
            elif os.path.isdir(data_dir_item_path):
                if data_dir_item in self.all_endpoints:
                    has_data_file = False
                    sub_dir_list = os.listdir(data_dir_item_path)
                    for sub_dir_item in sub_dir_list:
                        sub_dir_item_path = os.path.join(data_dir_item_path, sub_dir_item)
                        if os.path.isfile(sub_dir_item_path):
                            filename = os.path.basename(sub_dir_item)
                            extension = filename.rsplit(".", 1)[-1]
                            filename_without_extension = filename.rsplit(".", 1)[0]
                            if (
                                extension in self.DATA_FILE_EXTENSIONS # valid file extension
                                and data_dir_item in self.all_endpoints # valid endpoint
                                and data_dir_item in filter_endpoints # selected endpoint
                            ):
                                has_data_file = True
                                break
                    if has_data_file:
                        endpoints_with_data.append(data_dir_item)
                        
        # now we have the endpoints with data, but they're in whatever order
        # os.listdir() gave (usually alphabetical)... so we must re-order them
        # back to the `filter_endpoints` order:
        final_endpoints_with_data = []
        for endpoint in filter_endpoints:
            if endpoint in endpoints_with_data:
                final_endpoints_with_data.append(endpoint)
        return final_endpoints_with_data
    
    # Returns a generator which produces json lines for a given endpoint based on relevant files in config.data_dir
    # def get_jsonl_for_endpoint(self, endpoint):
    #     file_list = self.get_data_files_for_endpoint(endpoint)
    #     for f in file_list:
    #         with open(f) as fd:
    #             for line in fd:
    #                 yield line.strip()
    # (not used, because processes want to be able to report which file and line number errors happen at...)
                

    ###################### Async task-processing methods ######################

    # Waits for an entire queue of `counter` `tasks` to complete (asynchronously)
    async def do_tasks(self, tasks, counter, log_status_counts=True):
        async with self.api.get_retry_client() as client:
            self.api.client = client
            self.lock = asyncio.Lock()
            await asyncio.wait(tasks)
        if log_status_counts:
            self.logger.info("  (... status counts: {0}) ".format(str(self.status_counts)))
 

    ################ Status counting and error logging methods ################

    def reset_counters(self):
        self.num_finished = 0
        self.num_skipped = 0
        self.num_errors = 0
        self.status_counts = {}
        self.status_reasons = {}

    def increment_status_counts(self, status):
        if status not in self.status_counts:
            self.status_counts[status] = 1
        else:
            self.status_counts[status] += 1
    
    def increment_status_reason(self, reason):
        if reason not in self.status_reasons:
            self.status_reasons[reason] = 1
        else:
            self.status_reasons[reason] += 1
    
    def log_status_reasons(self):
        if self.status_reasons:
            counter = 0
            for k,v in self.status_reasons.items():
                counter += 1
                if counter>self.MAX_STATUS_REASONS_TO_DISPLAY: break
                self.logger.info("  (reason: [{0}]; instances: {1})".format(k, str(v)))
            if len(self.status_reasons.keys())>self.MAX_STATUS_REASONS_TO_DISPLAY:
                num_others = str(len(self.status_reasons.keys())-self.MAX_STATUS_REASONS_TO_DISPLAY)
                self.logger.info(f"  (... and {num_others} others)")


# This allows us to determine the YAML file line number for any element loaded from YAML
# (very useful for debugging and giving meaningful error messages)
# (derived from https://stackoverflow.com/a/53647080)
# Also added env var interpolation based on
# https://stackoverflow.com/questions/52412297/how-to-replace-environment-variable-value-in-yaml-file-to-be-parsed-using-python#answer-55301129
class SafeLineEnvVarLoader(SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = super(SafeLineEnvVarLoader, self).construct_mapping(node, deep=deep)

        # expand env vars:
        for k, v in mapping.items():
            if isinstance(v, str):
                mapping[k] = os.path.expandvars(v)

        # Add 1 so line numbering starts at 1
        mapping['__line__'] = node.start_mark.line + 1
        return mapping
