import os
import json
import yaml
import logging
import asyncio
import dateutil.parser
from datetime import datetime
from yaml.loader import SafeLoader

from lightbeam import util
from lightbeam.api import EdFiAPI
from lightbeam.validate import Validator
from lightbeam.send import Sender
from lightbeam.delete import Deleter


class Lightbeam:

    config_defaults = {
        "data_dir": "./",
        "namespace": "ed-fi",
        "edfi_api": {
            "base_url": "",
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
        "log_level": "INFO",
        "show_stacktrace": False
    }
    MAX_TASK_QUEUE_SIZE = 1000
    STATUS_UPDATE_WAIT = 5 # seconds
    MAX_STATUS_REASONS_TO_DISPLAY = 10
    DATA_FILE_EXTENSIONS = ['json', 'jsonl', 'ndjson']
    
    def __init__(self, config_file, logger=None, selector="*", params="", wipe=False, force=False, older_than="", newer_than="", resend_status_codes="", results_file=""):
        self.config_file = config_file
        self.logger = logger
        self.errors = 0
        self.params = params
        self.force = force
        self.wipe = wipe
        self.older_than=older_than
        self.newer_than=newer_than
        self.resend_status_codes=resend_status_codes
        self.endpoints = []
        self.validator = Validator(self)
        self.sender = Sender(self)
        self.deleter = Deleter(self)
        self.api = EdFiAPI(self)
        self.is_locked = False
        self.results_file = results_file

        # load params and/or env vars for config YAML interpolation
        self.params = json.loads(params) if params else {}
        user_config = self.load_config_file()
        
        self.config = util.merge_dicts(user_config, self.config_defaults)
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

        self.api.prepare(selector)

        # filter down to selected endpoints that actually have .jsonl in config.data_dir
        self.endpoints = self.get_endpoints_with_data(self.endpoints)
        if len(self.endpoints)==0:
            self.logger.critical("`data_dir` {0} has no *.jsonl files".format(self.config["data_dir"]) + (" for selected endpoints" if selector!="*" and selector!="" else ""))
        
        # parse timestamps and/or status codes for state-based filtering
        if self.older_than!='': self.older_than = dateutil.parser.parse(self.older_than).timestamp()
        if self.newer_than!='': self.newer_than = dateutil.parser.parse(self.newer_than).timestamp()
        if self.resend_status_codes!='': self.resend_status_codes = [int(code) for code in self.resend_status_codes.split(",")]

        # create state_dir if it doesn't exist
        if self.track_state and not os.path.isdir(self.config["state_dir"]):
            self.logger.debug("creating state dir {0}".format(self.config["state_dir"]))
            os.mkdir(self.config["state_dir"])
    
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
    def get_endpoints_with_data(self, endpoints):
        self.logger.debug("discovering data...")
        endpoints_with_data = []
        for endpoint in endpoints:
            if self.get_data_files_for_endpoint(endpoint):
                endpoints_with_data.append(endpoint)
        return endpoints_with_data
    
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
    async def do_tasks(self, tasks, counter):
        # we also append a task to the queue that logs a status update every second
        tasks.append(asyncio.ensure_future(self.periodic_update_until_done(counter)))

        # now process the task queue (concurrently)
        await self.gather_with_concurrency(self.config["connection"]["pool_size"], *tasks)

    # Waits for an entire task queue to finish processing
    async def gather_with_concurrency(self, n, *tasks):
        semaphore = asyncio.Semaphore(n)
        async def sem_task(task):
            async with semaphore:
                return await task
        return await asyncio.gather(*(sem_task(task) for task in tasks), return_exceptions=True)

    # Logs a status update every second
    async def periodic_update_until_done(self, counter):
        period_counter = 0
        while self.num_finished + self.num_skipped < counter:
            period_counter += 1
            if self.status_counts and period_counter%self.STATUS_UPDATE_WAIT==0:
                self.logger.info("  (... status counts: {0}) ".format(str(self.status_counts)))
            await asyncio.sleep(1)
    

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
