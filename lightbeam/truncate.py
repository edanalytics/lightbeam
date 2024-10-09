import os
import json
import copy
import asyncio

from lightbeam import util
from lightbeam import hashlog


class Truncator:

    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        self.lightbeam.reset_counters()
        self.logger = self.lightbeam.logger
        self.hashlog_data = {}
    
    # Deletes all data in the Ed-Fi API for selected endpoints
    def truncate(self):
        # get token with which to send requests
        self.lightbeam.api.do_oauth()

        # process endpoints in reverse-dependency order, so we don't get dependency errors
        endpoints = self.lightbeam.endpoints
        endpoints.reverse()

        # prompt to confirm this destructive operation
        self.lightbeam.confirm_truncate(endpoints)

        for endpoint in endpoints:
            asyncio.run(self.do_truncates(endpoint))
            self.logger.info("finished processing endpoint {0}!".format(endpoint))
            self.logger.info("  (final status counts: {0})".format(self.lightbeam.status_counts))
            self.lightbeam.log_status_reasons()

    # Deletes data matching payloads in config.data_dir for single endpoint
    async def do_truncates(self, endpoint):
        # load the hashlog, since we delete previously-seen payloads from it after deleting them
        if self.lightbeam.track_state:
            hashlog_file = os.path.join(self.lightbeam.config["state_dir"], f"{endpoint}.dat")
            self.hashlog_data = hashlog.load(hashlog_file)
        
        
        selector_backup = self.lightbeam.selector
        exclude_backup = self.lightbeam.exclude
        track_state_backup = self.lightbeam.track_state
        self.lightbeam.selector = endpoint
        self.lightbeam.keep_keys = "id"
        self.lightbeam.track_state = False
        all_endpoints = self.lightbeam.api.get_sorted_endpoints()
        self.lightbeam.endpoints = self.lightbeam.api.apply_filters(all_endpoints)

        # this fetches the IDs of all payloads in the Ed-Fi API into self.lightbeam.results:
        self.lightbeam.results = []
        await self.lightbeam.fetcher.get_records(do_write=False, log_status_counts=False)

        self.logger.info("TRUNCATING ALL DATA from endpoint {0} ...".format(endpoint))
        tasks = []
        counter = 0
        self.lightbeam.reset_counters()
        # loop over IDs and delete each one:
        for result in self.lightbeam.results:
            counter += 1
            id = result["id"]
            tasks.append(asyncio.create_task(self.lightbeam.deleter.do_delete_id(endpoint, id)))
            # process task queue occasionally before if gets too big:
            if counter%self.lightbeam.MAX_TASK_QUEUE_SIZE==0:
                await self.lightbeam.do_tasks(tasks, counter)
                tasks = []

        # finish up any uncompleted tasks:
        if len(tasks)>0: await self.lightbeam.do_tasks(tasks, counter)

        # clear out the hashlog file, since those payloads aren't in Ed-Fi anymore
        if track_state_backup:
            self.hashlog_data = {}
            hashlog.save(hashlog_file, self.hashlog_data)

        self.lightbeam.results = []
        self.lightbeam.selector = selector_backup
        self.lightbeam.exclude = exclude_backup
        self.lightbeam.track_state = track_state_backup
        self.lightbeam.keep_keys = ""
        all_endpoints = self.lightbeam.api.get_sorted_endpoints()
        self.lightbeam.endpoints = self.lightbeam.api.apply_filters(all_endpoints)