import os
import json
import copy
import asyncio

from lightbeam import util
from lightbeam import hashlog


class Deleter:

    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        self.logger = self.lightbeam.logger
        self.hashlog_data = {}
    
    # Deletes data matching payloads in config.data_dir for selected endpoints
    def delete(self):
        # prompt to confirm this destructive operation
        if not self.lightbeam.config["force_delete"]:
            if input('Type "yes" to confirm you want to delete payloads for the selected endpoints? ')!="yes":
                exit('You did not type "yes" - exiting.')
        
        # load swagger docs, so we can find natural keys for each resource and query the API for existing records to delete
        self.lightbeam.api.load_swagger_docs()
        
        # get token with which to send requests
        self.lightbeam.api.do_oauth()

        # process endpoints in reverse-dependency order, so we don't get dependency errors
        endpoints = copy.deepcopy(self.lightbeam.endpoints)
        endpoints.reverse()

        for endpoint in endpoints:
            # it doesn't seem possible to delete students once you've sent them
            # (I think because other entities may have referenced them in the meantime)
            if endpoint=='students':
                self.logger.warn("data for {0} endpoint cannot be deleted (this is an Ed-Fi limitation); skipping".format(endpoint))
                continue

            self.logger.info("deleting data from endpoint {0} ...".format(endpoint))
            asyncio.run(self.do_deletes(endpoint))
            self.logger.info("finished deleting data from endpoint {0}!".format(endpoint))
            self.logger.info("  (status counts: {0})".format(str(self.lightbeam.status_counts)))
            self.lightbeam.log_status_reasons()

    # Deletes data matching payloads in config.data_dir for single endpoint
    async def do_deletes(self, endpoint):
        # load the hashlog, since we delete previously-seen payloads from it after deleting them
        hashlog_file = os.path.join(self.lightbeam.config["state_dir"], f"{endpoint}.dat")
        self.hashlog_data = hashlog.load(hashlog_file)
        
        self.lightbeam.reset_counters()
        # here we set up a smart retry client with exponential backoff and a connection pool
        async with util.get_retry_client(self.lightbeam.config['connection'], self.lightbeam.api.token) as client:
            data_files = self.lightbeam.get_data_files_for_endpoint(endpoint)
            tasks = []

            # determine the fields that uniquely define a record for this endpoint
            params_structure = self.lightbeam.api.get_params_for_endpoint(endpoint)

            # process each file
            counter = 0
            for file_name in data_files:
                with open(file_name) as file:
                    # process each payload
                    for line in file:
                        counter += 1
                        data = line.strip()
                        # fill out the required fields from the data payload
                        # (so we can search for matching records in the API)
                        params = util.interpolate_params(params_structure, data)

                        hash = hashlog.get_hash(data)
                        if hash in self.hashlog_data.keys():
                            # remove the payload from the hashlog
                            del self.hashlog_data[hash]
                        
                        # append a delete task to the queue
                        tasks.append(asyncio.ensure_future(
                            self.do_delete(endpoint, file_name, params, client, counter)))

                        if counter%self.lightbeam.MAX_TASK_QUEUE_SIZE==0:
                            await self.lightbeam.do_tasks(tasks, counter)
                            tasks = []
                        
                await self.lightbeam.do_tasks(tasks, counter)

            # any task may have updated the hashlog, so we need to re-save it out to disk
            hashlog.save(hashlog_file, self.hashlog_data)

    # Deletes a single payload for a single endpoint
    async def do_delete(self, endpoint, file_name, params, client, line):
        try:
            # we have to get the `id` for a particular resource by first searching for its natural keys
            async with client.get(self.lightbeam.api.config["data_url"] + endpoint, params=params,
                                    ssl=self.lightbeam.config["connection"]["verify_ssl"]) as response:
                body = await response.text()
                status = str(response.status)
                skip_reason = ""
                if status in ['200', '201']:
                    j = json.loads(body)
                    if type(j)==list and len(j)==1:
                        the_id = j[0]['id']
                        # now we can delete by `id`
                        async with client.delete(self.lightbeam.api.config["data_url"] + endpoint + '/' + the_id,
                                                    ssl=self.lightbeam.config["connection"]["verify_ssl"]) as response:
                            body = await response.text()
                            status = str(response.status)
                            self.lightbeam.num_finished += 1
                            self.lightbeam.increment_status_counts(status)
                            if response.status not in [ 204 ]:
                                message = str(response.status) + ": " + util.linearize(body)
                                self.lightbeam.increment_status_reason(message)
                                self.lightbeam.num_errors += 1
                    elif type(j)==list and len(j)==0:
                        skip_reason = "payload not found in API"
                    elif type(j)==list and len(j)>1:
                        skip_reason = "multiple matching payloads found in API"
                    else:
                        skip_reason = "searching API for payload returned a response that is not a list"
                else:
                    skip_reason = f"searching API for payload returned a {status} response"
                if skip_reason != "":
                    self.lightbeam.num_skipped += 1
                    self.lightbeam.increment_status_reason(skip_reason)
                    
        except Exception as e:
            self.num_errors += 1
            self.logger.exception(e, exc_info=self.config["show_stacktrace"])
            self.logger.error("  (at line {0} of {1}; ID: {2} )".format(line, file_name, id))

    