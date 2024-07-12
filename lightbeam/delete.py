import os
import json
import copy
import asyncio

from lightbeam import util
from lightbeam import hashlog
from lightbeam.api import EdFiAPI

class Deleter:

    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        self.lightbeam.reset_counters()
        self.logger = self.lightbeam.logger
        self.hashlog_data = {}
    
    # Deletes data matching payloads in config.data_dir for selected endpoints
    def delete(self):
        # prompt to confirm this destructive operation
        if not self.lightbeam.config.get("force_delete", False):
            if input('Type "yes" to confirm you want to delete payloads for the selected endpoints? ')!="yes":
                exit('You did not type "yes" - exiting.')
        
        # load swagger docs, so we can find natural keys for each resource and query the API for existing records to delete
        self.lightbeam.api.load_swagger_docs()
        
        # get token with which to send requests
        EdFiAPI.do_oauth()

        # filter down to selected endpoints that actually have .jsonl in config.data_dir
        endpoints = self.lightbeam.get_endpoints_with_data(self.lightbeam.endpoints)
        if len(endpoints)==0:
            self.logger.critical("`data_dir` {0} has no *.jsonl files".format(self.lightbeam.config["data_dir"]) + " for selected endpoints")
        
        # process endpoints in reverse-dependency order, so we don't get dependency errors
        endpoints = copy.deepcopy(endpoints)
        endpoints.reverse()

        for endpoint in endpoints:
            # it doesn't seem possible to delete students once you've sent them
            # (I think because other entities may have referenced them in the meantime)
            if endpoint=='students':
                self.logger.warn("data for {0} endpoint cannot be deleted (this is an Ed-Fi limitation); skipping".format(endpoint))
                continue

            asyncio.run(self.do_deletes(endpoint))
            self.logger.info("finished processing endpoint {0}!".format(endpoint))
            self.logger.info("  (final status counts: {0})".format(self.lightbeam.status_counts))
            self.lightbeam.log_status_reasons()

    # Deletes data matching payloads in config.data_dir for single endpoint
    async def do_deletes(self, endpoint):
        # load the hashlog, since we delete previously-seen payloads from it after deleting them
        if self.lightbeam.track_state:
            hashlog_file = os.path.join(self.lightbeam.config["state_dir"], f"{endpoint}.dat")
            self.hashlog_data = hashlog.load(hashlog_file)
        
        self.lightbeam.reset_counters()
        
        data_files = self.lightbeam.get_data_files_for_endpoint(endpoint)
        tasks = []

        # determine the fields that uniquely define a record for this endpoint
        params_structure = self.lightbeam.api.get_params_for_endpoint(endpoint)

        # for Descriptors, we need to fetch all Descriptor values first, then we can look up the ID for deletion
        if endpoint.endswith('Descriptors'):
            self.logger.info("fetching current descriptors from endpoint {0} ...".format(endpoint))
            selector_backup = self.lightbeam.selector
            exclude_backup = self.lightbeam.exclude
            drop_keys_backup = self.lightbeam.drop_keys
            self.lightbeam.selector = endpoint
            self.lightbeam.exclude = ""
            self.lightbeam.drop_keys = "_etag,_lastModifiedDate"
            self.lightbeam.endpoints = [endpoint]
            await self.lightbeam.fetcher.get_records(do_write=False, log_status_counts=False)
            self.lightbeam.reset_counters()

        self.logger.info("deleting data from endpoint {0} ...".format(endpoint))
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

                    # check if we've posted this data before
                    data_hash = hashlog.get_hash(data)
                    if self.lightbeam.track_state and data_hash in self.hashlog_data.keys():
                        # check if the last post meets criteria for a delete
                        if self.lightbeam.meets_process_criteria(self.hashlog_data[data_hash]):
                            # yes, we need to delete it; append to task queue
                            tasks.append(asyncio.create_task(
                                self.do_delete(endpoint, file_name, params, counter, data_hash)))
                        else:
                            # no, do not delete
                            self.lightbeam.num_skipped += 1
                            continue
                    else:
                        # new, never-before-seen payload! delete it (maybe this should be a warning instead?)
                        tasks.append(asyncio.create_task(
                            self.do_delete(endpoint, file_name, params, counter)))

                    if counter%self.lightbeam.MAX_TASK_QUEUE_SIZE==0:
                        await self.lightbeam.do_tasks(tasks, counter)
                        tasks = []
                    
            if len(tasks)>0: await self.lightbeam.do_tasks(tasks, counter)

        if endpoint.endswith('Descriptors'):
            self.lightbeam.results = []
            self.lightbeam.selector = selector_backup
            self.lightbeam.exclude = exclude_backup
            self.lightbeam.drop_keys = drop_keys_backup
            self.lightbeam.api.prepare()

        # any task may have updated the hashlog, so we need to re-save it out to disk
        if self.lightbeam.track_state:
            hashlog.save(hashlog_file, self.hashlog_data)

    # Deletes a single payload for a single endpoint
    async def do_delete(self, endpoint, file_name, params, line, data_hash=None):
        curr_token_version = int(str(self.lightbeam.token_version))
        while True: # this is not great practice, but an effective way (along with the `break` below) to achieve a do:while loop
            try:
                if endpoint.endswith('Descriptors'):
                    skip_reason = None
                    matching_descriptors = [x for x in self.lightbeam.results if x['namespace']==params['namespace'] and x['codeValue']==params['codeValue']]
                    if len(matching_descriptors)==1:
                        the_id = matching_descriptors[0]['id']
                        await self.do_delete_id(endpoint, the_id, file_name, line)
                        break
                    elif len(matching_descriptors)>1: skip_reason = "multiple matching payloads found in API"
                    else: skip_reason = "payload not found in API"
                    if skip_reason:
                        self.lightbeam.num_skipped += 1
                        self.lightbeam.increment_status_reason(skip_reason)
                        break # (out of while loop)
                else:

                    # we have to get the `id` for a particular resource by first searching for its natural keys
                    async with self.lightbeam.api.client.get(
                        util.url_join(self.lightbeam.api.config["data_url"], self.lightbeam.config["namespace"], endpoint),
                        params=params,
                        ssl=self.lightbeam.config["connection"]["verify_ssl"],
                        headers=EdFiAPI.headers
                        ) as get_response:
                        body = await get_response.text()
                        status = get_response.status
                        if status!=401:
                            skip_reason = None
                            if status in [200, 201]:
                                j = json.loads(body)
                                if type(j)==list and len(j)==1:
                                    the_id = j[0]['id']
                                    # now we can delete by `id`
                                    await self.do_delete_id(endpoint, the_id, file_name, line, data_hash)
                                    break
                                    
                                elif type(j)==list and len(j)==0: skip_reason = "payload not found in API"
                                elif type(j)==list and len(j)>1: skip_reason = "multiple matching payloads found in API"
                                else: skip_reason = "searching API for payload returned a response that is not a list"
                            
                            else: skip_reason = f"searching API for payload returned a {status} response"
                            
                            if skip_reason:
                                self.lightbeam.num_skipped += 1
                                self.lightbeam.increment_status_reason(skip_reason)
                                break # (out of while loop)
                        else:
                            # this could be broken out to a separate function call,
                            # but not doing so should help keep the critical section small
                            if self.lightbeam.token_version == curr_token_version:
                                self.lightbeam.lock.acquire()
                                EdFiAPI.update_oauth()
                                self.lightbeam.lock.release()
                            else:
                                await asyncio.sleep(1)
                            curr_token_version = int(str(self.lightbeam.token_version))
                    
            except RuntimeError as e:
                await asyncio.sleep(1)
            except Exception as e:
                self.lightbeam.num_errors += 1
                self.logger.exception(e, exc_info=self.lightbeam.config["show_stacktrace"])
                self.logger.error("  (at line {0} of {1}; ID: {2} )".format(line, file_name, id))
                break

    async def do_delete_id(self, endpoint, id, file_name=None, line=None, data_hash=None):
        curr_token_version = int(str(self.lightbeam.token_version))
        while True: # this is not great practice, but an effective way (along with the `break` below) to achieve a do:while loop
            try:
                async with self.lightbeam.api.client.delete(
                    util.url_join(self.lightbeam.api.config["data_url"], self.lightbeam.config["namespace"], endpoint, id),
                    ssl=self.lightbeam.config["connection"]["verify_ssl"],
                    headers=self.lightbeam.api.headers
                    ) as delete_response:
                    body = await delete_response.text()
                    status = delete_response.status
                    if status!=401:
                        self.lightbeam.num_finished += 1
                        self.lightbeam.increment_status_counts(status)
                        if status not in [ 204 ]:
                            message = str(status) + ": " + util.linearize(body)
                            self.lightbeam.increment_status_reason(message)
                            self.lightbeam.num_errors += 1
                        else:
                            if self.lightbeam.track_state and data_hash is not None:
                                # if we're certain delete was successful, remove this
                                # line of data from internal tracking
                                del self.hashlog_data[data_hash]
                        break # (out of while loop)
                    else:
                        # this could be broken out to a separate function call,
                        # but not doing so should help keep the critical section small
                        if self.lightbeam.token_version == curr_token_version:
                            self.lightbeam.lock.acquire()
                            self.lightbeam.api.update_oauth()
                            self.lightbeam.lock.release()
                        else:
                            await asyncio.sleep(1)
                        curr_token_version = int(str(self.lightbeam.token_version))
            except RuntimeError as e:
                await asyncio.sleep(1)
            except Exception as e:
                self.lightbeam.num_errors += 1
                self.logger.exception(e, exc_info=self.lightbeam.config["show_stacktrace"])
                if line and file_name:
                    self.logger.error("  (at line {0} of {1}; ID: {2} )".format(line, file_name, id))
                break
