import os
import time
import asyncio

from lightbeam import util
from lightbeam import hashlog


class Sender:

    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        self.lightbeam.reset_counters()
        self.logger = self.lightbeam.logger
        self.hashlog_data = {}
    
    # Sends all (selected) endpoints
    def send(self):
        # get token with which to send requests
        self.lightbeam.api.do_oauth()

        # send each endpoint
        for endpoint in self.lightbeam.endpoints:
            self.logger.info("sending endpoint {0} ...".format(endpoint))
            asyncio.run(self.do_send(endpoint))
            self.logger.info("finished processing endpoint {0}!".format(endpoint))
            self.logger.info("  (final status counts: {0}) ".format(self.lightbeam.status_counts))
            self.lightbeam.log_status_reasons()

    # Sends a single endpoint
    async def do_send(self, endpoint):
        # We try to  avoid re-POSTing JSON we've already (successfully) sent.
        # This is done by storing a few things in a file we call a hashlog:
        # - the hash of the JSON (so we can recognize it in the future)
        # - the timestamp of the last send of this JSON
        # - the returned status code for the last send
        # Using these hashlogs, we can do things like retry JSON that previously
        # failed, resend JSON older than a certain age, etc.
        if self.lightbeam.track_state:
            hashlog_file = os.path.join(self.lightbeam.config["state_dir"], f"{endpoint}.dat")
            self.hashlog_data = hashlog.load(hashlog_file)
        
        self.lightbeam.reset_counters()
        # here we set up a smart retry client with exponential backoff and a connection pool
        async with self.lightbeam.api.get_retry_client() as client:
            # process each file
            data_files = self.lightbeam.get_data_files_for_endpoint(endpoint)
            tasks = []
            total_counter = 0
            for file_name in data_files:
                with open(file_name) as file:
                    # process each line
                    for line in file:
                        total_counter += 1
                        data = line.strip()
                        # compute hash of current row
                        hash = hashlog.get_hash(data)
                        # check if we've posted this data before
                        if self.lightbeam.track_state and hash in self.hashlog_data.keys():
                            # check if the last post meets criteria for a resend
                            if self.lightbeam.meets_process_criteria(self.hashlog_data[hash]):
                                # yes, we need to (re)post it; append to task queue
                                tasks.append(asyncio.ensure_future(
                                    self.do_post(endpoint, file_name, data, client, total_counter, hash)))
                            else:
                                # no, do not (re)post
                                self.lightbeam.num_skipped += 1
                                continue
                        else:
                            # new, never-before-seen payload! append it to task queue
                            tasks.append(asyncio.ensure_future(
                                self.do_post(endpoint, file_name, data, client, total_counter, hash)))
                    
                        if total_counter%self.lightbeam.MAX_TASK_QUEUE_SIZE==0:
                            await self.lightbeam.do_tasks(tasks, total_counter)
                            tasks = []
                        
                    if self.lightbeam.num_skipped>0:
                        self.logger.info("skipped {0} of {1} payloads because they were previously processed and did not match any resend criteria".format(self.lightbeam.num_skipped, total_counter))
                        
                await self.lightbeam.do_tasks(tasks, total_counter)
            
            # any task may have updated the hashlog, so we need to re-save it out to disk
            if self.lightbeam.track_state:
                hashlog.save(hashlog_file, self.hashlog_data)
        
    
    # Posts a single data payload to a single endpoint using the client
    async def do_post(self, endpoint, file_name, data, client, line, hash):
        status = 401
        while status==401:
            
            # wait if another process has locked lightbeam while we refresh the oauth token:
            while self.lightbeam.is_locked:
                await asyncio.sleep(1)
            
            try:
                async with client.post(util.url_join(self.lightbeam.api.config["data_url"], self.lightbeam.config["namespace"], endpoint),
                                        data=data,
                                        ssl=self.lightbeam.config["connection"]["verify_ssl"],
                                        headers=self.lightbeam.api.headers) as response:
                    body = await response.text()
                    status = response.status
                    if status!=401:
                        # update status_counts (for every-second status update)
                        self.lightbeam.increment_status_counts(status)
                        self.lightbeam.num_finished += 1
                        
                        # warn about errors
                        if response.status not in [ 200, 201 ]:
                            message = str(response.status) + ": " + util.linearize(body)
                            self.lightbeam.increment_status_reason(message)
                            self.lightbeam.num_errors += 1
                            if response.status==400:
                                raise Exception(message)

                        
                        # update hashlog
                        if self.lightbeam.track_state:
                            self.hashlog_data[hash] = (round(time.time()), response.status)

                    else:
                        self.lightbeam.api.update_oauth()
        
            except Exception as e:
                status = 400
                self.lightbeam.num_errors += 1
                self.logger.warn("{0}  (at line {1} of {2} )".format(str(e), line, file_name))

