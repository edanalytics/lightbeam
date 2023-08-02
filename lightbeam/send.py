import os
import time
import json
import asyncio
import datetime

from lightbeam import util
from lightbeam import hashlog
from lightbeam.rest_event import RESTEvent


class Sender:

    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        RESTEvent.reset_counters()
        self.logger = self.lightbeam.logger
        self.hashlog_data = {}
        self.start_timestamp = datetime.datetime.now()

    # Sends all (selected) endpoints
    def send(self):

        # get token with which to send requests
        self.lightbeam.api.do_oauth()

        # send each endpoint
        for endpoint in self.lightbeam.endpoints:
            self.logger.info("sending endpoint {0} ...".format(endpoint))
            asyncio.run(self.do_send(endpoint))
            self.logger.info("finished processing endpoint {0}!".format(endpoint))
            self.logger.info("  (final status counts: {0}) ".format(RESTEvent.endpoint_counts[endpoint])) ## TODO
            self.lightbeam.log_status_reasons()

        ### Create structured output results_file if necessary
        if self.lightbeam.results_file:

            end_timestamp = datetime.datetime.now()

            full_payload = {
                "started_at": self.start_timestamp.isoformat(timespec='microseconds'),
                "completed_at": end_timestamp.isoformat(timespec='microseconds'),
                "runtime_sec": (end_timestamp - self.start_timestamp).total_seconds(),

                "config_file": self.lightbeam.config_file,
                "working_dir": os.getcwd(),
                "data_dir": self.lightbeam.config["data_dir"],
                "api_url": self.lightbeam.config["edfi_api"]["base_url"],
                "namespace": self.lightbeam.config["namespace"],

                "endpoints": RESTEvent.to_json(),
                "total_records_processed": RESTEvent.total_processed,
                "total_records_skipped": RESTEvent.total_skipped,
                "total_records_failed": RESTEvent.total_failed,
            }

            # create directory if not exists
            os.makedirs(os.path.dirname(self.lightbeam.results_file), exist_ok=True)

            with open(self.lightbeam.results_file, 'w') as fp:
                fp.write(json.dumps(full_payload, indent=4))

        if RESTEvent.total_processed == RESTEvent.total_skipped:
            self.logger.info("all payloads skipped")
            exit(99) # signal to downstream tasks (in Airflow) all payloads skipped

        if RESTEvent.total_processed == RESTEvent.total_failed:
            self.logger.info("all payloads failed")
            exit(1) # signal to downstream tasks (in Airflow) all payloads failed


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
            tasks = []

            # process each file
            for file_name in self.lightbeam.get_data_files_for_endpoint(endpoint):

                with open(file_name) as fp:
                    # process each line
                    for line, data in enumerate(fp):

                        event = RESTEvent(
                            endpoint=endpoint,
                            namespace=self.lightbeam.config["namespace"],
                            file=file_name,
                            line=line,
                            data=data,
                        )

                        # check if we've posted this data before and whether the last post meets criteria for a resend
                        event_hash = self.hashlog_data.get(event.data_hash)
                        if self.lightbeam.track_state and not self.lightbeam.meets_process_criteria(event_hash):
                            event.increment_num_skipped()
                            continue

                        # if so, append it to task queue
                        tasks.append(
                            asyncio.ensure_future(event.post(client, lightbeam=self.lightbeam))
                        )

                        if RESTEvent.total_seen % self.lightbeam.MAX_TASK_QUEUE_SIZE == 0:
                            await self.lightbeam.do_tasks(tasks, RESTEvent.total_seen)
                            tasks = []

                    if RESTEvent.total_skipped > 0:  # TODO
                        self.logger.info(
                            "skipped {0} of {1} payloads because they were previously processed and did not match any resend criteria".format(self.lightbeam.num_skipped, total_counter)
                        )

                await self.lightbeam.do_tasks(tasks, RESTEvent.total_seen)
            
            # any task may have updated the hashlog, so we need to re-save it out to disk
            if self.lightbeam.track_state:
                hashlog.save(hashlog_file, self.hashlog_data)
