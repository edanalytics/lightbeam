import os
import math
import json
import asyncio
from lightbeam import util

class Fetcher:

    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        self.logger = self.lightbeam.logger
    
    def fetch(self):
        self.lightbeam.results = []
        asyncio.run(self.get_records())
    
    async def get_records(self):
        self.lightbeam.api.do_oauth()
        self.lightbeam.reset_counters()
        self.logger.debug(f"fetching records...")

        tasks = []
        counter = 0
        limit = 500 # self.DESCRIPTORS_PAGE_SIZE
        for endpoint in self.lightbeam.endpoints:
            # figure out how many (paginated) requests we must make
            tasks.append(asyncio.create_task(self.lightbeam.counter.get_record_count(endpoint)))
            await self.lightbeam.do_tasks(tasks, counter)
            num_records = self.lightbeam.results[0][1]
            num_pages = math.ceil(num_records / limit)
            tasks = []
            self.lightbeam.results = []

            # do the requests
            with open(os.path.join(self.lightbeam.config["data_dir"], endpoint + ".jsonl"), "w") as file_handle:
                for p in range(0, num_pages):
                    counter += 1
                    tasks.append(asyncio.create_task(self.get_endpoint_records(endpoint, file_handle, limit, p*limit)))

                await self.lightbeam.do_tasks(tasks, counter)
    
    # Fetches valid descriptor values for a specific descriptor endpoint
    async def get_endpoint_records(self, endpoint, file_handle, limit, offset):
        curr_token_version = int(str(self.lightbeam.token_version))
        while True: # this is not great practice, but an effective way (along with the `break` below) to achieve a do:while loop
            try:
                async with self.lightbeam.api.client.get(
                    util.url_join(self.lightbeam.api.config["data_url"], self.lightbeam.config["namespace"], endpoint+"?limit="+str(limit)+"&offset="+str(offset)),
                    ssl=self.lightbeam.config["connection"]["verify_ssl"],
                    headers=self.lightbeam.api.headers
                    ) as response:
                    body = await response.text()
                    status = str(response.status)
                    if status=='401':
                        # this could be broken out to a separate function call,
                        # but not doing so should help keep the critical section small
                        if self.lightbeam.token_version == curr_token_version:
                            self.lightbeam.lock.acquire()
                            self.lightbeam.api.update_oauth()
                            self.lightbeam.lock.release()
                        else:
                            await asyncio.sleep(1)
                        curr_token_version = int(str(self.lightbeam.token_version))
                    elif status not in ['200', '201']:
                        self.logger.warn(f"Unable to load records for {endpoint}... {status} API response.")
                    else:
                        if response.content_type == "application/json":
                            values = json.loads(body)
                            if type(values) != list:
                                self.logger.warn(f"Unable to load records for {endpoint}... API JSON response was not a list of records.")
                            else:
                                for v in values:
                                    file_handle.write(json.dumps(v)+"\n")
                                    self.lightbeam.increment_status_counts(status)
                                break
                        else:
                            self.logger.warn(f"Unable to load records for {endpoint}... API response was not JSON.")

            except RuntimeError as e:
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.critical(f"Unable to load records for {endpoint} from API... terminating. Check API connectivity.")
        
        self.lightbeam.num_finished += 1