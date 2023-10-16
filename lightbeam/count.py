import os
import json
import asyncio
from lightbeam import util

class Counter:

    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        self.logger = self.lightbeam.logger
    
    def count(self):
        self.lightbeam.results = []
        asyncio.run(self.get_record_counts())
        # sort results into dependency order:
        sort_keys = self.lightbeam.api.get_sorted_endpoints()
        self.lightbeam.results = sorted(self.lightbeam.results ,key=lambda x:sort_keys.index(x[0]))

        # output to results file
        if self.lightbeam.results_file:
            # create directory if not exists
            os.makedirs(os.path.dirname(self.lightbeam.results_file), exist_ok=True)
            with open(self.lightbeam.results_file, 'w') as fp:
                # write header
                fp.write("Records" + self.lightbeam.config["count"]["separator"] + "Endpoint\n")
                for result in self.lightbeam.results:
                    # write row
                    fp.write(str(result[1]) + self.lightbeam.config["count"]["separator"] + result[0] + "\n")
        # output to console
        else:
            # print header
            print("Records" + self.lightbeam.config["count"]["separator"] + "Endpoint")
            for result in self.lightbeam.results:
                # when printing to the console, only include endpoints with >0 records
                if result[1] > 0:
                    # print row
                    print(str(result[1]) + self.lightbeam.config["count"]["separator"] + result[0])
    
    async def get_record_counts(self):
        self.lightbeam.api.do_oauth()
        self.lightbeam.reset_counters()
        self.logger.debug(f"fetching record counts...")

        tasks = []
        counter = 0
        for endpoint in self.lightbeam.endpoints:
            counter += 1
            tasks.append(asyncio.create_task(self.get_record_count(endpoint)))

        await self.lightbeam.do_tasks(tasks, counter, log_status_counts=False)
    
    async def get_record_count(self, endpoint, params={}):
        try:
            # don't bother with handling 401 token expiry (like `send` and `delete` do)
            # since `count` should finish _very_ quickly - way before expiry
            params.update({ "limit": "0", "totalCount": "true" })
            async with self.lightbeam.api.client.get(
                util.url_join(self.lightbeam.api.config["data_url"], self.lightbeam.config["namespace"], endpoint),
                params=params,
                ssl=self.lightbeam.config["connection"]["verify_ssl"],
                headers=self.lightbeam.api.headers
                ) as response:
                body = await response.text()
                status = str(response.status)
                if status not in ['200', '201']:
                    self.logger.warn(f"Unable to load counts for {endpoint}... {status} API response.")
                else:
                    total_count = int(response.headers.get("Total-Count", "-1"))
                    if total_count < 0:
                        self.logger.warn(f"Unable to load counts for {endpoint}...")
                        self.lightbeam.num_errors += 1
                    else:
                        self.lightbeam.results.append([endpoint, total_count])
                self.lightbeam.num_finished += 1

        except Exception as e:
            self.logger.critical(f"Unable to load counts for {endpoint} from API... terminating. Check API connectivity.")