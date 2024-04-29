import os
import math
import json
import asyncio
from urllib.parse import urlencode
from lightbeam import util


class Fetcher:

    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        self.logger = self.lightbeam.logger

    def fetch(self):
        self.lightbeam.results = []
        asyncio.run(self.get_records())

    async def get_records(self, do_write=True, log_status_counts=True):
        self.lightbeam.api.do_oauth()
        self.lightbeam.reset_counters()
        self.logger.debug(f"fetching records...")

        tasks = []
        counter = 0
        limit = self.lightbeam.config["fetch"]["page_size"]
        params = json.loads(self.lightbeam.query)
        for endpoint in self.lightbeam.endpoints:
            # figure out how many (paginated) requests we must make
            tasks.append(
                asyncio.create_task(
                    self.lightbeam.counter.get_record_count(endpoint, params)
                )
            )
        await self.lightbeam.do_tasks(
            tasks, counter, log_status_counts=log_status_counts
        )

        tasks = []
        record_counts = self.lightbeam.results
        self.lightbeam.results = []
        for endpoint in self.lightbeam.endpoints:
            try:
                num_records = [x for x in record_counts if x[0] == endpoint][0][1]
            except IndexError:
                continue
            num_pages = math.ceil(num_records / limit)

            # do the requests
            file_handle = None
            if do_write and num_records > 0:
                file_handle = open(
                    os.path.join(
                        self.lightbeam.config["data_dir"], endpoint + ".jsonl"
                    ),
                    "w",
                )
            for p in range(0, num_pages):
                counter += 1
                tasks.append(
                    asyncio.create_task(
                        self.get_endpoint_records(
                            endpoint, limit, p * limit, file_handle
                        )
                    )
                )

        if len(tasks) > 0:
            await self.lightbeam.do_tasks(
                tasks, counter, log_status_counts=log_status_counts
            )

    # Fetches records for a specific endpoint
    async def get_endpoint_records(self, endpoint, limit, offset, file_handle=None):
        curr_token_version = int(str(self.lightbeam.token_version))
        while (
            True
        ):  # this is not great practice, but an effective way (along with the `break` below) to achieve a do:while loop
            try:
                # construct the URL query params:
                params = json.loads(self.lightbeam.query)
                params.update({"limit": str(limit), "offset": str(offset)})

                # send GET request
                async with self.lightbeam.api.client.get(
                    util.url_join(
                        self.lightbeam.api.config["data_url"],
                        self.lightbeam.config["namespace"],
                        endpoint,
                    ),
                    params=urlencode(params),
                    ssl=self.lightbeam.config["connection"]["verify_ssl"],
                    headers=self.lightbeam.api.headers,
                ) as response:
                    body = await response.text()
                    status = str(response.status)
                    if status == "401":
                        # this could be broken out to a separate function call,
                        # but not doing so should help keep the critical section small
                        if self.lightbeam.token_version == curr_token_version:
                            self.lightbeam.lock.acquire()
                            self.lightbeam.api.update_oauth()
                            self.lightbeam.lock.release()
                        else:
                            await asyncio.sleep(1)
                        curr_token_version = int(str(self.lightbeam.token_version))
                    elif status not in ["200", "201"]:
                        self.logger.warn(
                            f"Unable to load records for {endpoint}... {status} API response."
                        )
                    else:
                        if response.content_type == "application/json":
                            values = json.loads(body)
                            if type(values) != list:
                                self.logger.warn(
                                    f"Unable to load records for {endpoint}... API JSON response was not a list of records."
                                )
                            else:
                                payload_keys = list(values[0].keys())
                                final_keys = util.apply_selections(
                                    payload_keys,
                                    self.lightbeam.keep_keys,
                                    self.lightbeam.drop_keys,
                                )
                                do_key_filtering = len(payload_keys) != len(final_keys)
                                for v in values:
                                    if do_key_filtering:
                                        row = {k: v[k] for k in final_keys}
                                    else:
                                        row = v
                                    if file_handle:
                                        file_handle.write(json.dumps(row) + "\n")
                                    else:
                                        self.lightbeam.results.append(row)
                                    self.lightbeam.increment_status_counts(status)
                                break
                        else:
                            self.logger.warn(
                                f"Unable to load records for {endpoint}... API response was not JSON."
                            )

            except RuntimeError as e:
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.critical(
                    f"Unable to load records for {endpoint} from API... terminating. Check API connectivity."
                )

        self.lightbeam.num_finished += 1
