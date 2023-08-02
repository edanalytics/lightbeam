import asyncio
import copy
import hashlib
import time

from collections import defaultdict
from typing import Dict, List

from . import util


class RESTEvent:

    total_seen: int = 0
    total_processed: int = 0
    total_succeeded: int = 0
    total_skipped: int = 0
    total_failed: int = 0

    endpoint_total_counts: Dict[str, dict] = defaultdict(
        lambda: {"records_processed": 0, "records_skipped": 0, "records_failed": 0}
    )
    # Keys: Tuple[endpoint, status, message, file]
    endpoint_status_reason_counts: Dict[(str, int, str, str), int] = defaultdict(int)
    endpoint_failed_lines: Dict[(str, int, str, str), List[int]] = defaultdict(list)

    def __init__(self,
        endpoint: str,
        namespace: str,

        file: str,
        line: int,

        data: str
    ):
        self.endpoint: str = endpoint
        self.namespace: str = namespace
        # self.full_endpoint: str = f"{camel_case(self.namespace)}_{self.endpoint}"

        self.file: str = file
        self.line: int = line

        self.data: str = data.strip()

        self.status: int = 401
        self.reason: str = None

        self._data_hash = None

        RESTEvent.total_seen += 1

    @property
    def message(self):
        return f"{self.status}: {self.reason}"

    @property
    def data_hash(self):
        if self._data_hash is None:
            self._data_hash = hashlib.md5(self.data.encode()).digest()
        return self._data_hash

    def increment_num_processed(self):
        self.total_processed += 1
        self.endpoint_total_counts[self.endpoint]["records_processed"] += 1

    def increment_num_succeeded(self):
        self.total_succeeded += 1
        self.endpoint_status_reason_counts[(self.endpoint, self.status, self.reason, self.file)] += 1

    def increment_num_failed(self):
        self.total_failed += 1
        self.endpoint_total_counts[self.endpoint]["records_failed"] += 1
        self.endpoint_status_reason_counts[(self.endpoint, self.status, self.reason, self.file)] += 1
        self.endpoint_failed_lines[(self.endpoint, self.status, self.message, self.file)].append(self.line)

    def increment_num_skipped(self):
        self.increment_num_processed()
        self.total_skipped += 1
        self.endpoint_total_counts[self.endpoint]["records_skipped"] += 1

    @classmethod
    def reset_counters(cls):
        cls.total_seen: int = 0
        cls.total_processed: int = 0
        cls.total_skipped: int = 0
        cls.total_failed: int = 0

        cls.endpoint_counts: Dict[str, dict] = defaultdict(
            lambda: {"records_processed": 0, "records_skipped": 0, "records_failed": 0}
        )
        cls.failed_lines: Dict[str, Dict[(int, str, str): List[int]]] = defaultdict(defaultdict(list))  # (status, message, file)

    @classmethod
    def status_reasons(cls):
        """  """
        reasons_dict = defaultdict(int)

        for (status, message, file), line_nums in cls.failed_lines.items():
            reasons_dict[message] += len(line_nums)

        return reasons_dict

    @classmethod
    def status_counts(cls):
        """  """
        statuses_dict = defaultdict(int)

        for (status, message, file), line_nums in cls.failed_lines.items():
            statuses_dict[status] += len(line_nums)

        return statuses_dict

    async def post(self, client, *, lightbeam: 'Lightbeam'):
        """
        Post a single data payload to a single endpoint using the client

        :param client:
        :param lightbeam:
        :return:
        """
        while self.status == 401:

            # wait if another process has locked lightbeam while we refresh the oauth token:
            while lightbeam.is_locked:
                await asyncio.sleep(1)

            try:
                async with client.post(
                    util.url_join(
                        lightbeam.api.config["data_url"],
                        self.namespace,
                        self.endpoint
                    ),
                    data=self.data,
                    ssl=lightbeam.config["connection"]["verify_ssl"],
                    headers=lightbeam.api.headers
                ) as response:

                    await response.text()

                    self.status = response.status
                    self.reason = util.linearize(response.text())

                    if self.status == 401:
                        lightbeam.api.update_oauth()
                        continue

                    self.increment_num_processed()

                    # warn about errors; update output and counters
                    if not response.ok:
                        self.increment_num_failed()

                        if self.status == 400:
                            raise Exception(self.message)

                    # update hashlog
                    if lightbeam.track_state:
                        self.hashlog_data[self.data_hash] = (round(time.time()), self.status)

            except Exception as err:
                self.logger.warn(
                    f"{err}  (at line {self.line} of {self.file} )"
                )
                break

    @classmethod
    def to_json(cls):
        """
        v:endpoints:"studentAssessments":statuses:"400":responses[0]

        "endpoints": {
            "{studentAssessments}: {
                "statuses": {
                    "{400}": {
                        "responses": [{
                            "message": "string",
                            "file": "string",
                            "count": 0,
                            "errors": "1,2,3",
                        }],
                        "count": 0,
                    }
                },
                "records_processed": 0,
                "records_skipped": 0,
                "records_failed": 0,
            }
        }

        :return:
        """
        status_payload = {
            "responses": [],
            "count": 0,
        }

        endpoint_payload = {
            "statuses": defaultdict(lambda: copy.deepcopy(status_payload)),
            "records_processed": 0,
            "records_skipped": 0,
            "records_failed": 0,
        }

        full_payload = defaultdict(lambda: copy.deepcopy(endpoint_payload))

        for full_endpoint, endpoint_count_dict in cls.endpoint_counts.items():
            for count_type in ("records_processed", "records_skipped", "records_failed"):
                full_payload[full_endpoint][count_type] += endpoint_count_dict[count_type]

            for (status, message, file), line_numbers in cls.failed_lines.items():
                full_payload[full_endpoint]["statuses"][status]["responses"].append(
                    {
                        "message": message,
                        "file": file,
                        "count": len(line_numbers),
                        "errors": ",".join(map(str, line_numbers)),
                    }
                )
                full_payload[full_endpoint]["statuses"][status]["count"] += len(line_numbers)

        return full_payload
