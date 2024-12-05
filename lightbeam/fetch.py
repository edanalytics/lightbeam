import os
import re
import math
import json
import asyncio
from urllib.parse import urlencode
from lightbeam import util

class Fetcher:

    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        self.logger = self.lightbeam.logger
        if self.lightbeam.query and type(self.lightbeam.query) == str:
            try:
                self.lightbeam.query = json.loads(self.lightbeam.query)
            except Exception as e:
                self.logger.error(f"A query was provided, but could not be parsed. Please give a JSON object as a string.")
    
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
        params = self.lightbeam.query
        for endpoint in self.lightbeam.endpoints:
            # figure out how many (paginated) requests we must make
            tasks.append(asyncio.create_task(self.lightbeam.counter.get_record_count(endpoint, params)))
        await self.lightbeam.do_tasks(tasks, counter, log_status_counts=log_status_counts)
        
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
            if do_write and num_records>0:
                file_handle = open(os.path.join(self.lightbeam.config["data_dir"], endpoint + ".jsonl"), "w")
            for p in range(0, num_pages):
                counter += 1
                tasks.append(asyncio.create_task(self.get_endpoint_records(endpoint, limit, p*limit, file_handle, depth=int(self.lightbeam.config.get("fetch",{}).get("follow_refs", 999999)))))

        if len(tasks)>0:
            await self.lightbeam.do_tasks(tasks, counter, log_status_counts=log_status_counts)
    
    # Fetches records for a specific endpoint
    async def get_endpoint_records(self, endpoint, limit, offset, file_handle=None, depth=999999):
        curr_token_version = int(str(self.lightbeam.token_version))
        refs = {}
        stop = False
        while not stop:
            try:
                # this section deals with the fact that the query might be
                # singular (if provided via CLI) or a list (from `--follow-refs`)
                params = self.lightbeam.query
                if type(params) == dict:
                    params_list = [params]
                else: params_list = params
                
                for params in params_list:
                    # construct the URL query params:
                    params.update({"limit": str(limit), "offset": str(offset)})
                    
                    # send GET request
                    async with self.lightbeam.api.client.get(
                        util.url_join(self.lightbeam.api.config["data_url"], self.lightbeam.config["namespace"], endpoint),
                        params=params,
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
                                    if len(list(values))>0:
                                        payload_keys = list(values[0].keys())
                                        final_keys = util.apply_selections(payload_keys, self.lightbeam.keep_keys, self.lightbeam.drop_keys)
                                        do_key_filtering = len(payload_keys) != len(final_keys)

                                        # follow-refs: set up the data structure where we store the refs to fetch next
                                        ref_keys = [k for k in payload_keys if k.endswith("Reference") and util.pluralize_endpoint(k.replace("Reference","")) not in self.lightbeam.endpoints]
                                        refs = {k: [] for k in ref_keys}

                                        for v in values:
                                            if do_key_filtering: row = {k: v.get(k, None) for k in final_keys} #v.get() to account for missing keys
                                            else: row = v
                                            # follow-refs:
                                            for k in ref_keys:
                                                if v.get(k, False):
                                                    q = v[k]
                                                    if "link" in q.keys(): del q["link"] # remove "link" element
                                                    if q not in refs[k]: # add ref payload to data structure, if not already present
                                                        refs[k].append(q)
                                            # back to row processing: write to JSONL file
                                            if file_handle: file_handle.write(json.dumps(row)+"\n")
                                            else: self.lightbeam.results.append(row)
                                            self.lightbeam.increment_status_counts(status)
                                    stop = True
                            else:
                                self.logger.warn(f"Unable to load records for {endpoint}... API response was not JSON.")

            except RuntimeError as e:
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.critical(f"Unable to load records for {endpoint} from API... terminating. Check API connectivity.")
        
        # follow-refs:
        save_query = self.lightbeam.query
        for k in refs.keys():
            if depth>0 and len(refs[k])>0:
                ref_endpoint = util.pluralize_endpoint(k.replace("Reference",""))
                # this deals with the fact that an educationOrganizationReference may be to a school, LEA, etc.:
                endpoints_to_check = self.lightbeam.EDFI_GENERICS_TO_RESOURCES_MAPPING.get(ref_endpoint, [ref_endpoint])
                for ref_endpoint in endpoints_to_check:
                    # this renames (for example) course.educationOrganizationReference: { educationOrganizationId: 9999 }
                    # to { localEducationAgencyId: 9999 }, { stateEducationAgencyId: 9999 }, etc.
                    for ref_k in self.lightbeam.EDFI_GENERIC_REFS_TO_PROPERTIES_MAPPING.keys():
                        new_key = self.lightbeam.EDFI_GENERIC_REFS_TO_PROPERTIES_MAPPING[ref_k].get(endpoint, ref_k)
                        refs[k] = [{new_key if k == 2 else k:v for k,v in row.items()} for row in refs[k]]
                    # some refs have a descriptive label prepended to the endpoint name, for example:
                    # * `studentSchoolAssociations.graduationSchoolYearTypeReference` is a `schoolYearType` reference
                    # * `assessments.contentStandard.mandatingEducationOrganizationReference` is a `educationOrganization` reference
                    # * `courseTranscripts.responsibleTeacherStaffReference` is a `staffs` reference
                    # (etc.) This while-loop repeatedly removes the front word from a camel-cased endpoint name
                    # and checkes whether the result is a valid endpoint name. If none is found, a warning is printed.
                    pieces = re.split('(?<=[a-z])(?=[A-Z])', ref_endpoint)
                    while len(pieces)>0 and ref_endpoint not in self.lightbeam.all_endpoints:
                        ref_endpoint = "".join(pieces[1:])
                        if len(ref_endpoint)>0: ref_endpoint = ref_endpoint[0].lower() + ref_endpoint[1:]
                        else:
                            pieces = []
                            break
                        pieces = re.split('(?<=[a-z])(?=[A-Z])', ref_endpoint)
                    if len(pieces)==0:
                        self.logger.warn(f"Could not find an endpoint corresponding to {k}.")
                        continue
                    # set up the fetch for this ref endpoint, and all its payloads
                    self.lightbeam.query = refs[k]
                    # print(ref_endpoint, refs[k][0])
                    self.lightbeam.results = []
                    await self.get_endpoint_records(ref_endpoint, limit=limit, offset=0, file_handle=None, depth=depth-1)
                    # if there were results, write them to a JSONL file
                    if len(self.lightbeam.results)>0:
                        with open(os.path.join(self.lightbeam.config["data_dir"], ref_endpoint + ".jsonl"), "w") as ref_file_handle:
                            for result in self.lightbeam.results:
                                ref_file_handle.write(json.dumps(result)+"\n")
                        break # no need to process other `endpoints_to_check`
                    
        self.lightbeam.query = save_query

        self.lightbeam.num_finished += 1