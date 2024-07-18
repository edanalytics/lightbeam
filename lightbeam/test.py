import os
import json

class Tester:

    def __init__(self, lightbeam=None):
        self.lightbeam = lightbeam
        self.logger = self.lightbeam.logger
    
    def test(self, tests_dir):
        self.lightbeam.results = []

        # * Test `lightbeam count`:
        self.logger.info("testing `lightbeam count`...")
        count_file = os.path.join( tests_dir, "output", "count.tsv" )
        self.lightbeam.results_file = count_file
        self.lightbeam.selector = "localEducationAgencies,schoolTypeDescriptors"
        self.lightbeam.api.prepare()
        self.lightbeam.counter.count()
        expected_count_file = os.path.join( tests_dir, "expected", "count.tsv" )
        if not self.count_files_are_identical(count_file, expected_count_file):
            self.logger.critical("`lightbeam fetch -s localEducationAgencies` did not return the expected number of records")
        count_counts = self.load_count_file(count_file)

        # * Test `lightbeam fetch`:
        self.logger.info("testing `lighbeam fetch`...")
        self.lightbeam.results_file = ""
        self.lightbeam.selector = "localEducationAgencies,schoolTypeDescriptors"
        self.lightbeam.drop_keys = "id,_etag,_lastModified"
        self.lightbeam.api.prepare()
        self.lightbeam.fetcher.fetch()

        lea_jsonl_file = os.path.join( tests_dir, "data", "localEducationAgencies.jsonl" )
        num_leas = self.num_nonempty_lines_in_jsonl_file(lea_jsonl_file)
        if num_leas != count_counts["localEducationAgencies"]:
            self.logger.critical("`lightbeam fetch -s localEducationAgencies` did not return the expected number of records")
        keys_leas = self.keys_for_first_row_of_jsonl_file(lea_jsonl_file)
        lea_required_fields = ["localEducationAgencyId", "nameOfInstitution", "localEducationAgencyCategoryDescriptor", "categories"]
        for field in lea_required_fields:
            if field not in keys_leas:
                self.logger.critical(f"`lightbeam fetch -s localEducationAgencies` did not return a value for the required field {field}")

        schoolTypeDescriptors_jsonl_file = os.path.join( tests_dir, "data", "schoolTypeDescriptors.jsonl" )
        num_schoolTypeDescriptors = self.num_nonempty_lines_in_jsonl_file(schoolTypeDescriptors_jsonl_file)
        if num_schoolTypeDescriptors != count_counts["schoolTypeDescriptors"]:
            self.logger.critical("`lightbeam fetch -s schoolTypeDescriptors` did not return the expected number of records")
        keys_schoolTypeDescriptors = self.keys_for_first_row_of_jsonl_file(schoolTypeDescriptors_jsonl_file)
        schoolTypeDescriptors_required_field = ["codeValue", "namespace", "shortDescription"]
        for field in schoolTypeDescriptors_required_field:
            if field not in keys_schoolTypeDescriptors:
                self.logger.critical(f"`lightbeam fetch -s schoolTypeDescriptors` did not return a value for the required field {field}")
        
        os.remove(lea_jsonl_file)
        os.remove(schoolTypeDescriptors_jsonl_file)

        # * Test `lightbeam validate`:
        self.logger.info("testing `lighbeam validate`...")
        validate_file = os.path.join( tests_dir, "output", "validate.json" )
        self.lightbeam.results_file = validate_file
        self.lightbeam.selector = "*"
        self.lightbeam.drop_keys = ""
        self.lightbeam.api.prepare()
        self.lightbeam.validator.validate()
        # TODO: Uncomment the following once stuctured logging for validate is implemented
        # expected_validate_file = os.path.join( tests_dir, "expected", "validate.json" )
        # if not self.log_files_are_identical(validate_file, expected_validate_file):
        #     self.logger.critical("`lightbeam validate` did not return the expected output")
        
        # * Test `lightbeam send`:
        self.logger.info("testing `lighbeam send`...")
        pre_send_count_file = os.path.join( tests_dir, "output", "send-pre-count.tsv" )
        self.lightbeam.results_file = pre_send_count_file
        self.lightbeam.selector = "schools,educationServiceCenters"
        self.lightbeam.api.prepare()
        self.lightbeam.counter.count()
        pre_count = self.load_count_file(pre_send_count_file)
        
        send_file = os.path.join( tests_dir, "output", "send.json" )
        self.lightbeam.results_file = send_file
        self.lightbeam.api.prepare()
        self.lightbeam.sender.send()
        expected_send_file = os.path.join( tests_dir, "expected", "send.json" )
        if not self.log_files_are_identical(send_file, expected_send_file):
            self.logger.critical("`lightbeam send` did not return the expected output")
        
        post_send_count_file = os.path.join( tests_dir, "output", "send-post-count.tsv" )
        self.lightbeam.results_file = post_send_count_file
        self.lightbeam.selector = "schools,educationServiceCenters"
        self.lightbeam.api.prepare()
        self.lightbeam.counter.count()
        post_count = self.load_count_file(post_send_count_file)
        
        for k in set(pre_count.keys()).union(post_count.keys()):
            if pre_count.get(k, 0) + 1 != post_count.get(k, 0):
                self.logger.critical(f"`lightbeam send` did not send the expected number of records for {k}")

        # * Test `lightbeam delete`:
        self.logger.info("testing `lighbeam delete`...")
        self.lightbeam.results_file = ""
        self.lightbeam.api.prepare()
        self.lightbeam.deleter.delete()

        post_delete_count_file = os.path.join( tests_dir, "output", "delete-post-count.tsv" )
        self.lightbeam.results_file = post_delete_count_file
        self.lightbeam.selector = "schools,educationServiceCenters"
        self.lightbeam.api.prepare()
        self.lightbeam.counter.count()
        if not self.count_files_are_identical(pre_send_count_file, post_delete_count_file):
            self.logger.critical("`lightbeam delete` did not affect the expected number of records")
        
        # * Test `lightbeam truncate`:
        self.logger.info("testing `lighbeam truncate`...")
        pre_truncate_count_file = os.path.join( tests_dir, "output", "truncate-pre-count.tsv" )
        self.lightbeam.results_file = pre_truncate_count_file
        self.lightbeam.selector = "disciplineActions"
        self.lightbeam.api.prepare()
        self.lightbeam.counter.count()
        pre_count = self.load_count_file(pre_truncate_count_file)
        
        self.lightbeam.results_file = ""
        self.lightbeam.selector = "disciplineActions"
        self.lightbeam.drop_keys = "id,_etag,_lastModified"
        self.lightbeam.api.prepare()
        self.lightbeam.fetcher.fetch()
        records_jsonl_file = os.path.join( tests_dir, "data", "disciplineActions.jsonl" )
        num_records = self.num_nonempty_lines_in_jsonl_file(records_jsonl_file)
        # Just to be super safe, make sure we've fetched the same number of records as we counted:
        if num_records != pre_count.get("disciplineActions"):
            self.logger.critical(f"Preparation for `lightbeam truncate` resulted in a discrepancy between `count` and `fetch`. (No data has been deleted.)")
        
        self.lightbeam.results_file = ""
        self.lightbeam.selector = "disciplineActions"
        self.lightbeam.drop_keys = ""
        self.lightbeam.api.prepare()
        self.lightbeam.truncator.truncate()

        post_truncate_count_file = os.path.join( tests_dir, "output", "truncate-post-count.tsv" )
        self.lightbeam.results_file = post_truncate_count_file
        self.lightbeam.selector = "disciplineActions"
        self.lightbeam.api.prepare()
        self.lightbeam.counter.count()
        post_count = self.load_count_file(post_truncate_count_file)
        if post_count.get("disciplineActions") != 0:
            self.logger.critical(f"`lightbeam truncate` appears did not actually truncate the resource.")
        
        self.lightbeam.results_file = ""
        self.lightbeam.selector = "disciplineActions"
        self.lightbeam.api.prepare()
        self.lightbeam.sender.send()

        post_truncate_send_count_file = os.path.join( tests_dir, "output", "truncate-post-send-count.tsv" )
        self.lightbeam.results_file = post_truncate_send_count_file
        self.lightbeam.selector = "disciplineActions"
        self.lightbeam.api.prepare()
        self.lightbeam.counter.count()
        if not self.count_files_are_identical(pre_truncate_count_file, post_truncate_send_count_file):
            self.logger.critical("`lightbeam send` did not restore the original records after `truncate`")
        
        os.remove(records_jsonl_file)
        output_dir = os.path.join( tests_dir, "output")
        visible_files = [file for file in os.listdir(output_dir) if not file.startswith('.')]
        for file in visible_files:
            os.remove( os.path.join( output_dir, file) )
        
        return True
    
    
    @staticmethod
    def num_nonempty_lines_in_jsonl_file(file):
        with open(file, 'r') as f:
            lines = f.readlines()
            num_lines = len([l for l in lines if l.strip(' \n') != ''])
            return num_lines
    
    @staticmethod
    def load_count_file(file):
        counter = 0
        counts = {}
        with open(file, 'r') as f:
            for line in f:
                counter += 1
                if counter == 1: continue
                (records, endpoint) = line.strip(' \n').split("\t")
                counts[endpoint] = int(records)
            return counts
    
    @staticmethod
    def keys_for_first_row_of_jsonl_file(file):
        with open(file, 'r') as f:
            line = f.readline()
            return json.loads(line).keys()
    
    @staticmethod
    def count_files_are_identical(file1, file2):
        file1_counts = Tester.load_count_file(file1)
        file2_counts = Tester.load_count_file(file2)
        if file1_counts != file2_counts: return False
        return True
    
    @staticmethod
    def log_files_are_identical(file1, file2):
        file1_log = json.load(open(file1))
        file2_log = json.load(open(file2))
        # unset keys that will definitely change
        keys_to_ignore = ["started_at", "working_dir", "config_file", "data_dir", "api_url", "completed_at", "runtime_sec"]
        for k in keys_to_ignore:
            del file1_log[k]
            del file2_log[k]
        logs = [file1_log, file2_log]
        for log in logs:
            for res,val in log.get("resources",{}).items():
                if "failures" in val.keys():
                    for failure in val.get("failures", []):
                        failure.file = ""
        if file1_log != file2_log: return False
        return True