This directory contains data and configuration required for a test suite for lightbeam. The `data/` directory contains synthetic data for testing lightbeam's functionality. It is intended to be used against a test Ed-Fi API that is prepopulated with the Grand Bend dataset.

Run the test suite with `lightbeam -t`. You will be prompted for the Ed-Fi API base URL, key, and secret to test against.

The point of this test suite is to test lightbeam's commands and communication with an Ed-Fi API. We are careful to try to make it robust to changes in the Ed-Fi data model.

The test suite does the following:
* Test `lightbeam count`:
    * Run `lightbeam count --results-file ./output/count.tsv -s localEducationAgencies,schoolTypeDescriptors` and compare row counts to expected (1, 4)
* Test `lightbeam fetch`:
    * `lightbeam fetch -s localEducationAgencies,schoolTypeDescriptors --drop-keys id,_etag,_lastModified` and compare row counts to expected, plus check that natural keys exist
    * delete `data/localEducationAgencies.jsonl` and `data/schoolTypeDescriptors.jsonl`
* Test `lightbeam validate`:
    * `lightbeam validate --results-file ./output/validate.json` and compare output to `expected/validate-output.json`
* Test `lightbeam send`:
    * `lightbeam count --results-file ./output/send-pre-count.tsv -s schools,students` to document existing row counts
    * `lightbeam send --results-file ./output/send.json` and compare output to `expected/output/send.json`
    * `lightbeam count --results-file ./output/send-post-count.tsv -s schools,students` and compare to `send-pre-count.tsv`
* Test `lightbeam delete`:
    * `lightbeam delete` (to delete records sent y `send` above)
    * `lightbeam count --results-file ./output/delete-post-count.tsv -s schools,students` and confirm row counts back to `send-pre-count.tsv`
* Test `lightbeam truncate`:
    * `lightbeam count --results-file ./output/truncate-pre-count.tsv -s studentDisciplineIncidentAssociations`
    * `lightbeam fetch -s studentDisciplineIncidentAssociations --drop-keys id,_etag,_lastModified`
    * `lightbeam truncate -s studentDisciplineIncidentAssociations`
    * `lightbeam count --results-file ./output/truncate-post-count.tsv -s studentDisciplineIncidentAssociations` and confirm row count is zero
    * `lightbeam send --results-file ./output/send3.json -s studentDisciplineIncidentAssociations` and compare output to `expected/send3-output.json`
    * clean up by deleting `data/studentDisciplineIncidentAssociations.jsonl` and `output/*`