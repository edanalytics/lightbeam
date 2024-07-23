# Run this test by first `pip install -U pytest` and then `pytest -o log_cli=True` (the flag shows progress)
# We recommend that the test suite is run against a local Docker deployment of an Ed-Fi API with the Grand Bend sample dataset.
# (see https://github.com/Ed-Fi-Alliance-OSS/Ed-Fi-ODS-Docker)
# Details about the tests this suite runs are documented in `lightbeam/tests/README.md`

# TODO: update the test suite to compare CLI output to expected

import os
from lightbeam.tests.suite import run_test_suite
import logging
from lightbeam.lightbeam import Lightbeam

logger = logging.getLogger("lightbeam")
logger.setLevel(logging.getLevelName('INFO'))

def test_suite():
    tests_dir = os.path.join( os.path.realpath(os.path.dirname(__file__)), "lightbeam", "tests" )
            
    # Prompt for Ed-Fi API base_url, key, secret (with defaults)!
    logger.info("Welcome to the lightbeam test suite. This feature is intended for developer use.")
    logger.info("RUNNING THIS TEST SUITE WILL CAUSE CHANGES TO YOUR ED-FI DATA - USE WITH CAUTION!")
    logger.info("")

    # Unfortunately it's not possible to solicit user input during a pytest,
    # so we define these as environment variables:
    base_url = os.environ.get('EDFI_API_BASE_URL', "https://localhost/api")
    client_id = os.environ.get('EDFI_API_CLIENT_ID', "populated")
    client_secret = os.environ.get('EDFI_API_CLIENT_SECRET', "populatedSecret")
    
    logger.info("Running with")
    logger.info(f"- base_url={base_url} (modify via env var EDFI_API_BASE_URL)")
    logger.info(f"- client_id={client_id} (modify via env var EDFI_API_CLIENT_ID)")
    logger.info(f"- client_secret={client_secret[0:5]}... (modify via env var EDFI_API_CLIENT_SECRET)")
    logger.info("")

    lb = Lightbeam(
        config_file=os.path.join(tests_dir, "lightbeam.yaml"),
        logger=logger,
        selector="*",
        exclude="",
        keep_keys="*",
        drop_keys="",
        query="{}",
        params='{"BASE_DIR": "' + tests_dir + '", "BASE_URL": "' + base_url + '", "CLIENT_ID": "' + client_id + '", "CLIENT_SECRET": "' + client_secret + '"}',
        )
    lb.logger.info("running tests...")
    assert run_test_suite(tests_dir, lb, lb.logger)
    lb.logger.info('all tests passed successfully.')
    