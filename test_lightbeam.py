# Run this test by first `pip install -U pytest` and then `pytest -o log_cli=True` (the flag shows progress)
# We recommend that the test suite is run against a local Docker deployment of an Ed-Fi API with the Grand Bend sample dataset.
# (see https://github.com/Ed-Fi-Alliance-OSS/Ed-Fi-ODS-Docker)
# Details about the tests this suite runs are documented in `lightbeam/tests/README.md`

# TODO: update the test suite to compare CLI output to expected

import os
import logging
from lightbeam.lightbeam import Lightbeam

logger = logging.getLogger("lightbeam")
logger.setLevel(logging.getLevelName('INFO'))

def test_suite():
    tests_dir = os.path.join( os.path.realpath(os.path.dirname(__file__)), "lightbeam", "tests" )
            
    # Prompt for Ed-Fi API base_url, key, secret (with defaults)!
    logger.info("Welcome to the lightbeam test suite. This feature is intended for developer use.")
    logger.info("RUNNING THIS TEST SUITE WILL CAUSE CHANGES TO YOUR ED-FI DATA - USE WITH CAUTION!")

    # base_url = input("Enter your Ed-Fi API's base URL: [\"https://localhost/api\"]:") or "https://localhost/api"
    # client_id = input("Enter your API client ID: [\"populated\"]:") or "populated"
    # client_secret = input("Enter your API client secret: [\"populatedSecret\"]:") or "populatedSecret"
    
    # Unfortunately it's not possible to solicit user input during a pytest,
    # so we have to hard-code the ODS params to use for the test:
    base_url = "https://localhost/api"
    client_id = "populated"
    client_secret = "populatedSecret"

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
    try:
        lb.logger.info("running tests...")
        assert lb.tester.test(tests_dir)
        lb.logger.info('all tests passed successfully.')
    except Exception as e:
        logger.exception(e, exc_info=lb.config["show_stacktrace"])