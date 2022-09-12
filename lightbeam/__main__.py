import os
import sys
import filecmp
import logging
import argparse
import traceback
from lightbeam import Lightbeam


class ExitOnExceptionHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        if record.levelno in (logging.ERROR, logging.CRITICAL):
            raise SystemExit(-1)

# Set up logging
handler = ExitOnExceptionHandler()
formatter = logging.Formatter("%(asctime)s.%(msecs)03d %(name)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
logger = logging.getLogger("lightbeam")
logger.setLevel(logging.getLevelName('INFO'))
logger.addHandler(handler)
# logging.basicConfig(handlers=[ExitOnExceptionHandler()])

def main(argv=None):
    if argv is None: argv = sys.argv
    
    description = "Sends JSONL data into an Ed-Fi API"
    
    parser = argparse.ArgumentParser(
        prog="lightbeam",
        description=description,
        epilog="Full documentation at https://github.com/edanalytics/lightbeam"
        )
    parser.add_argument('command',
        nargs="?",
        type=str,
        help='the command to run: `validate`, `send`, `validate+send`, or `delete`'
        )
    parser.add_argument('config_file',
        nargs="?",
        type=str,
        help='Specify YAML config file',
        metavar="FILE"
        )
    parser.add_argument("-v", "--version",
        action='store_true',
        help='view tool version'
        )
    parser.add_argument("-s", "--selector",
        nargs='?',
        help='sepecify a subset of resources to send'
        )
    parser.add_argument("-p", "--params",
        type=str,
        help='specify parameters as a JSON object via CLI (overrides environment variables)'
        )
    parser.add_argument("-f", "--force",
        action='store_true',
        help='resend all payloads, regardless of history'
        )
    parser.add_argument("-o", "--older-than",
        type=str,
        help='only payloads last sent before this timestamp will be resent'
        )
    parser.add_argument("-n", "--newer-than",
        type=str,
        help='only payloads last sent after this timestamp will be resent'
        )
    parser.add_argument("-r", "--resend-status-codes",
        type=str,
        help='only payloads that returned one of these comma-delimited HTTP status codes on last send will be resent'
        )
    parser.add_argument("-d", "--delete",
        type=str,
        help='deletes records with the IDs in your payload data (for clearing out data you sent previously)'
        )
    defaults = { "selector":"*", "params": "", "older_than": "", "newer_than": "", "resend_status_codes": "" }
    parser.set_defaults(**defaults)
    args, remaining_argv = parser.parse_known_args()
    
    if args.version: exit("lightbeam, version {0}".format(Lightbeam.version))

    if args.command not in ['validate', 'send', 'validate+send', 'delete']:
        logger.error("Please specify a command to run: `validate`, `send`, `validate+send`, or `delete`. (Try the -h flag for help.)")

    if not args.config_file:
        logger.error("Please pass a config YAML file as a command line argument. (Try the -h flag for help.)")
    try:
        logger.info("starting...")
        lb = Lightbeam(
            config_file=args.config_file,
            logger=logger,
            selector=args.selector,
            params=args.params,
            force=args.force,
            older_than=args.older_than,
            newer_than=args.newer_than,
            resend_status_codes=args.resend_status_codes
            )
        if args.command=='validate': lb.validate()
        elif args.command=='send': lb.send()
        elif args.command=='validate+send':
            lb.validate()
            lb.send()
        elif args.command=='delete': lb.delete()
        lb.logger.info("done!")
    except Exception as e:
        logger.exception(e, exc_info=lb.config["show_stacktrace"])

if __name__ == "__main__":
    sys.exit(main())