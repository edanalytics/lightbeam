import os
import sys
import logging
import argparse
from lightbeam.lightbeam import Lightbeam


class ExitOnExceptionHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        if record.levelno in (logging.ERROR, logging.CRITICAL):
            raise SystemExit(-1)

DEFAULT_CONFIG_FILES = ['lightbeam.yaml', 'lightbeam.yml']

# use a dictionary here so that command strings can be accessed with a lookup.
#   This helps enforce usage of this structure
ALLOWED_COMMANDS = {
   "validate": "validate",
   "send": "send",
   "validate+send": "validate+send",
   "delete": "delete",
   "truncate": "truncate",
   "count": "count",
   "fetch": "fetch",
}
command_list = ', '.join(f"'{c}'" for c in ALLOWED_COMMANDS.values())

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
        help=f'the command to run: {command_list}'
        )
    parser.add_argument("-c", "--config-file",
        nargs="?",
        type=str,
        help='Specify YAML config file',
        metavar="CONFIG_FILE"
        )
    parser.add_argument("-v", "--version",
        action='store_true',
        help='view tool version'
        )
    parser.add_argument("-s", "--selector",
        nargs='?',
        help='sepecify a subset of resources to process'
        )
    parser.add_argument("-e", "--exclude",
        nargs='?',
        help='sepecify a subset of resources to exclude from processing'
        )
    parser.add_argument("-k", "--keep-keys",
        nargs='?',
        help='sepecify a comma-delimited list of keys to keep from `fetch`ed payloads (if not specified, all keys are kept)'
        )
    parser.add_argument("-d", "--drop-keys",
        nargs='?',
        help='sepecify a comma-delimited list of keys to remove from `fetch`ed payloads (like `id,_etag,_lastModifiedDate`)'
        )
    parser.add_argument("-q", "--query",
        type=str,
        help='a JSON dictionary of query parameters to add to GET requests when using the `fetch` command'
        )
    parser.add_argument("-p", "--params",
        type=str,
        help='specify parameters as a JSON object via CLI (overrides environment variables)'
        )
    parser.add_argument("-w", "--wipe",
        action='store_true',
        help='wipe cached Swagger docs and descriptor values'
        )
    parser.add_argument("-f", "--force",
        action='store_true',
        help='process all payloads, ignoring history'
        )
    parser.add_argument("-o", "--older-than",
        type=str,
        help='only payloads last sent before this timestamp will be processed'
        )
    parser.add_argument("-n", "--newer-than",
        type=str,
        help='only payloads last sent after this timestamp will be processed'
        )
    parser.add_argument("-r", "--resend-status-codes",
        type=str,
        help='only payloads that returned one of these comma-delimited HTTP status codes on last send will be processed'
        )
    parser.add_argument("--results-file",
        type=str,
        help='produces a JSON output file with structured information about run results'
    )
    parser.add_argument("--set",
        type=str,
        nargs="*",
        help='overrides a setting in the config YAML; example: --set fetch.page_size 1000'
    )

    defaults = { "selector":"*", "params": "", "older_than": "", "newer_than": "", "resend_status_codes": "", "results_file": "" }
    parser.set_defaults(**defaults)
    args, unknown_args = parser.parse_known_args()
    if len(unknown_args) > 0:
        unknown_args_str = ', '.join(f"`{c}`" for c in unknown_args)
        print(f"unknown arguments {unknown_args_str} passed, use -h flag for help")
        exit(1)
    
    if args.version:
        lb_dir = os.path.dirname(os.path.abspath(__file__))
        version_file = os.path.join(lb_dir, 'VERSION.txt')
        with open(version_file, 'r') as f:
            VERSION = f.read().strip()
            print(f"lightbeam, version {VERSION}")
        exit(0)
    

    if args.command not in ALLOWED_COMMANDS.values():
        if args.command is None:
            logger.error(f"no command provided. Use one of ({command_list}), see -h flag for help")
        else:
            logger.error(f"unknown command '{args.command}' passed, use -h flag for help")
        exit(1)
    
    if not args.config_file:
        for file in DEFAULT_CONFIG_FILES:
            test_file = os.path.join(".", file)
            if os.path.isfile(test_file):
                args.config_file = test_file
                logger.info(f"config file not specified with `-c` flag... but found and using ./{file}")
                break

    if not args.config_file:
        logger.error("config file not specified with `-c` flag, and no default {" + ", ".join(DEFAULT_CONFIG_FILES) + "} found")
    
    if args.set and len(args.set)%2 != 0: # odd number of overrides
        logger.error("overrides specified with --set must be followed by an even number of strings (key value key value ...)")
    overrides = None
    if args.set:
        overrides = dict(zip(args.set[::2], args.set[1::2]))

    lb = Lightbeam(
        config_file=args.config_file,
        logger=logger,
        selector=args.selector or "*",
        exclude=args.exclude or "",
        keep_keys=args.keep_keys or "*",
        drop_keys=args.drop_keys or "",
        query=args.query or "{}",
        params=args.params,
        wipe=args.wipe,
        force=args.force,
        older_than=args.older_than,
        newer_than=args.newer_than,
        resend_status_codes=args.resend_status_codes,
        results_file=args.results_file,
        overrides=overrides,
        )
    try:
        logger.info("starting...")
        if args.command==ALLOWED_COMMANDS['count']: lb.counter.count()
        elif args.command==ALLOWED_COMMANDS['fetch']: lb.fetcher.fetch()
        elif args.command==ALLOWED_COMMANDS['validate']: lb.validator.validate()
        elif args.command==ALLOWED_COMMANDS['send']: lb.sender.send()
        elif args.command==ALLOWED_COMMANDS['validate+send']:
            lb.validator.validate()
            lb.sender.send()
        elif args.command==ALLOWED_COMMANDS['delete']: lb.deleter.delete()
        elif args.command==ALLOWED_COMMANDS['truncate']: lb.truncator.truncate()
        lb.logger.info("done!")
    except Exception as e:
        logger.exception(e, exc_info=lb.config["show_stacktrace"])

if __name__ == "__main__":
    sys.exit(main())
