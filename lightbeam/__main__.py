import os
import sys
import filecmp
import argparse
import traceback
from lightbeam import Lightbeam


def main(argv=None):
    if argv is None: argv = sys.argv
    
    description = "Sends JSONL data into an Ed-Fi API"
    
    parser = argparse.ArgumentParser(
        prog="lightbeam",
        description=description,
        epilog="Full documentation at https://github.com/edanalytics/lightbeam"
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
    defaults = { "selector":"*", "params": "", "older_than": "", "newer_than": "", "resend_status_codes": "" }
    parser.set_defaults(**defaults)
    args, remaining_argv = parser.parse_known_args()
    
    if args.version: exit("lightbeam, version {0}".format(Lightbeam.version))
    if not args.config_file: exit("FATAL: Please pass a config YAML file as a command line argument. (Try the -h flag for help.)")
    try:
        lb = Lightbeam(
            config_file=args.config_file,
            params=args.params,
            force=args.force,
            older_than=args.older_than,
            newer_than=args.newer_than,
            resend_status_codes=args.resend_status_codes
            )
        lb.dispatch(selector=args.selector)
    except Exception as e:
        print('FATAL: ' + str(e))
        if(lb.config.show_stacktrace): traceback.print_exc()
        exit

if __name__ == "__main__":
    sys.exit(main())