import argparse
import logging
from deriva_common import DEFAULT_CONFIG_FILE, DEFAULT_CREDENTIAL_FILE, init_logging


class BaseCLI(object):

    def __init__(self, description, epilog):
        self.parser = argparse.ArgumentParser(description=description, epilog=epilog)

        self.parser.add_argument(
            '--quiet', action="store_true", help="Suppress logging output.")

        self.parser.add_argument(
            '--debug', action="store_true", help="Enable debug logging output.")

        self.parser.add_argument(
            '--config-file', metavar='<file>', help="Optional path to a configuration file.")

        self.parser.add_argument(
            '--credential-file', metavar='<file>', help="Optional path to a credential file.")

    def parse_cli(self):
        args = self.parser.parse_args()
        init_logging(level=logging.ERROR if args.quiet else (logging.DEBUG if args.debug else logging.INFO))

        return args
