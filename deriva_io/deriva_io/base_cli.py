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
            '--config-file', default=DEFAULT_CONFIG_FILE, metavar='<file>',
            help="Optional path to a configuration file. If this argument is not specified, the configuration file "
                 "defaults to: %s " % DEFAULT_CONFIG_FILE)

        self.parser.add_argument(
            '--credential-file', default=DEFAULT_CREDENTIAL_FILE, metavar='<file>',
            help="Optional path to a credential file. If this argument is not specified, the credential file "
                 "defaults to: %s " % DEFAULT_CREDENTIAL_FILE)

        self.parser.add_argument(
            '--catalog-host', metavar="<FQDN[:port]>",
            help="Optional catalog host. If this argument is not specified the catalog host  "
                 "defaults to the value present in %s or the current hostname if the value cannot be determined"
                 % DEFAULT_CONFIG_FILE)

        self.parser.add_argument(
            '--catalog-id', metavar='<file>',
            help="Optional catalog ID. If this argument is not specified the catalog ID  "
                 "defaults to the value present in %s or 1 if the value cannot be determined" % DEFAULT_CONFIG_FILE)

        self.parser.add_argument(
            '--data-path', metavar="<path>", required=True,
            help="Path to data directory")

    def parse_cli(self):
        args = self.parser.parse_args()
        init_logging(level=logging.ERROR if args.quiet else (logging.DEBUG if args.debug else logging.INFO))

        return args
