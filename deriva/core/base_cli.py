import argparse
import logging

from . import init_logging


class BaseCLI(object):

    def __init__(self, description, epilog, version=None):
        assert version, "A valid version string is required"

        self.version = version

        self.parser = argparse.ArgumentParser(description=description, epilog=epilog)

        self.parser.add_argument(
            '--version', action='version', version=self.version, help="Print version and exit.")

        self.parser.add_argument(
            '--quiet', action="store_true", help="Suppress logging output.")

        self.parser.add_argument(
            '--debug', action="store_true", help="Enable debug logging output.")

        self.parser.add_argument(
            '--host', metavar='<fqhn>', help="Optional fully qualified host name to connect to.")

        self.parser.add_argument(
            '--config-file', metavar='<file>', help="Optional path to a configuration file.")

        self.parser.add_argument(
            '--credential-file', metavar='<file>', help="Optional path to a credential file.")

        self.parser.add_argument(
            "--token", metavar="<auth-token>", help="Authorization bearer token.")

    def remove_options(self, options):
        for option in options:
            for action in self.parser._actions:
                if vars(action)['option_strings'][0] == option:
                    self.parser._handle_conflict_resolve(None, [(option, action)])
                    break

    def parse_cli(self):
        args = self.parser.parse_args()
        init_logging(level=logging.CRITICAL if args.quiet else (logging.DEBUG if args.debug else logging.INFO))

        return args


class KeyValuePairArgs(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        self._nargs = nargs
        super(KeyValuePairArgs, self).__init__(option_strings, dest, nargs=nargs, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        kwargs = dict()
        for kv in values:
            arg = kv.split("=", 1)
            if len(arg) < 2:
                raise ValueError(
                    "Invalid key-value argument %s: Key-Value pairs must be given in the form <key=value>." % arg)
            kwargs[arg[0]] = arg[1]
            setattr(namespace, self.dest, kwargs)
