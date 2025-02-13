import argparse
import logging

from . import init_logging, __version__
from .utils.version_utils import get_installed_version


class BaseCLI(object):

    def __init__(self, description, epilog, version=__version__, hostname_required=False, config_file_required=False):

        self.version = get_installed_version(version)

        self.parser = argparse.ArgumentParser(description=description, epilog=epilog)

        self.parser.add_argument(
            '--version', action='version', version=self.version, help="Print version and exit.")

        self.parser.add_argument(
            '--quiet', action="store_true", help="Suppress logging output.")

        self.parser.add_argument(
            '--debug', action="store_true", help="Enable debug logging output.")

        self.parser.add_argument(
            '--credential-file', metavar='<file>', help="Optional path to a credential file.")

        token_group = self.parser.add_mutually_exclusive_group()

        token_group.add_argument(
            "--token", metavar="<auth-token>", help="Authorization bearer token.")

        token_group.add_argument(
            "--oauth2-token", metavar="<oauth2-token>", help="OAuth2 bearer token.")        

        self.parser.add_argument(
            'host' if hostname_required else '--host', metavar='<host>', help="Fully qualified host name.")

        self.parser.add_argument(
            'config_file' if config_file_required else '--config-file',
            metavar='<config file>', help="Path to a configuration file.")

    def remove_options(self, options):
        for option in options:
            for action in self.parser._actions:
                if option in vars(action)['option_strings']:
                    self.parser._handle_conflict_resolve(None, [(option, action)])
                    break

    def parse_cli(self):
        args = self.parser.parse_args()
        init_logging(level=logging.CRITICAL if args.quiet else (logging.DEBUG if args.debug else logging.INFO))

        return args

    # Function to convert comma-separated CLI input into a tuple
    @staticmethod
    def parse_tuple(value):
        # Split the input by commas and convert to a tuple
        return tuple(map(float, value.split(',')))

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
