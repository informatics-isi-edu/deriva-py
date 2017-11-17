import sys
import traceback
from deriva.core import __version__ as VERSION
from deriva.core import BaseCLI

if sys.version_info > (3,):
    from urllib.parse import urlparse
else:
    from urlparse import urlparse


class DerivaHatracCLI (BaseCLI):
    """Deriva Hatrac Command-line Interface.
    """
    def __init__(self, description, epilog):
        """Initializes the CLI.
        """
        BaseCLI.__init__(self, description, epilog, VERSION)
        self.parser.add_argument("--token", default=1, metavar="<auth-token>", help="Authorization bearer token.")

    def main(self):
        """Main routine of the CLI.
        """
        sys.stderr.write("\n")
        try:
            args = self.parse_cli()
            print(args)
        except RuntimeError:
            return 1
        except:
            traceback.print_exc()
            return 1
        finally:
            sys.stderr.write("\n\n")
        return 0


def main():
    DESC = "Deriva Hatrac Command-Line Interface"
    INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-py"
    return DerivaHatracCLI(DESC, INFO).main()


if __name__ == '__main__':
    sys.exit(main())
