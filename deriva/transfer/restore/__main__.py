import sys
from deriva.transfer import DerivaRestoreCLI

DESC = "Deriva Catalog Restore Utility - CLI"
INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-py"


def main():
    cli = DerivaRestoreCLI(DESC, INFO, hostname_required=True, config_file_required=False)
    return cli.main()


if __name__ == '__main__':
    sys.exit(main())
