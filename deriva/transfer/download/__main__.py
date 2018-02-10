import sys
from deriva.transfer import DerivaDownloadCLI

DESC = "Deriva Data Download Utility - CLI"
INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-py"


def main():
    cli = DerivaDownloadCLI(DESC, INFO)
    return cli.main()


if __name__ == '__main__':
    sys.exit(main())
