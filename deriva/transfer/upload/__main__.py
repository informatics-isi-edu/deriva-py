import sys
from deriva.transfer import DerivaUploadCLI, GenericUploader

DESC = "Deriva Data Upload Utility - CLI"
INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-py"


def main():
    cli = DerivaUploadCLI(GenericUploader, DESC, INFO)
    return cli.main()


if __name__ == '__main__':
    sys.exit(main())
