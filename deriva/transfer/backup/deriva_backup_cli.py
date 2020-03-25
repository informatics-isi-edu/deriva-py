import os
import sys
import json
import traceback
import argparse
from deriva.core import BaseCLI, KeyValuePairArgs, format_credential, format_exception, urlparse
from deriva.transfer import DerivaBackup, DerivaDownloadCLI


class DerivaBackupCLI(DerivaDownloadCLI):
    def __init__(self, description, epilog, **kwargs):

        DerivaDownloadCLI.__init__(self, description, epilog, **kwargs)
        mutex_group = self.parser.add_mutually_exclusive_group()
        mutex_group.add_argument("--no-data", action="store_true",
                                 help="Do not export data, export schema only.")
        mutex_group.add_argument("--no-schema", action="store_true",
                                 help="Do not export schema, export data only.")
        bag_mutex_group = self.parser.add_mutually_exclusive_group()
        bag_mutex_group.add_argument("--no-bag", action="store_true",
                                     help="Do not store the output in a bag container.")
        bag_mutex_group.add_argument("--include-assets", choices=['full', 'references'],
                                     help="Include related file assets in output bag. Use \"full\" to download "
                                          "related assets to the output bag. Use \"references\" to store "
                                          "references to asset files in the bag's \"fetch.txt\" file.")
        self.parser.add_argument("--bag-archiver", choices=['zip', 'tgz', 'bz2'],
                                 help="Format for compressed bag output.")
        self.parser.add_argument("--exclude-data", default=list(),
                                 type=lambda s: [item.strip() for item in s.split(',')],
                                 metavar="<schema>, <schema:table>, ...",
                                 help="List of comma-delimited schema-name and/or schema-name/table-name to "
                                      "exclude from data export, in the form <schema> or <schema:table>.")

    @classmethod
    def get_downloader(cls, *args, **kwargs):
        return DerivaBackup(*args, dcctx_cid="cli/" + DerivaBackupCLI.__name__, **kwargs)
