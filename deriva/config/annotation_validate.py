"""DERIVA command-line interface for validating annotations."""

import re
from deriva.core import BaseCLI, DerivaServer, tag, annotation

_epilog = """
Known tag names include: {tags}.
""".format(tags=', '.join(tag.values()))


class AnnotationValidateCLI (BaseCLI):
    """Annotation Validate Command-Line Interface"""

    def __init__(self):
        super(AnnotationValidateCLI, self).__init__(__doc__, _epilog, hostname_required=True)
        # self.remove_options(['--config-file'])
        self.parser.add_argument('catalog', metavar='<catalog>', help="Catalog identifier.")
        self.parser.add_argument('-s', '--schema', metavar='<schema>', default='.*', help="Regular expression pattern for schema name")
        self.parser.add_argument('-t', '--table', metavar='<table>', default='.*', help="Regular expression pattern for table name")
        self.parser.add_argument('-a', '--tag', metavar='<tag>', default=None, help="Tag name of annotation")

    def main(self):
        args = self.parse_cli()
        hostname = args.host
        server = DerivaServer('https', hostname)
        catalog = server.connect_ermrest(args.catalog)
        model = catalog.getCatalogModel()
        has_errors = False
        for schema_name in model.schemas:
            if not re.search(args.schema, schema_name):
                continue
            for table_name in model.schemas[schema_name].tables:
                if not re.search(args.table, table_name):
                    continue
                print("Validating '%s:%s'..." % (schema_name, table_name))
                obj = model.schemas[schema_name].tables[table_name]
                errors = annotation.validate(obj, tag_name=args.tag)
                has_errors = has_errors or len(errors) > 0
                for err in errors:
                    print(err)
        return 1 if has_errors else 0


def main():
    cli = AnnotationValidateCLI()
    return cli.main()
