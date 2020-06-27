"""DERIVA command-line interface for validating annotations."""

from deriva.core import BaseCLI, DerivaServer, tag, annotation

_epilog = """
Known tag names include: {tags}.
""".format(tags=', '.join(tag.values()))


class AnnotationValidateCLI (BaseCLI):
    """Annotation Validate Command-Line Interface"""

    def __init__(self):
        super(AnnotationValidateCLI, self).__init__(__doc__, _epilog, hostname_required=True)
        # self.remove_options('<config file>')
        self.parser.add_argument('catalog', metavar='<catalog>', help="Catalog identifier.")
        self.parser.add_argument('schema', metavar='<schema>', help="Schema")
        self.parser.add_argument('table', metavar='<table>', help="Table")
        self.parser.add_argument('tag', metavar='<tag>', help="Tag name of annotation")

    def main(self):
        args = self.parse_cli()
        hostname = args.host
        server = DerivaServer('https', hostname)
        catalog = server.connect_ermrest(args.catalog)
        model = catalog.getCatalogModel()
        obj = model.schemas[args.schema].tables[args.table]
        errors = annotation.validate(obj, args.tag)
        for err in errors:
            print(err)
        return 1 if errors else 0


def main():
    cli = AnnotationValidateCLI()
    return cli.main()
