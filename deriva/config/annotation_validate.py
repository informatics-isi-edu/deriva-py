"""DERIVA command-line interface for validating annotations."""

import logging
import re
from deriva.core import BaseCLI, DerivaServer, tag, annotation

logger = logging.getLogger(__name__)

_epilog = """
Known tag names include: {tags}.
""".format(tags=', '.join(tag.values()))


class AnnotationValidateCLI (BaseCLI):
    """Annotation Validate Command-Line Interface"""

    def __init__(self):
        super(AnnotationValidateCLI, self).__init__(__doc__, _epilog, hostname_required=True)
        self.remove_options(['--config-file'])
        self.parser.add_argument('catalog', metavar='<catalog>', help="Catalog identifier.")
        self.parser.add_argument('-s', '--schema', metavar='<schema>', default='.*', help="Regular expression pattern for schema name")
        self.parser.add_argument('-t', '--table', metavar='<table>', default='.*', help="Regular expression pattern for table name")
        self.parser.add_argument('-k', '--key', metavar='<key>', default='.*', help="Regular expression pattern for key constraint name")
        self.parser.add_argument('-f', '--foreign-key', metavar='<foreign_key>', default='.*', help="Regular expression pattern for foreign key constraint name")
        self.parser.add_argument('-a', '--tag', metavar='<tag>', default=None, help="Tag name of annotation")

    def main(self):
        args = self.parse_cli()
        hostname = args.host
        server = DerivaServer('https', hostname)
        catalog = server.connect_ermrest(args.catalog)
        model = catalog.getCatalogModel()

        # catalog annotation validation...
        logger.info("Validating catalog annotations...")
        errors = annotation.validate(model, tag_name=args.tag)
        has_errors = len(errors) > 0

        for schema_name in model.schemas:
            if not re.search(args.schema, schema_name):
                continue

            # schema annotation validation...
            logger.info("Validating '%s' annotations..." % schema_name)
            schema = model.schemas[schema_name]
            errors = annotation.validate(schema, tag_name=args.tag)
            has_errors = has_errors or len(errors) > 0

            for table_name in schema.tables:
                if not re.search(args.table, table_name):
                    continue

                # table annotations validation
                logger.info("Validating '%s:%s' annotations..." % (schema_name, table_name))
                table = model.schemas[schema_name].tables[table_name]
                errors = annotation.validate(table, tag_name=args.tag)
                has_errors = has_errors or len(errors) > 0

                for key in table.keys:
                    if not re.search(args.key, key.constraint_name):
                        continue

                    # key annotations validation
                    logger.info("Validating '%s:%s' annotations..." % (schema_name, key.constraint_name))
                    errors = annotation.validate(key, tag_name=args.tag)
                    has_errors = has_errors or len(errors) > 0

                for fkey in table.foreign_keys:
                    if not re.search(args.foreign_key, fkey.constraint_name):
                        continue

                    # fkey annotations validation
                    logger.info("Validating '%s:%s' annotations..." % (schema_name, fkey.constraint_name))
                    errors = annotation.validate(fkey, tag_name=args.tag)
                    has_errors = has_errors or len(errors) > 0

        return 1 if has_errors else 0


def main():
    cli = AnnotationValidateCLI()
    return cli.main()
