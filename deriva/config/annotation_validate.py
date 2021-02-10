"""DERIVA command-line interface for validating annotations."""

import logging
import re
import sys
from deriva.core import BaseCLI, DerivaServer, tag, annotation, get_credential, format_credential

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
        self.parser.add_argument('-c', '--column', metavar='<column>', default='.*', help="Regular expression pattern for column name")
        self.parser.add_argument('-k', '--key', metavar='<key>', default='.*', help="Regular expression pattern for key constraint name")
        self.parser.add_argument('-f', '--foreign-key', metavar='<foreign_key>', default='.*', help="Regular expression pattern for foreign key constraint name")
        self.parser.add_argument('-a', '--tag', metavar='<tag>', default=None, help="Tag name of annotation")
        self.parser.add_argument('--skip-model-names', action='store_true', help="Skip validation of model names found inside of annotations")

    def main(self):
        args = self.parse_cli()
        validate_model_names = not args.skip_model_names

        if args.token:
            credential = format_credential(token=args.token)
        elif args.oauth2_token:
            credential = format_credential(oauth2_token=args.oauth2_token)
        else:
            credential = get_credential(args.host, credential_file=args.credential_file)

        server = DerivaServer('https', args.host, credentials=credential)
        catalog = server.connect_ermrest(args.catalog)
        catalog.dcctx['cid'] = "cli/" + AnnotationValidateCLI.__name__
        model = catalog.getCatalogModel()
        errors = []
        num_objects_tested = 0

        # catalog annotation validation...
        logger.debug("Validating catalog annotations...")
        errors.extend(annotation.validate(model, tag_name=args.tag, validate_model_names=validate_model_names))
        num_objects_tested += 1

        for schema_name in model.schemas:
            if not re.search(args.schema, schema_name):
                continue

            # schema annotation validation...
            logger.debug("Validating '%s' annotations..." % schema_name)
            schema = model.schemas[schema_name]
            errors.extend(annotation.validate(schema, tag_name=args.tag, validate_model_names=validate_model_names))
            num_objects_tested += 1

            for table_name in schema.tables:
                if not re.search(args.table, table_name):
                    continue

                # table annotations validation
                logger.debug("Validating '%s:%s' annotations..." % (schema_name, table_name))
                table = model.schemas[schema_name].tables[table_name]
                errors.extend(annotation.validate(table, tag_name=args.tag, validate_model_names=validate_model_names))
                num_objects_tested += 1

                for column in table.column_definitions:
                    if not re.search(args.column, column.name):
                        continue

                    # column annotations validation
                    logger.debug("Validating '%s:%s:%s' annotations..." % (schema_name, table_name, column.name))
                    errors.extend(annotation.validate(column, tag_name=args.tag, validate_model_names=validate_model_names))
                    num_objects_tested += 1

                for key in table.keys:
                    if not re.search(args.key, key.constraint_name):
                        continue

                    # key annotations validation
                    logger.debug("Validating '%s:%s' annotations..." % (schema_name, key.constraint_name))
                    errors.extend(annotation.validate(key, tag_name=args.tag, validate_model_names=validate_model_names))
                    num_objects_tested += 1

                for fkey in table.foreign_keys:
                    if not re.search(args.foreign_key, fkey.constraint_name):
                        continue

                    # fkey annotations validation
                    logger.debug("Validating '%s:%s' annotations..." % (schema_name, fkey.constraint_name))
                    errors.extend(annotation.validate(fkey, tag_name=args.tag, validate_model_names=validate_model_names))
                    num_objects_tested += 1

        logger.info("Found %d error(s) in %d model object(s)." % (len(errors), num_objects_tested))
        return 1 if errors else 0


def main():
    cli = AnnotationValidateCLI()
    return cli.main()


if __name__ == '__main__':
    sys.exit(main())
