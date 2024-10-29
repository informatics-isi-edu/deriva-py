import sys
from deriva.core import get_credential, BaseCLI, DerivaServer, __version__


def rollback_annotation(host, catalog_id, snaptime=None, prot='https', credential=None):
    """
    Rollback the entire annotation hierarchy for the specified catalog to a given point in time specified by snaptime.
    """
    if not credential:
        credential = get_credential(host)
    server = DerivaServer(prot, host, credentials=credential)

    good_catalog = server.connect_ermrest(catalog_id, snaptime)
    good_config = good_catalog.getCatalogModel()

    live_catalog = server.connect_ermrest(catalog_id)
    live_config = live_catalog.getCatalogModel()

    # copy over annotations

    # catalog-level
    live_config.annotations.clear()
    live_config.annotations.update(good_config.annotations)

    for sname, live_schema in live_config.schemas.items():
        good_schema = good_config.schemas[sname]

        # schema-level
        live_schema.annotations.clear()
        live_schema.annotations.update(good_schema.annotations)

        for tname, live_table in live_schema.tables.items():
            if tname not in good_schema.tables:
                print('Warning: skipping live table %s.%s which lacks known-good table' % (sname, tname))
                continue

            good_table = good_schema.tables[tname]

            # table-level
            live_table.annotations.clear()
            live_table.annotations.update(good_table.annotations)

            for live_column in live_table.column_definitions:
                cname = live_column.name
                good_column = good_table.column_definitions[cname]

                # column-level
                live_column.annotations.clear()
                live_column.annotations.update(good_column.annotations)

            for live_key in live_table.keys:
                constr_name = tuple(live_key.names[0])
                good_key = live_table.keys[constr_name]

                # key-level
                live_key.annotations.clear()
                live_key.annotations.update(good_key.annotations)

            for live_fkey in live_table.foreign_keys:
                constr_name = tuple(live_fkey.names[0])
                good_fkey = live_table.foreign_keys[constr_name]

                # fkey-level
                live_fkey.annotations.clear()
                live_fkey.annotations.update(good_fkey.annotations)

    live_config.apply(live_catalog)


def main():
    cli = BaseCLI("annotation rollback tool", None, version=__version__, hostname_required=True)
    cli.parser.add_argument("--catalog", default=1, metavar="<1>", help="Catalog number. Default: 1")
    cli.parser.add_argument("--snapshot", metavar="<snapshot ID", help="Catalog snapshot ID. Example: 2QG-VWP6-0YG0")
    args = cli.parse_cli()
    credential = get_credential(args.host, args.credential_file)
    rollback_annotation(args.host, args.catalog, snaptime=args.snapshot, credential=credential)


if __name__ == '__main__':
    sys.exit(main())
