from deriva.core import BaseCLI, __version__
from deriva.seo import SitemapBuilder
import sys
import json
import logging

def main():
    logger = logging.getLogger(__name__)
    logger.setLevel("WARNING")
    cli = BaseCLI(__name__,
                  "Create a sitemap from a table specified on the command line or a set of tables from a config file",
                  __version__, hostname_required=True)
    cli.remove_options(["--credential-file", "--token", "--oauth2-token"])
    cli.parser.add_argument("--catalog", default=1, metavar="<1>", help="Catalog number")
    cli.parser.add_argument("-p", "--priority", type=float,
                            help="A floating-point number between 0.0 and 1.0 indicating the table's priority")
    cli.parser.add_argument("-s", "--schema", help="the name of the schema of the (single) table to include")
    cli.parser.add_argument("-t", "--table", help="the name of the (single) table to include")
    args = cli.parse_cli()

    if args.priority is not None:
        if args.priority < 0 or args.priority > 1:
            logger.error("priority should be a floating-point number between 0 and 1")
            sys.exit(1)

    if not ((args.schema and args.table) or args.config_file):
        logger.error("must specify either a schema and table or a config file")
        sys.exit(1)

    sb = SitemapBuilder("https", args.host, args.catalog)
    if args.schema and args.table:
        sb.add_table_spec(args.schema, args.table, priority=args.priority)
    if args.config_file:
        rows = json.load(open(args.config_file))
        for row in rows:
            if row.get("schema") is None or row.get("table") is None:
                logger.warning("malformed entry in {f}: schema or table is missing. Skipping".format(f=args.config_file))
                next
            if row.get("priority") is None:
                priority = args.priority
            else:
                try:
                    priority = float(row.get("priority"))
                    if priority < 0 or priority > 1:
                        logger.warning("bad priority '{p}' - should be a floating-point number between 0 and 1. Ignoring".format(p=priority))
                        priority = args.priority
                except ValueError:
                    logger.warning("malformed priority '{p}' - should be a floating-point number between 0 and 1. Ignoring".format(p=row.get("priority")))
                    priority = args.priority

            sb.add_table_spec(row["schema"], row["table"], priority=priority)
    sb.write_sitemap(sys.stdout)
    return 0

if __name__ == '__main__':
    sys.exit(main())
