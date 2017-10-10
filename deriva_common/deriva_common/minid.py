# !/usr/bin/python
from argparse import ArgumentParser
import minid_client.minid_client_api as mca
import requests

import deriva_common

from deriva_common import ErmrestCatalog, get_credential, urlquote
from deriva_common.utils.hash_utils import compute_hashes
from versioned_catalog import VersionedCatalog

from urllib.parse import urlparse, urlunparse
import re


# Usage
# minid.py <file_path> --- will give you info on file if it has been registered
# minid.py <identifer> -- will give you info on identifier
# minid.py <file_path> --register --title "My file"
# minid.py <identifier> --update --title "My file"  --status "TOMBSTONE" --obsoleted_by "ark://99999/abcd"
# minid.py --register_user --email "Email" --name "Name" [--orcid "orcid"]


class MINIDError(Exception):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


def create_catalog_minid(catalog, minidserver, email, code, title=None, test=False, key=None):
    """
    Create a MIND from a ERMRest catalog

    :param catalog:  VersionedCatalog object
    :param minidserver:  Host name of minid service
    :param email: EMAIL address to be used for MINID metadata
    :param code:  Authentication code for MINID serivce users
    :param title: Title of catalog MIND.  Will default to catalog URL and version if not provided
    :param test:  Use MINID test ID space
    :param key: contect key
    :return: ID of an ARK
    """

    catalog_path = catalog.path

    # Create a default title...
    if title is None:
        title = "ERMRest Catalog: " + catalog_path

    # see if this catalog or name exists
    checksum = catalog.CheckSum()
    entities = mca.get_entities(minidserver, checksum, test)

    # register file or display info about the entity
    if entities:
        raise MINIDError("MINID for this catalog version already exists: " + [x for x in entities.keys()][0])
    else:
        # register_entity wants a list of locations....
        minid = mca.register_entity(minidserver, checksum, email, code, [catalog.URL()], title, test, key)
        return minid


def update_catalog_minid(catalog, minidserver, email, code,
                         status=None, obsoleted_by=None, title=None, test=False):
    catalog_location = catalog.URL(version=version)

    # see if this catalog or name exists
    checksum = catalog.CheckSum()
    entities = mca.get_entities(minidserver, checksum, test)

    if entities is None:
        raise MINIDError('No entity found to update. You must use a valid minid.')
    elif len(entities) > 1:
        raise MINIDError("More than one minid identified. Please use a minid identifier")
    else:
        entity = entities.values()[0]

        if status is not None:
            entity['status'] = status
        if obsoleted_by is not None:
            entity['obsoleted_by'] = obsoleted_by
        if title is not None:
            entity['titles'] = [{"title": title}]
        updated_entity = mca.update_entity(minidserver, catalog_location, entity, email, code)
        return updated_entity


def lookup_catalog_minid(catalog, minidserver, test=False):
    """
    Return a list of entities by looking up values using the checksum
    :param catalog: VersionedCatalog object
    :param minidserver: Hostname of minid server
    :param test: user test MINID server
    :return:  List of entitys that match the requested catalog
    """

    checksum = catalog.CheckSum()
    entities = mca.get_entities(minidserver, checksum, test)
    return entities


def resolve_catalog_minid(ark):
    """
   Given an MIND, return the URL for the ERMREst Catalog that the MINID refers to.

    The Argument should be an ARK of the form ark:/99999/fk43n2fv37
    The ARK is looked up using N2T and resolved to a MINID landing page.
    The location link on that page is returned as a string

    """

    if not ('ark:/' == ark[:len('ark:/')]):
        raise MINIDError('MINID must be in form of ark:/99999/fk43n2fv37')

    # Make ARK resolvable by prepending with N2T
    n2t_url = 'http://n2t.net/' + ark

    # Get landing page ....
    headers = {'Accept': 'application/json'}
    r = requests.get(n2t_url, headers=headers)

    # Get the location element from the landing page. There are a list of locations....
    location = r.json()['locations'][0]

    # Return the URL to the location.
    return location['link']


def parse_cli():
    description = 'DERIVA minid tool for assigning an identifier to catalog snapshot'
    parser = ArgumentParser(description=description)

    parser.add_argument('--quiet', action="store_true", help="suppress logging output")

    # Basic modes
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--register', action="store_true", help="Register a catalog")
    group.add_argument('--update', action="store_true", help="Update a minid")
    group.add_argument('--register_user', action="store_true", help="Register a new user")
    group.add_argument('--url', action="store_true", help="Return URL of a catalog from a MINID")
    group.add_argument('--landingpage', action="store_true", help="Return the landing page info for a catalog")

    # Overrides of the config file...
    parser.add_argument('--config', default=mca.DEFAULT_CONFIG_FILE)
    parser.add_argument('--server', help="Minid server")
    parser.add_argument('--email', help="User email address")
    parser.add_argument('--name', help="User name")
    parser.add_argument('--orcid', help="user orcid")
    parser.add_argument('--code', help="user code")

    parser.add_argument('--test', action="store_true",
                        help="Run a test of this registration using the test minid namespace")
    parser.add_argument('--json', action="store_true", help="Return output as JSON")

    # MINID Metadata values....
    parser.add_argument('--title', help="Title of named catalog")
    parser.add_argument('--status', help="Status of the minid (ACTIVE or TOMBSTONE)")
    parser.add_argument('--obsoleted_by', help="A minid that replaces this minid")

    parser.add_argument('--from_minid')

    parser.add_argument('path', nargs='?', help="Hostname where catalog server is located")
    parser.add_argument('catalogid', nargs='?', help="Catalog ID number")
    parser.add_argument('version', nargs='?', help="Version number of catalog to be identified")

    return parser.parse_args()


def main():
    args = parse_cli()

    # Need to change this....
    args.scheme = 'https'

    if not (args.register or args.update or args.register_user or args.url or args.landingpage):
        args.register = True

    if not args.quiet:
        mca.configure_logging()

    config = mca.parse_config(args.config)

    email = args.email if args.email else config["email"]
    code = args.code if args.code else config["code"]
    server = args.server if args.server else config['minid_server']
    try:
        # register a new user
        if args.register_user:
            mca.register_user(server, email, config['name'], args.orcid)
            return

        if args.url:
            url = resolve_catalog_minid(args.path)
            print(url)
            return

        if args.landingpage:
            if args.path[0:len('ark:')] == 'ark:':
                entities = mca.get_entities(server, args.path, args.test)
            else:
                vc = VersionedCatalog(args.url, args.version)
                entities = lookup_catalog_minid(vc, server, test=args.test)
            if entities is not None:
                mca.print_entities(entities, args.json)
            else:
                print("Catalog is not named. Use --register to create a name for this file.")
            return

        if args.path is None:
            print("Catalog path must be specified")
            return

        vc = VersionedCatalog(args.path, args.version)

        # register file or display info about the entity
        if args.register:
            create_catalog_minid(vc, server, email, code, title=args.title, test=args.test)
        elif args.update:
            update_catalog_minid(vc, server, email, code,
                                 title=args.title, status=args.status, obsoleted_by=args.obsoleted_by, test=args.test)
    except MINIDError as err:
        print('ERROR: ' + err.message)
    except requests.exceptions.HTTPError as err:
        print(err)


if __name__ == '__main__':
    main()
