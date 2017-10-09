# !/usr/bin/python

from argparse import ArgumentParser
import minid_client.minid_client_api as mca
import requests
import io

from deriva_common import ErmrestCatalog, get_credential, urlquote
from deriva_common.utils.hash_utils import compute_hashes

from urllib.parse import urlparse, urlunparse


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


def catalog_from_minid(ark):
    """Given an MIND, return the URL for the ERMREst Catalog that the MINID refers to.

    The Argument should be an ARK of the form ark:/99999/fk43n2fv37
    The ARK is looked up using N2T and resolved to a MINID landing page.
    The location link on that page is returned as a string

    """

    assert ('ark:/' == ark[:len('ark:/')])

    # Make ARK resolvable by prepending with N2T
    n2t_url = 'http://n2t.net/' + ark

    # Get landing page ....
    headers = {'Accept': 'application/json'}
    r = requests.get(n2t_url, headers=headers)

    # Get the location element from the landing page. There are a list of locations....
    location = r.json()['locations'][0]

    # Return the URL to the location.
    return location['link']


def compute_catalog_checksum(caturl, hashalg='sha256'):
    """

    :rtype: basestring
    """
    fd = io.BytesIO(caturl.encode())

    # Get back a dictionary of hash codes....
    hashcodes = compute_hashes(fd, [hashalg])
    return hashcodes[hashalg][0]


def get_catalog_url(scheme, ermresthost, catalog_id, catalog_version=None):
    """
    :param scheme:  scheme used for catalog URL
    :param ermresthost:  hostname of ERMRest catalog service
    :param catalog_id:  Integer ID number of catalog
    :param catalog_version: Version of the catalog to use
    :rtype: URL with catalog and version included.
    """
    assert (scheme is not None and ermresthost is not None and catalog_id is not None)
    credential = get_credential(ermresthost)
    catalog = ErmrestCatalog(scheme, ermresthost, catalog_id, credentials=credential)

    # Get current version of catalog and construct a new URL that fully qualifies catalog with version.
    catalog_url = urlparse(catalog._server_uri)
    catalog_version = catalog_version if catalog_version is not None else catalog.get('/').json()['version']
    catalog_location = urlunparse([catalog_url.scheme, catalog_url.hostname,
                                   urlquote(catalog_url.path + '@' + catalog_version),
                                   '', '', ''])

    #  Ermrest bug on quoting @?
    catalog_location = str.replace(catalog_location, '%40', '@')
    return catalog_location


def minid_from_catalog(scheme, ermresthost, catalog_id, minidserver, email, code,
                       title=None, version=None, test=False, key=None):
    """
    Create a MIND from a ERMRest catalog

    :param scheme:  scheme used for catalog URL
    :param ermresthost:  hostname of ERMRest catalog service
    :param catalog_id:  Integer ID number of catalog
    :param minidserver:  Host name of minid service
    :param email: EMAIL address to be used for MINID metadata
    :param code:  Authentication code for MINID serivce users
    :param title: Title of catalog MIND.  Will default to catalog URL and version if not provided
    :param version: Version string of catalog to use. Defaults to current version if not provided
    :param test:  Use MINID test ID space
    :param key: contect key
    :return: ID of an ARK
    """

    catalog_location = get_catalog_url(scheme, ermresthost, catalog_id, version)
    catalog_path = urlparse(catalog_location).path

    # Create a default title...
    if title is None:
        title = "ERMRest Catalog: " + catalog_path

    # see if this catalog or name exists
    checksum = compute_catalog_checksum(catalog_location)
    entities = mca.get_entities(minidserver, checksum, test)

    # register file or display info about the entity
    if entities:
        raise MINIDError("MINID for this catalog version already exists: " + [x for x in entities.keys()][0])
    else:
        # register_entity wants a list of locations....
        minid = mca.register_entity(minidserver, checksum, email, code, [catalog_location], title, test, key)
        return minid


def update_catalog_minid(scheme, ermresthost, catalog_id, minidserver, email, code,
                         status=None, obsoleted_by=None, title=None, version=None, test=False):
    catalog_location = get_catalog_url(scheme, ermresthost, catalog_id, version)

    # see if this catalog or name exists
    checksum = compute_catalog_checksum(catalog_location)
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


def get_catalog_minid(scheme, ermresthost, catalog_id, minidserver, version=None, test=False):
    # Need to use Deriva authentication agent before executing this

    catalog_location = get_catalog_url(scheme, ermresthost, catalog_id, version)

    # see if this catalog or name exists
    checksum = compute_catalog_checksum(catalog_location)
    entities = mca.get_entities(minidserver, checksum, test)
    return entities


def parse_cli():
    description = 'DERIVA minid tool for assigning an identifier to catalog snapshot'
    parser = ArgumentParser(description=description)

    parser.add_argument('--quiet', action="store_true", help="suppress logging output")

    # Basic modes
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--register', action="store_true", help="Register a catalog")
    group.add_argument('--update', action="store_true", help="Update a minid")
    group.add_argument('--register_user', action="store_true", help="Register a new user")
    group.add_argument('--get', action="store_true", help="Return the ID for a catalog")

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

    parser.add_argument('ermresthost', nargs='?', help="Hostname where catalog server is located")
    parser.add_argument('catalogid', nargs='?', type=int, help="Catalog ID number")
    parser.add_argument('version', nargs='?', help="Version number of catalog to be identified")

    return parser.parse_args()


def main():
    args = parse_cli()

    # Need to change this....
    args.scheme = 'https'

    if not (args.register or args.update or args.register_user or args.get):
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

        # if we got this far we *must* have a catalog (or identifier) arg
        if args.ermresthost is None or args.catalogid is None:
            print("A catalog and an identifier must be specified.")
            return

        # register file or display info about the entity
        if args.register:
            minid_from_catalog(args.scheme, args.ermresthost, args.catalogid, server, email, code,
                               title=args.title, version=args.version, test=args.test)
        elif args.update:
            update_catalog_minid(args.scheme, args.ermresthost, args.catalogid, server, email, code,
                                 title=args.title, version=args.version,
                                 status=args.status, obsoleted_by=args.obsoleted_by, test=args.test)
        else:
            entities = get_catalog_minid(args.scheme, args.ermresthost, args.catalogid, server, test=args.test)
            if entities is not None:
                mca.print_entities(entities, args.json)
            else:
                print("Catalog is not named. Use --register to create a name for this file.")
    except MINIDError as err:
        print('ERROR: ' + err.message)


if __name__ == '__main__':
    main()
