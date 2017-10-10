from deriva_common import ErmrestCatalog, get_credential, urlquote
from deriva_common.utils.hash_utils import compute_hashes

from urllib.parse import urlsplit, urlunsplit
import re
import io


class VersionedCatalogError(Exception):
        """Exception raised for errors in the input.

        Attributes:
            expression -- input expression in which the error occurred
            message -- explanation of the error
        """

        def __init__(self, message):
            self.message = message


class VersionedCatalog:

    def __init__(self, url, host=None, cid=None, version=None):

        # Initialize everything to None.
        self.scheme = self.host = self.query = self.fragment = self.id = self.version = self.path = None

        # Figure out if we have a scheme/host/id triple, or a real URL.
        if url == 'http' or url == 'https':
            if host is None:
                raise VersionedCatalogError('ERMRest host name required')
            if cid is None:
                raise VersionedCatalogError('Catalog ID number required')
            if not str(id).isdigit():
                raise VersionedCatalogError('Catalog ID must be an integer')

            url = '%s://%s/ermrest/catalog/%s' % (url, host, id)

        vc = self.ParseCatalogURL(url, version)

        self.scheme = vc['scheme']
        self.host = vc['host']
        self.query = vc['query']
        self.fragment = vc['fragment']
        self.id = vc['id']
        self.version = vc['version']
        self.path = vc['path']

    def ParseCatalogURL(self, url, version=None):
        """
        Parse a URL to an ermrest catalog, defaulting the version to the current version if not provided

        :param url: catalog URL

        """
        urlparts = urlsplit(url, scheme='https')

        scheme = urlparts.scheme
        host = urlparts.netloc
        query = urlparts.query
        fragment = urlparts.fragment

        # Look in the path and pull out id, version number if it exists, and ermrest path....
        catparts = re.match(r'(/ermrest/catalog/(?P<id>\d+)(@(?P<version>[^/]+))?)?(?P<path>.*)', urlparts.path)

        cid, version, path = catparts.group('id', 'version', 'path')

        # fill in missing values ....
        scheme = scheme if scheme is not None else self.scheme
        host = host if host is not None else self.host
        cid = cid if cid is not None else self.id
        version = version if version is not None else self.version

        # If there was no version in the URL, either use provided version, or current version.
        if version is None:
            credential = get_credential(host)
            catalog = ErmrestCatalog(scheme, host, cid, credentials=credential)

            # Get current version of catalog and construct a new URL that fully qualifies catalog with version.
            version = catalog.get('/').json()['version']

        return {'scheme': scheme, 'host': host, 'query': query, 'fragment': fragment,
                'id': cid, 'version': version, 'path': path}

    def URL(self, path=None, version=None):

        # Use path if it is provided as an argument.
        path = self.path if path is None else path

        vc = self.ParseCatalogURL(path, version)

        if len(vc['path']) > 0 and vc['path'][0] != '/':
            vc['path'] = '/' + vc['path']

        versioned_path = urlquote('/ermrest/catalog/%s' % (vc['id'])) + '@' \
                                  + urlquote('%s%s' % (vc['version'], vc['path']))

        url = urlunsplit([vc['scheme'], vc['host'], versioned_path, vc['query'], vc['fragment']])
        return url

    def CheckSum(self, path=None, version=None, hashalg='sha256'):
        """
        """
        # Use path if it is provided as an argument.
        path = self.path if path is None else path

        print('checksum path is: ' + path)
        fd = io.BytesIO(self.URL(path, version).encode())

        # Get back a dictionary of hash codes....
        hashcodes = compute_hashes(fd, [hashalg])
        return hashcodes[hashalg][0]
