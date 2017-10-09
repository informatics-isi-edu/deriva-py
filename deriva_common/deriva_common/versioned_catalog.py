from deriva_common import ErmrestCatalog, get_credential, urlquote
from deriva_common.utils.hash_utils import compute_hashes

from urllib.parse import urlparse, urlunparse
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
    def __init__(self, url, host=None, id=None, version=None):

        if url == 'http' or url == 'https':
            if id is None:
                raise VersionedCatalogError('Catalog ID number required')
            if host is None:
                raise VersionedCatalogError('ERMRest host name required')
            if not str(id).isdigit():
                raise VersionedCatalogError('Catalog ID must be an integer')

            url = '%s://%s/ermrest/catalog/%s' % (url, host, id)

        self.ParseCatalog(url, version)

    def ParseCatalogURL(self, url, version=None):
        """
        Parse a URL to an ermrest catalog, defaulting the version to the current version if not provided

        :param url: catalog URL

        """
        urlparts = urlparse(url)

        self.scheme = urlparts.scheme
        self.host = urlparts.netloc,
        self.params = urlparts.params
        self.query = urlparts.query
        self.fragment = urlparts.fragment

        catparts = re.match(r'/ermrest/catalog/(?P<id>\d+)(@(?P<version>[^/]+))?(?P<path>.*)', urlparts.path)

        if catparts is None:
            raise VersionedCatalogError("Ill formed catalog URL: " + url)

        self.id, self.version, self.path = catparts.group('id', 'version', 'path')

        # If there was no version in the URL, either use provided version, or current version.
        if self.version is None:
            credential = get_credential(self.host)
            catalog = ErmrestCatalog(self.scheme, self.host, self.id, credentials=credential)

            # Get current version of catalog and construct a new URL that fully qualifies catalog with version.
            self.version = catalog.get('/').json()['version']


    def URL(self):
        versioned_path = urlquote('/ermrest/catalog/%s@%s%s' % (self.id, self.version, self.path))

        #  Ermrest bug on quoting @?
        versioned_path = str.replace(versioned_path, '%40', '@')
        url = urlunparse([scheme, host, versioned_path, params, query, fragment])
        return url


    def CheckSum(self, hashalg='sha256'):
        """
        """
        fd = io.BytesIO(self.URL().encode())

        # Get back a dictionary of hash codes....
        hashcodes = compute_hashes(fd, [hashalg])
        return hashcodes[hashalg][0]