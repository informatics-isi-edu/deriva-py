from .deriva_binding import DerivaBinding
from .ermrest_catalog import ErmrestCatalog, ErmrestSnapshot


class DerivaServer (DerivaBinding):
    """Persistent handle for a Deriva server."""

    def __init__(self, scheme, server, credentials=None, caching=True, session_config=None):
        """Create a Deriva server binding.

           Arguments:
             scheme: 'http' or 'https'
             server: server FQDN string
             credentials: credential secrets, e.g. cookie
             caching: whether to retain a GET response cache
        """
        super(DerivaServer, self).__init__(scheme, server, credentials, caching, session_config)
        self.scheme = scheme
        self.server = server
        self.credentials = credentials
        self.caching = caching
        self.session_config = session_config

    def connect_ermrest(self, catalog_id, snaptime=None):
        """Connect to an ERMrest catalog.

           Arguments:
             catalog_id: e.g., '1' or '1@2PM-DGYP-56Z4'
             snaptime: e.g., '2PM-DGYP-56Z4' (optional)
        """
        if not snaptime:
            splits = str(catalog_id).split('@')
            if len(splits) > 2:
                raise Exception('Malformed catalog identifier: multiple "@" characters found.')
            catalog_id = splits[0]
            snaptime = splits[1] if len(splits) == 2 else None

        if snaptime:
            return ErmrestSnapshot(self.scheme, self.server, catalog_id, snaptime, self.credentials, self.caching, self.session_config)

        return ErmrestCatalog(self.scheme, self.server, catalog_id, self.credentials, self.caching, self.session_config)

    def create_ermrest_catalog(self):
        """Create an ERMrest catalog.
        """
        path = '/ermrest/catalog'
        r = self.post(path)
        r.raise_for_status()
        return self.connect_ermrest(r.json()['id'])
