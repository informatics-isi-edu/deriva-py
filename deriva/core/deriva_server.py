from .deriva_binding import DerivaBinding
from .ermrest_catalog import ErmrestCatalog


class DerivaServer (DerivaBinding):
    """Represents a handle to a Deriva server."""

    def __init__(self, scheme, server, credentials=None, caching=True, session_config=None):
        """Instantiates a DerivaServer.
        """
        super(DerivaServer, self).__init__(scheme, server, credentials, caching, session_config)
        self.scheme = scheme
        self.server = server
        self.credentials = credentials
        self.caching = caching
        self.session_config = session_config

    def connect_ermrest(self, catalog_id):
        """Connect to an ERMrest catalog.
        """
        return ErmrestCatalog(self.scheme, self.server, catalog_id, self.credentials, self.caching, self.session_config)

    def create_ermrest_catalog(self):
        """Create an ERMrest catalog.
        """
        path = '/ermrest/catalog'
        r = self.post(path)
        r.raise_for_status()
        return self.connect_ermrest(r.json()['id'])
