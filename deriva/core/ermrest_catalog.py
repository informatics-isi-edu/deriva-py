import os
import io
import re
import logging
import datetime
import codecs
import csv
import json
import requests
from typing import NamedTuple

from . import urlquote, urlsplit, urlunsplit, datapath, DEFAULT_HEADERS, DEFAULT_CHUNK_SIZE, DEFAULT_SESSION_CONFIG, \
    Megabyte, Kilobyte, get_transfer_summary
from .deriva_binding import DerivaBinding, DerivaPathError
from . import ermrest_model
from .ermrest_model import nochange

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
        """Connect to an ERMrest catalog and return the catalog binding.

        :param catalog_id: The id (or alias) of the existing catalog
        :param snaptime: The id for a desired catalog snapshot (default None)

        The catalog_id is normally a bare id (str), and the optional
        snaptime is a bare snapshot id (str). If the snaptime is None,
        the catalog_id may be a concatenated <id>@<snaptime> string,
        and it will be split to determine the snaptime.

        If no snaptime is passed separately or compounded with
        catalog_id, an ErmrestCatalog binding will be
        returned. Conversely, if a snaptime is determined, an
        ErmrestSnapshot (immutable) binding will be returned.

        """
        return ErmrestCatalog.connect(self, catalog_id, snaptime)

    def create_ermrest_catalog(self, id=None, owner=None, name=None, description=None, is_persistent=None, clone_source=None):
        """Create an ERMrest catalog.

        :param id: The (str) id desired by the client (default None)
        :param owner: The initial (list of str) ACL desired by the client (default None)
        :param name: Initial (str) catalog name if not None
        :param description: Initial (str) catalog description if not None
        :param is_persistent: Initial (bool) catalog persistence flag if not None
        :param clone_source: Initial catalog clone_source if not None

        The new catalog id will be returned in the response, and used
        in future catalog access. The use of the id parameter
        may yield errors if the supplied value is not available for
        use by the client. The value None will result in a
        server-assigned catalog id.

        The initial "owner" ACL on the new catalog will be the
        client-supplied owner if provided. The use of owner parameter
        may yield errors if the supplied ACL does not match the
        client, i.e. the client cannot lock themselves out of the
        catalog. The value None will result in a server-assigned ACL
        with the requesting client's identity.

        Certain failure modes (or message loss) may leave the
        id reserved in the system. In this case, the effective
        owner ACL influences which client(s) are allowed to retry
        creation with the same id.

        The name, description, is_persistent, and clone_source
        parameters are passed through to the catalog creation service
        to initialize those respective metadata fields of the new
        catalog's registry entry. See ERMrest documentation for more
        detail. Authorization failures may occur when attempting to
        set the is_persistent flag. By default, these fields are not
        initialized in the catalog creation request, and they instead
        receive server-assigned defaults.

        """
        return ErmrestCatalog.create(self, id, owner, name, description, is_persistent, clone_source)

    def connect_ermrest_alias(self, id):
        """Connect to an ERMrest alias and return the alias binding.

        :param id: The id of the existing alias

        """
        return ErmrestAlias.connect(self, id)

    def create_ermrest_alias(self, id=None, owner=None, alias_target=None, name=None, description=None):
        """Create an ERMrest catalog alias.

        :param id: The (str) id desired by the client (default None)
        :param owner: The initial (list of str) ACL desired by the client (default None)
        :param alias_target: The initial target catalog id binding desired by the client (default None)
        :param name: Initial (str) catalog name if not None
        :param description: Initial (str) catalog description if not None

        The new alias id will be returned in the response, and used
        in future alias access. The use of the id parameter
        may yield errors if the supplied value is not available for
        use by the client. The value None will result in a
        server-assigned alias id.

        The initial "owner" ACL on the new alias will be the
        client-supplied owner. The use of owner parameter may yield
        errors if the supplied ACL does not match the client, i.e. the
        client cannot lock themselves out of the alias. The value
        None will result in a server-assigned ACL with the requesting
        client's identity.

        The alias is bound to the client-supplied alias_target, if
        supplied. The use of alias_target may yield errors if the
        supplied value is not a valid target catalog id. The value
        None will reserve the alias in an unbound state.

        Certain failure modes (or message loss) may leave the id
        reserved in the system. In this case, the effective owner_acl
        influences which client(s) are allowed to retry creation with
        the same id.

        The name and description parameters are passed through to the
        alias creation service to initialize those respective metadata
        fields of the new aliase's registry entry. See ERMrest
        documentation for more detail.

        """
        return ErmrestAlias.create(self, id, owner, alias_target, name, description)

class ErmrestCatalogMutationError(Exception):
    pass


_clone_state_url = "tag:isrd.isi.edu,2018:clone-status"

DEFAULT_PAGE_SIZE = 100000

class ResolveRidResult (NamedTuple):
    datapath: datapath.DataPath
    table: ermrest_model.Table
    rid: str


class ErmrestCatalog(DerivaBinding):
    """Persistent handle for an ERMrest catalog.

       Provides basic REST client for HTTP methods on arbitrary
       paths. Caller has to understand ERMrest APIs and compose
       appropriate paths, headers, and/or content.

       Additional utility methods provided for accessing catalog metadata.
    """
    table_schemas = dict()

    @property
    def deriva_server(self):
        """Return DerivaServer binding for the same server this catalog belongs to."""
        return DerivaServer(
            self._scheme,
            self._server,
            self._credentials,
            self._caching,
            self._session_config,
        )

    @classmethod
    def connect(cls, deriva_server, catalog_id, snaptime=None):
        """Connect to an ERMrest catalog and return the catalog binding.

        :param deriva_server: The DerivaServer binding which hosts ermrest
        :param catalog_id: The id (or alias) of the existing catalog
        :param snaptime: The id for a desired catalog snapshot (default None)

        The catalog_id is normally a bare id (str), and the optional
        snaptime is a bare snapshot id (str). If the snaptime is None,
        the catalog_id may be a concatenated <id>@<snaptime> string,
        and it will be split to determine the snaptime.

        If no snaptime is passed separately or compounded with
        catalog_id, an ErmrestCatalog binding will be
        returned. Conversely, if a snaptime is determined, an
        ErmrestSnapshot (immutable) binding will be returned.

        """
        if not snaptime:
            splits = str(catalog_id).split('@')
            if len(splits) > 2:
                raise Exception('Malformed catalog identifier: multiple "@" characters found.')
            catalog_id = splits[0]
            snaptime = splits[1] if len(splits) == 2 else None

        if snaptime:
            return ErmrestSnapshot(
                deriva_server.scheme,
                deriva_server.server,
                catalog_id,
                snaptime,
                deriva_server.credentials,
                deriva_server.caching,
                deriva_server.session_config
            )

        return cls(
            deriva_server.scheme,
            deriva_server.server,
            catalog_id,
            deriva_server.credentials,
            deriva_server.caching,
            deriva_server.session_config
        )

    @classmethod
    def _digest_catalog_args(cls, id, owner, name=None, description=None, is_persistent=None, clone_source=None):
        rep = dict()

        for v, k, typ in [
                (id, 'id', str),
                (name, 'name', str),
                (description, 'description', str),
                (is_persistent, 'is_persistent', bool),
                (clone_source, 'clone_source', str),
        ]:
            if isinstance(v, typ):
                rep[k] = v
            elif isinstance(v, (type(nochange), type(None))):
                pass
            else:
                raise TypeError('%s must be of type %s or None or nochange, not %s' % (k, typ.__name__, type(v)))

        if isinstance(owner, list):
            for e in owner:
                if not isinstance(e, str):
                    raise TypeError('owner members must be of type str, not %s' % type(e))
            rep['owner'] = owner
        elif isinstance(owner, (type(nochange), type(None))):
            pass
        else:
            raise TypeError('owner must be of type list or None or nochange, not %s' % type(owner))

        return rep

    @classmethod
    def create(cls, deriva_server, id=None, owner=None, name=None, description=None, is_persistent=None, clone_source=None):
        """Create an ERMrest catalog and return the ERMrest catalog binding.

        :param deriva_server: The DerivaServer binding which hosts ermrest.
        :param id: The (str) id desired by the client (default None)
        :param owner: The initial (list of str) ACL desired by the client (default None)
        :param name: Initial (str) catalog name if not None
        :param description: Initial (str) catalog description if not None
        :param is_persistent: Initial (bool) catalog persistence flag if not None
        :param clone_source: Initial catalog clone_source if not None

        The new catalog id will be returned in the response, and used
        in future catalog access. The use of the id parameter
        may yield errors if the supplied value is not available for
        use by the client. The value None will result in a
        server-assigned catalog id.

        The initial "owner" ACL on the new catalog will be the
        client-supplied owner ACL. The use of owner parameter
        may yield errors if the supplied ACL does not match the
        client, i.e. the client cannot lock themselves out of the
        catalog. The value None will result in a server-assigned ACL
        with the requesting client's identity.

        Certain failure modes (or message loss) may leave the id
        reserved in the system. In this case, the effective owner ACL
        influences which client(s) are allowed to retry creation with
        the same id.

        The name, description, is_persistent, and clone_source
        parameters are passed through to the catalog creation service
        to initialize those respective metadata fields of the new
        catalog's registry entry. See ERMrest documentation for more
        detail. Authorization failures may occur when attempting to
        set the is_persistent flag. By default, these fields are not
        initialized in the catalog creation request, and they instead
        receive server-assigned defaults.

        """
        path = '/ermrest/catalog'
        r = deriva_server.post(path, json=cls._digest_catalog_args(id, owner, name, description, is_persistent, clone_source))
        r.raise_for_status()
        return cls.connect(deriva_server, r.json()['id'])

    def __init__(self, scheme, server, catalog_id, credentials=None, caching=True, session_config=None):
        """Create ERMrest catalog binding.

           Arguments:
             scheme: 'http' or 'https'
             server: server FQDN string
             catalog_id: e.g. '1'
             credentials: credential secrets, e.g. cookie
             caching: whether to retain a GET response cache

           Deriva Client Context: You MAY mutate self.dcctx to
           customize the context for this service endpoint prior to
           invoking web requests.  E.g.:

             self.dcctx['cid'] = 'my application name'

           You MAY also supply custom per-request context by passing a
           headers dict to web request methods, e.g.

             self.get(..., headers={'deriva-client-context': {'action': 'myapp/function1'}})

           This custom header will be merged as override values with
           the default context in self.dcctx in order to form the
           complete context for the request.
        """
        super(ErmrestCatalog, self).__init__(scheme, server, credentials, caching, session_config)
        if isinstance(catalog_id, int):
            catalog_id = str(catalog_id)
        self._server_uri = "%s/ermrest/catalog/%s" % (
            self._server_uri,
            urlquote(catalog_id),
        )
        self._scheme, self._server, self._catalog_id, self._credentials, self._caching, self._session_config = \
            scheme, server, catalog_id, credentials, caching, session_config

    @property
    def catalog_id(self):
        return self._catalog_id

    @property
    def alias_target(self):
        r = self.get('/')
        r.raise_for_status()
        rep = r.json()
        return rep.get('alias_target')

    def exists(self):
        """Simple boolean test for catalog existence.

        :return: True if exists, False if not (404), otherwise raises exception
        """
        try:
            self.get('/')
            return True
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return False
            else:
                raise

    def latest_snapshot(self):
        """Gets a handle to this catalog's latest snapshot.
        """
        r = self.get('/')
        r.raise_for_status()
        return ErmrestSnapshot(self._scheme, self._server, self._catalog_id, r.json()['snaptime'],
                               self._credentials, self._caching, self._session_config)

    def getCatalogModel(self):
        return ermrest_model.Model.fromcatalog(self)

    def getCatalogSchema(self):
        path = '/schema'
        r = self.get(path)
        r.raise_for_status()
        return r.json()

    def getPathBuilder(self):
        """Returns the 'path builder' interface for this catalog."""
        return datapath.from_catalog(self)

    def getTableSchema(self, fq_table_name):
        # first try to get from cache(s)
        s, t = self.splitQualifiedCatalogName(fq_table_name)
        cat = self.getCatalogSchema()
        schema = cat['schemas'][s]['tables'][t] if cat else None
        if schema:
            return schema
        schema = self.table_schemas.get(fq_table_name)
        if schema:
            return schema

        path = '/schema/%s/table/%s' % (s, t)
        r = self.get(path)
        resp = r.json()
        self.table_schemas[fq_table_name] = resp
        r.raise_for_status()

        return resp

    def getTableColumns(self, fq_table_name):
        columns = set()
        schema = self.getTableSchema(fq_table_name)
        for column in schema['column_definitions']:
            columns.add(column['name'])

        return columns

    def validateRowColumns(self, row, fq_tableName):
        columns = self.getTableColumns(fq_tableName)
        return set(row.keys()) - columns

    def getDefaultColumns(self, row, table, exclude=None, quote_url=True):
        columns = self.getTableColumns(table)
        if isinstance(exclude, list):
            for col in exclude:
                columns.remove(col)

        defaults = []
        supplied_columns = row.keys()
        for col in columns:
            if col not in supplied_columns:
                defaults.append(urlquote(col, safe='') if quote_url else col)

        return defaults

    @staticmethod
    def splitQualifiedCatalogName(name):
        entity = name.split(':')
        if len(entity) != 2:
            logging.debug("Unable to tokenize %s into a fully qualified <schema:table> name." % name)
            return None
        return entity[0], entity[1]

    def resolve_rid(self, rid: str, model: ermrest_model.Model=None, builder: datapath._CatalogWrapper=None) -> ResolveRidResult:
        """Resolve a RID value to return a ResolveRidResult (a named tuple).

        :param rid: The RID (str) to resolve
        :param model: A result from self.getCatalogModel() to reuse
        :param builder: A result from self.getPathBuilder() to reuse

        Raises KeyError if RID is not found in the catalog.

        The elements of the ResolveRidResult namedtuple provide more
        information about the entity identified by the supplied RID in
        this catalog:

        - datapath: datapath instance for querying the resolved entity
        - table: ermrest_model.Table instance containing the entity
        - rid: normalized version of the input RID value

        Example to simply retrieve entity content:

           path, _, _ = catalog.resolve_rid('1-0000')
           data = path.entities().fetch()[0]

        """
        if model is None:
            model = self.getCatalogModel()
        if builder is None:
            builder = self.getPathBuilder()
        try:
            r = self.get('/entity_rid/%s' % urlquote(rid))
            info = r.json()
            sname = info['schema_name']
            tname = info['table_name']
            rid = info['RID']

            ptable = builder.schemas[sname].tables[tname]

            return ResolveRidResult(
                ptable.path.filter(ptable.RID == rid),
                model.schemas[sname].tables[tname],
                rid
            )
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == requests.codes.not_found:
                raise KeyError(rid)
            raise

    def getAsFile(self,
                  path,
                  destfilename,
                  headers=DEFAULT_HEADERS,
                  callback=None,
                  delete_if_empty=False,
                  paged=False,
                  page_size=DEFAULT_PAGE_SIZE,
                  page_sort_columns=frozenset(["RID"])):
        """
           Retrieve catalog data streamed to destination file.
           Caller is responsible to clean up file even on error, when the file may or may not exist.
           If "delete_if_empty" is True, the file will be inspected for "empty" content. In the case of
           json/json-stream content, the presence of a single empty JSON object will be tested for. In the case of
           CSV content, the file will be parsed with CSV reader to determine that only a single header line and no row
           data is present.
        """
        self.check_path(path)

        # Only entity API supported with paged mode at this time, otherwise fallback. We fallback rather than raise an
        # exception in the case that the caller might be trying to perform an opportunistic paged request without
        # knowing a priori if paged support for the given query is available.
        page_size = page_size if page_size > 0 else DEFAULT_PAGE_SIZE
        if not (path.startswith("/entity") or path.startswith("/attribute")) and paged:
            logging.warning("Paged data retrieval only supported for entity or attribute API queries.")
            paged = False

        # Only "application/x-json-stream" or "text/csv" supported with paged mode at this time, otherwise fallback.
        accept = headers.get("accept")
        if accept not in ("application/x-json-stream", "text/csv"):
            logging.debug("Paged data retrieval not supported for content type: %s" % accept)
            paged = False

        headers = headers.copy()

        destfile = open(destfilename, 'w+b')

        try:
            total = 0
            start = datetime.datetime.now()

            if not paged:
                with self._session.get(self._server_uri + path, headers=headers, stream=True) as r:
                    self._response_raise_for_status(r)
                    content_type = r.headers.get("Content-Type")
                    logging.debug("Transferring file %s to %s" % (self._server_uri + path, destfilename))
                    for buf in r.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                        destfile.write(buf)
                        total += len(buf)
                        if callback:
                            if not callback(progress="Downloading: %.2f MB transferred" %
                                                     (float(total) / float(Megabyte))):
                                destfile.close()
                                return
                destfile.flush()
            else:
                first_page = True
                first_line = None
                last_record = None
                usr = urlsplit(self._server_uri + path)
                path = str(usr.path.split('@sort')[0])
                while True:
                    sort = "@sort(%s)%s" % (",".join(page_sort_columns or ["RID"]),
                                            ("@after(%s)" % ",".join(last_record)) if last_record is not None else "")
                    limit = "limit=%s" % int(page_size) if page_size > 0 else "none"
                    query = re.sub(r"([^.]*)(limit=.*?)($|[&;])([^.]*)$", r"\1%s\3\4" % limit, usr.query, flags=re.I)
                    url = urlunsplit((usr.scheme, usr.netloc, path + sort, query if query else limit, usr.fragment))

                    # 1. Try to get a page worth of data, back-off page size if query run time errors are encountered
                    with self._session.get(url, headers=headers) as r:
                        if r.status_code == 400 and "Query run time limit exceeded" in r.text:
                            if page_size == 1:
                                self._response_raise_for_status(r)
                            r.close()
                            page_size //= 2
                            page_size = 1 if page_size < 1 else page_size
                            logging.warning("Query runtime exceeded while attempting to transfer rows from %s to file "
                                            "[%s]. The page size is being reduced to %s and the query will be retried."
                                            % (url, destfilename, page_size))
                            if callback:
                                if not callback(progress="Retrying query: %s" % url):
                                    destfile.close()
                                    return
                            continue
                        else:
                            self._response_raise_for_status(r)

                        # 2. Write the page to disk and check the last record processed in order to get the next page
                        last_line = {}
                        content_type = r.headers.get("Content-Type")
                        logging.debug("Transferring data from [%s] to %s" % (url, destfilename))
                        # CSV processing iterates over lines in the response, skipping the header line(s) in all but
                        # the first page, and captures the last line of each page to determine the last record processed
                        if content_type == "text/csv":
                            skip = 1
                            line_num = 0
                            if first_page:
                                lines = r.iter_lines(decode_unicode=True)
                                reader = csv.reader(lines)
                                first_line = next(reader)
                                skip = reader.line_num
                            for line in r.iter_lines():
                                if not first_page:
                                    line_num += 1
                                    if line_num <= skip:
                                        continue
                                tline = line + b"\n"
                                destfile.write(tline)
                                total += len(tline)
                                last_line = tline
                            if last_line and last_line != first_line:
                                reader = csv.DictReader([last_line.decode('utf-8')], first_line)
                                last_line = next(reader)
                            first_page = False
                        # JSON-Stream processing writes the entire buffer to the destination file. The last line is
                        # captured by reverse seeking in the buffer from right before the last b'\n' newline to the next
                        # newline or buf[0], then calling readline from the current position
                        elif content_type == "application/x-json-stream":
                            buf = r.content
                            if not buf:
                                break
                            destfile.write(buf)
                            total += len(buf)
                            b = io.BytesIO(buf)
                            b.seek(-2, os.SEEK_END)
                            while b.read(1) != b'\n':
                                b.seek(-2, os.SEEK_CUR)
                                if b.tell() == os.SEEK_SET:
                                    break
                            last_line = json.loads(b.readline().decode('utf-8'))

                        # 3. Save the last record key and flush the destination file buffers to disk.
                        if not last_line:
                            break
                        destfile.flush()
                        last_record = [urlquote(str(last_line.get(key))) for key in page_sort_columns]
                        if callback:
                            if not callback(progress="Downloading: %.2f MB transferred" %
                                                     (float(total) / float(Megabyte))):
                                destfile.close()
                                return

            elapsed = datetime.datetime.now() - start
            summary = get_transfer_summary(total, elapsed)

            # perform automatic file deletion on detected "empty" content, if requested
            delete_file = True if total == 0 else False
            if delete_if_empty and total > 0:
                destfile.seek(0)
                if content_type == "application/json" or content_type == "application/x-json-stream":
                    buf = destfile.read(16)
                    if buf == b"[]\n" or buf == b"{}\n":
                        delete_file = True
                elif content_type == "text/csv":
                    reader = csv.reader(codecs.iterdecode(destfile, 'utf-8'))
                    rowcount = 0
                    for row in reader:
                        rowcount += 1
                        if rowcount > 1:
                            break
                    if rowcount <= 1:
                        delete_file = True

            # automatically delete zero-length files or detected "empty" content
            if delete_file:
                destfile.close()
                os.remove(destfilename)
                destfile = None

            log_msg = "File [%s] transfer successful. %s %s" % \
                      (destfilename, summary,
                       "File was automatically deleted due to empty content." if delete_file else "")
            logging.info(log_msg)
            if callback:
                callback(summary=log_msg, file_path=destfilename)

        finally:
            if destfile:
                destfile.close()

    def delete(self, path, headers=DEFAULT_HEADERS, guard_response=None):
        """Perform DELETE request, returning response object.

           Arguments:
             path: the path within this bound catalog
             headers: headers to set in request
             guard_response: expected current resource state
               as previously seen response object.

           Uses guard_response to build appropriate 'if-match' header
           to assure change is only applied to expected state.

           Raises ConcurrentUpdate for 412 status.

        """
        if path == "/":
            raise DerivaPathError('See self.delete_ermrest_catalog() if you really want to destroy this catalog.')
        return DerivaBinding.delete(self, path, headers=headers, guard_response=guard_response)

    def delete_ermrest_catalog(self, really=False):
        """Perform DELETE request, destroying catalog on server.

           Arguments:
             really: delete when True, abort when False (default)

        """
        if really is True:
            return DerivaBinding.delete(self, '/')
        else:
            raise ValueError('Catalog deletion refused when really is %s.' % really)

    def clone_catalog(self,
                      dst_catalog=None,
                      copy_data=True,
                      copy_annotations=True,
                      copy_policy=True,
                      truncate_after=True,
                      exclude_schemas=None,
                      dst_properties=None):
        """Clone this catalog's content into dest_catalog, creating a new catalog if needed.

        :param dst_catalog: Destination catalog or None to request creation of new destination (default).
        :param copy_data: Copy table contents when True (default).
        :param copy_annotations: Copy annotations when True (default).
        :param copy_policy: Copy access-control policies when True (default).
        :param truncate_after: Truncate destination history after cloning when True (default).
        :param exclude_schemas: A list of schema names to exclude from the cloning process.
        :param dst_properties: A dictionary of custom catalog-creation properties.

        When dst_catalog is provided, attempt an idempotent clone,
        assuming content MAY be partially cloned already using the
        same parameters. This routine uses a table-level annotation
        "tag:isrd.isi.edu,2018:clone-state" to save progress markers
        which help it restart efficiently if interrupted.

        When dst_catalog is not provided, a new catalog is
        provisioned. The optional dst_properties can customize
        metadata properties during this step:

        - name: str
        - description: str (markdown-formatted)
        - is_persistent: boolean

        Cloning preserves source row RID values for application tables
        so that any RID-based foreign keys are still valid. It is not
        generally advisable to try to merge more than one source into
        the same clone, nor to clone on top of rows generated locally
        in the destination, since this could cause duplicate RID
        conflicts.

        Cloning does not preserve all RID values for special ERMrest
        tables in the public schema (e.g. ERMrest_Client,
        ERMrest_Group) but normal applications should only consider
        the ID key of these tables.

        Truncation after cloning avoids retaining incremental
        snapshots which contain partial clones.

        """
        src_model = self.getCatalogModel()
        session_config = self._session_config.copy() if self._session_config else DEFAULT_SESSION_CONFIG.copy()
        session_config["allow_retry_on_all_methods"] = True

        if dst_catalog is None:
            if dst_properties is not None:
                if not isinstance(dst_properties, dict):
                    raise TypeError('dst_properties must be of type dict or None, not %s' % (type(dst_properties),))
            else:
                dst_properties = {}
            kwargs = {
                "name": dst_properties.get('name', 'Clone of %r' % (self._catalog_id,)),
                "description": dst_properties.get(
                    'description',
                    '''A cloned copy of catalog %r made with ErmrestCatalog.clone_catalog() using the following parameters:
- `copy_data`: %r
- `copy_annotations`: %r
- `copy_policy`: %r
- `truncate_after`: %r
- `exclude_schemas`: %r
''' % (
    self._catalog_id,
    copy_data,
    copy_annotations,
    copy_policy,
    truncate_after,
    exclude_schemas,
)),
                "clone_source": dst_properties.get('clone_source', self._catalog_id),
            }
            server = self.deriva_server
            dst_catalog = server.create_ermrest_catalog(**kwargs)

        # set top-level config right away and find fatal usage errors...
        if copy_policy:
            if not src_model.acls:
                raise ValueError("Use of copy_policy=True not possible when caller does not own source catalog.")
            dst_catalog.put('/acl', json=src_model.acls)

        if copy_annotations:
            dst_catalog.put('/annotation', json=src_model.annotations)

        # build up the model content we will copy to destination
        dst_model = dst_catalog.getCatalogModel()

        new_model = []
        new_columns = [] # ERMrest does not currently allow bulk column creation
        new_keys = [] # ERMrest does not currently allow bulk key creation
        clone_states = {}
        fkeys_deferred = {}
        exclude_schemas = [] if exclude_schemas is None else exclude_schemas

        def prune_parts(d, *extra_victims):
            victims = set(extra_victims)
            # we will apply config as a second pass after extending dest model
            # but loading bulk first may speed that up
            if not copy_annotations:
                victims |= {'annotations',}
            if not copy_policy:
                victims |= {'acls', 'acl_bindings'}
            for k in victims:
                d.pop(k, None)
            return d

        def copy_sdef(s):
            """Copy schema definition structure with conditional parts for cloning."""
            d = prune_parts(s.prejson(), 'tables')
            return d

        def copy_tdef_core(t):
            """Copy table definition structure with conditional parts excluding fkeys."""
            d = prune_parts(t.prejson(), 'foreign_keys')
            d['column_definitions'] = [ prune_parts(c) for c in d['column_definitions'] ]
            d['keys'] = [ prune_parts(c) for c in d.get('keys', []) ]
            d.setdefault('annotations', {})[_clone_state_url] = 1 if copy_data else None
            return d

        def copy_tdef_fkeys(t):
            """Copy table fkeys structure."""
            def check(fkdef):
                for fkc in fkdef['referenced_columns']:
                    if fkc['schema_name'] == 'public' \
                       and fkc['table_name'] in {'ERMrest_Client', 'ERMrest_Group', 'ERMrest_RID_Lease'} \
                       and fkc['column_name'] == 'RID':
                        raise ValueError("Cannot clone catalog with foreign key reference to %(schema_name)s:%(table_name)s:%(column_name)s" % fkc)
                return fkdef
            return [ prune_parts(check(d)) for d in t.prejson().get('foreign_keys', []) ]

        def copy_cdef(c):
            """Copy column definition with conditional parts."""
            return (sname, tname, prune_parts(c.prejson()))

        def check_column_compatibility(src, dst):
            """Check compatibility of source and destination column definitions."""
            def error(fieldname, sv, dv):
                return ValueError("Source/dest column %s mismatch %s != %s for %s:%s:%s" % (
                    fieldname,
                    sv, dv,
                    src.sname, src.tname, src.name
                ))
            if src.type.typename != dst.type.typename:
                raise error("type", src.type.typename, dst.type.typename)
            if src.nullok != dst.nullok:
                raise error("nullok", src.nullok, dst.nullok)
            if src.default != dst.default:
                raise error("default", src.default, dst.default)

        def copy_kdef(k):
            return (sname, tname, prune_parts(k.prejson()))

        for sname, schema in src_model.schemas.items():
            if sname in exclude_schemas:
                continue
            if sname not in dst_model.schemas:
                new_model.append(copy_sdef(schema))

            for tname, table in schema.tables.items():
                if table.kind != 'table':
                    logging.warning('Skipping cloning of %s %s:%s' % (table.kind, sname, tname))
                    continue

                if 'RID' not in table.column_definitions.elements:
                    raise ValueError("Source table %s.%s lacks system-columns and cannot be cloned." % (sname, tname))

                if sname not in dst_model.schemas or tname not in dst_model.schemas[sname].tables:
                    new_model.append(copy_tdef_core(table))
                    clone_states[(sname, tname)] = 1 if copy_data else None
                    fkeys_deferred[(sname, tname)] = copy_tdef_fkeys(table)
                else:
                    if dst_model.schemas[sname].tables[tname].foreign_keys:
                        # assume that presence of any destination foreign keys means we already loaded deferred_fkeys
                        copy_data = False
                    else:
                        fkeys_deferred[(sname, tname)] = copy_tdef_fkeys(table)

                    src_columns = { c.name: c for c in table.column_definitions }
                    dst_columns = { c.name: c for c in dst_model.schemas[sname].tables[tname].column_definitions }

                    for cname in src_columns:
                        if cname not in dst_columns:
                            new_columns.append(copy_cdef(src_columns[cname]))
                        else:
                            check_column_compatibility(src_columns[cname], dst_columns[cname])

                    for cname in dst_columns:
                        if cname not in src_columns:
                            raise ValueError("Destination column %s.%s.%s does not exist in source catalog." % (sname, tname, cname))

                    src_keys = { tuple(sorted(c.name for c in key.unique_columns)): key for key in table.keys }
                    dst_keys = { tuple(sorted(c.name for c in key.unique_columns)): key for key in dst_model.schemas[sname].tables[tname].keys }

                    for utuple in src_keys:
                        if utuple not in dst_keys:
                            new_keys.append(copy_kdef(src_keys[utuple]))

                    for utuple in dst_keys:
                        if utuple not in src_keys:
                            raise ValueError("Destination key %s.%s(%s) does not exist in source catalog." % (sname, tname, ', '.join(utuple)))

                    clone_states[(sname, tname)] = dst_model.schemas[sname].tables[tname].annotations.get(_clone_state_url)

        clone_states[('public', 'ERMrest_RID_Lease')] = None # never try to sync leases

        # apply the stage 1 model to the destination in bulk
        if new_model:
            dst_catalog.post("/schema", json=new_model).raise_for_status()

        for sname, tname, cdef in new_columns:
            dst_catalog.post("/schema/%s/table/%s/column" % (urlquote(sname), urlquote(tname)), json=cdef).raise_for_status()

        for sname, tname, kdef in new_keys:
            dst_catalog.post("/schema/%s/table/%s/key" % (urlquote(sname), urlquote(tname)), json=kdef).raise_for_status()

        # copy data in stage 2
        if copy_data:
            page_size = 10000
            for sname, tname in clone_states.keys():
                tname_uri = "%s:%s" % (urlquote(sname), urlquote(tname))
                if clone_states[(sname, tname)] == 1:
                    # determine current position in (partial?) copy
                    r = dst_catalog.get("/entity/%s@sort(RID::desc::)?limit=1" % tname_uri).json()
                    if r:
                        last = r[0]['RID']
                    else:
                        last = None

                    while True:
                        page = self.get(
                            "/entity/%s@sort(RID)%s?limit=%d" % (
                                tname_uri,
                                ("@after(%s)" % urlquote(last)) if last is not None else "",
                                page_size
                            )
                        ).json()
                        if page:
                            dst_catalog.post("/entity/%s?nondefaults=RID,RCT,RCB" % tname_uri, json=page)
                            last = page[-1]['RID']
                        else:
                            break

                    # record our progress on catalog in case we fail part way through
                    dst_catalog.put(
                        "/schema/%s/table/%s/annotation/%s" % (
                            urlquote(sname),
                            urlquote(tname),
                            urlquote(_clone_state_url),
                        ),
                        json=2
                    )
                elif clone_states[(sname, tname)] is None and (sname, tname) in {
                        ('public', 'ERMrest_Client'),
                        ('public', 'ERMrest_Group'),
                }:
                    # special sync behavior for magic ermrest tables
                    # HACK: these are assumed small enough to join via local merge of arrays
                    page = self.get("/entity/%s?limit=none" % tname_uri).json()
                    dst_catalog.post("/entity/%s?onconflict=skip" % tname_uri, json=page)

                    # record our progress on catalog in case we fail part way through
                    dst_catalog.put(
                        "/schema/%s/table/%s/annotation/%s" % (
                            urlquote(sname),
                            urlquote(tname),
                            urlquote(_clone_state_url),
                        ),
                        json=2
                    )

        # apply stage 2 model in bulk only... we won't get here unless preceding succeeded
        new_fkeys = []
        for fkeys in fkeys_deferred.values():
            new_fkeys.extend(fkeys)

        if new_fkeys:
            dst_catalog.post("/schema", json=new_fkeys)

        # copy over configuration in stage 3
        # we need to do this after deferred_fkeys to handle acl_bindings projections with joins
        dst_model = dst_catalog.getCatalogModel()

        for sname, src_schema in src_model.schemas.items():
            if sname in exclude_schemas:
                continue
            dst_schema = dst_model.schemas[sname]

            if copy_annotations:
                dst_schema.annotations.clear()
                dst_schema.annotations.update(src_schema.annotations)

            if copy_policy:
                dst_schema.acls.clear()
                dst_schema.acls.update(src_schema.acls)

            for tname, src_table in src_schema.tables.items():
                dst_table = dst_schema.tables[tname]

                if copy_annotations:
                    merged = dict(src_table.annotations)
                    if _clone_state_url in dst_table.annotations:
                        merged[_clone_state_url] = dst_table.annotations[_clone_state_url]
                    dst_table.annotations.clear()
                    dst_table.annotations.update(merged)

                if copy_policy:
                    dst_table.acls.clear()
                    dst_table.acls.update(src_table.acls)
                    dst_table.acl_bindings.clear()
                    dst_table.acl_bindings.update(src_table.acl_bindings)

                for cname, src_col in src_table.columns.elements.items():
                    dst_col = dst_table.columns[cname]

                    if copy_annotations:
                        dst_col.annotations.clear()
                        dst_col.annotations.update(src_col.annotations)

                    if copy_policy:
                        dst_col.acls.clear()
                        dst_col.acls.update(src_col.acls)
                        dst_col.acl_bindings.clear()
                        dst_col.acl_bindings.update(src_col.acl_bindings)

                for src_key in src_table.keys:
                    dst_key = dst_table.key_by_columns([ col.name for col in src_key.unique_columns ])

                    if copy_annotations:
                        dst_key.annotations.clear()
                        dst_key.annotations.update(src_key.annotations)

                def xlate_column_map(fkey):
                    dst_from_table = dst_table
                    dst_to_schema = dst_model.schemas[fkey.pk_table.schema.name]
                    dst_to_table = dst_to_schema.tables[fkey.pk_table.name]
                    return {
                        dst_from_table._own_column(from_col.name): dst_to_table._own_column(to_col.name)
                        for from_col, to_col in fkey.column_map.items()
                    }

                for src_fkey in src_table.foreign_keys:
                    dst_fkey = dst_table.fkey_by_column_map(xlate_column_map(src_fkey))

                    if copy_annotations:
                        dst_fkey.annotations.clear()
                        dst_fkey.annotations.update(src_fkey.annotations)

                    if copy_policy:
                        dst_fkey.acls.clear()
                        dst_fkey.acls.update(src_fkey.acls)
                        dst_fkey.acl_bindings.clear()
                        dst_fkey.acl_bindings.update(src_fkey.acl_bindings)

        # send all the config changes to the server
        dst_model.apply()

        # truncate cloning history
        if truncate_after:
            snaptime = dst_catalog.get("/").json()["snaptime"]
            dst_catalog.delete("/history/,%s" % urlquote(snaptime))

        return dst_catalog

class ErmrestSnapshot(ErmrestCatalog):
    """Persistent handle for an ERMrest catalog snapshot.

    Inherits from ErmrestCatalog and provides the same interfaces,
    except that the interfaces are now bound to a fixed snapshot
    of the catalog.
    """
    def __init__(self, scheme, server, catalog_id, snaptime, credentials=None, caching=True, session_config=None):
        """Create ERMrest catalog snapshot binding.

           Arguments:
             scheme: 'http' or 'https'
             server: server FQDN string
             catalog_id: e.g., '1'
             snaptime: e.g., '2PM-DGYP-56Z4'
             credentials: credential secrets, e.g. cookie
             caching: whether to retain a GET response cache
        """
        super(ErmrestSnapshot, self).__init__(scheme, server, catalog_id, credentials, caching, session_config)
        self._server_uri = "%s@%s" % (
            self._server_uri,
            snaptime
        )
        self._snaptime = snaptime

    @property
    def snaptime(self):
        """The snaptime for this catalog snapshot instance."""
        return self._snaptime

    def _pre_mutate(self, path, headers, guard_response=None):
        """Override and disable mutation operations.

        When called by the super-class, this method raises an exception.
        """
        raise ErmrestCatalogMutationError('Catalog snapshot is immutable')

class ErmrestAlias(DerivaBinding):
    """Persistent handle for an ERMrest alias.

       Provides basic REST client for HTTP methods on arbitrary
       paths. Caller has to understand ERMrest APIs and compose
       appropriate paths, headers, and/or content.

       Additional utility methods provided for accessing alias metadata.
    """
    @classmethod
    def connect(cls, deriva_server, alias_id):
        """Connect to an ERMrest alias and return the alias binding.

        :param deriva_server: The DerivaServer binding which hosts ermrest
        :param alias_id: The id of the existing alias

        The alias_id is a bare id (str).

        """
        return cls(
            deriva_server.scheme,
            deriva_server.server,
            alias_id,
            deriva_server.credentials,
            deriva_server.caching,
            deriva_server.session_config
        )

    @classmethod
    def _digest_alias_args(cls, id, owner, alias_target, name, description):
        rep = ErmrestCatalog._digest_catalog_args(id, owner, name, description)

        if isinstance(alias_target, (str, type(None))):
            rep['alias_target'] = alias_target
        elif isinstance(alias_target, type(nochange)):
            pass
        else:
            raise TypeError('alias_target must be of type str or None or nochange, not %s' % type(alias_target))

        return rep

    @classmethod
    def create(cls, deriva_server, id=None, owner=None, alias_target=None, name=None, description=None):
        """Create an ERMrest catalog alias.

        :param deriva_server: The DerivaServer binding which hosts ermrest
        :param id: The (str) id desired by the client (default None)
        :param owner: The initial (list of str) ACL desired by the client (default None)
        :param alias_target: The initial target catalog id desired by the client (default None)
        :param name: Initial (str) catalog name if not None
        :param description: Initial (str) catalog description if not None

        The new alias id will be returned in the response, and used
        in future alias access. The use of the id parameter
        may yield errors if the supplied value is not available for
        use by the client. The value None will result in a
        server-assigned alias id.

        The initial "owner" ACL on the new alias will be the
        client-supplied owner parameter. The use of owner may yield
        errors if the supplied ACL does not match the client, i.e. the
        client cannot lock themselves out of the alias. The value None
        will result in a server-assigned ACL with the requesting
        client's identity.

        The alias is bound to the client-supplied alias_target, if
        supplied. The use of alias_target may yield errors if the
        supplied value is not a valid target catalog id. The value
        None will reserve the alias in an unbound state.

        Certain failure modes (or message loss) may leave the id
        reserved in the system. In this case, the effective owner ACL
        influences which client(s) are allowed to retry creation with
        the same id.

        The name and description parameters are passed through to the
        alias creation service to initialize those respective metadata
        fields of the new aliase's registry entry. See ERMrest
        documentation for more detail.

        """
        path = '/ermrest/alias'
        r = deriva_server.post(path, json=cls._digest_alias_args(id, owner, alias_target, name, description))
        r.raise_for_status()
        return cls.connect(deriva_server, r.json()['id'])

    def __init__(self, scheme, server, alias_id, credentials=None, caching=True, session_config=None):
        """Create ERMrest alias binding.

        :param scheme: 'http' or 'https'
        :param server: server FQDN string
        :param alias_id: e.g. '1'
        :param credentials: credential secrets, e.g. cookie
        :param caching: whether to retain a GET response cache

        """
        super(ErmrestAlias, self).__init__(scheme, server, credentials, caching, session_config)
        self._server_uri = "%s/ermrest/alias/%s" % (
            self._server_uri,
            alias_id
        )
        self._scheme, self._server, self._alias_id, self._credentials, self._caching, self._session_config = \
            scheme, server, alias_id, credentials, caching, session_config

    @property
    def alias_id(self):
        return self._alias_id

    def check_path(self, path):
        if path != '':
            raise ValueError('ErmrestAlias requires "" relative path')

    def retrieve(self):
        """Retrieve current alias binding state as a dict.

        The returned dictionary is suitable for local revision and
        being passed back into self.update:

           state = self.retrieve()
           state.update({ "owner": ..., "alias_target": ...)
           self.update(**state)

        """
        return self.get('').json()

    def update(self, owner=nochange, alias_target=nochange, id=None):
        """Update alias binding state in server, returning the response message dict.

        :param owner: Revised owner ACL for binding or nochange (default None)
        :param alias_target: Revised target for binding or nochange (default None)
        :param id: Current self.alias_id or None (default None)

        The optional id parameter must be None or self.alias_id and
        does not affect state changes to the server. It is only
        specified in order to allow an idiom like:

           state = self.retrieve()
           state.update(...)
           self.update(**state)

        where the original "id" field of self.retrieve() is harmlessly
        passed through as a keyword.

        """
        rep = self._digest_alias_args(id, owner, alias_target)
        if id is not None and id != self.alias_id:
            raise ValueError('parameter id must be None or %r, not %r' % (self.alias_id, id))
        return self.put('', json=rep).json()

    def delete_ermrest_alias(self, really=False):
        """Perform DELETE request, destroying alias on server.

        :param really: delete when True, abort when False (default)

        """
        if really is True:
            return DerivaBinding.delete(self, '')
        else:
            raise ValueError('Alias deletion refused when really is %s.' % really)

