import os
import datetime
import requests
import logging
from . import format_exception, NotModified, DEFAULT_HEADERS, DEFAULT_CHUNK_SIZE, MAX_CHUNK_SIZE, urlquote, Megabyte, \
    get_transfer_summary, MaxRetryError
from .deriva_binding import DerivaBinding
from .utils import hash_utils as hu, mime_utils as mu


class HatracHashMismatch (ValueError):
    pass


class HatracJobAborted (Exception):
    pass


class HatracJobPaused (Exception):
    pass


class HatracJobTimeout (Exception):
    pass


class HatracStore(DerivaBinding):
    def __init__(self, scheme, server, credentials=None, session_config=None):
        """Create Hatrac server binding.

           Arguments:
             scheme: 'http' or 'https'
             server: server FQDN string
             credentials: credential secrets, e.g. cookie

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
        DerivaBinding.__init__(self, scheme, server, credentials, caching=False, session_config=session_config)

    def content_equals(self, path, filename=None, md5=None, sha256=None):
        """
        Check if a remote object's content is equal to the content of the at least one of the specified input file,
        input md5, or input sha256 by comparing MD5 hashes.
        :return: True IFF the object exists and the MD5 or SHA256 hash matches the MD5 or SHA256 hash of the input file
                 or the passed MD5 or SHA256 parameters.
        """
        self.check_path(path)

        assert filename or md5 or sha256
        if filename:
            hashes = hu.compute_file_hashes(filename, hashes=['md5', 'sha256'])
            md5 = hashes['md5'][1]
            sha256 = hashes['sha256'][1]

        r = self.head(path)
        r.raise_for_status()
        if r.status_code == 200 and \
                (md5 and r.headers.get('Content-MD5') == md5 or sha256 and r.headers.get('Content-SHA256') == sha256):
            return True
        else:
            return False

    def get_obj(self, path, headers=DEFAULT_HEADERS, destfilename=None, callback=None):
        """Retrieve resource optionally streamed to destination file.

           If destfilename is provided, download content to file with
           that name.  Caller is responsible to clean up file even on
           error, when the file may or may not be exist.

           If hatrac provides a Content-MD5 response header, the
           resulting download file will be hash-verified on success or
           raise HatracHashMismatch on errors.  This is not verified
           when destfilename is None, as the client must instead
           consume and validate content directly from the response
           object.

        """
        self.check_path(path)

        headers = headers.copy()

        if destfilename is not None:
            destfile = open(destfilename, 'w+b')
            stream = True
        else:
            destfile = None
            stream = False

        headers['deriva-client-context'] = self.dcctx.merged(headers.get('deriva-client-context', {})).encoded()

        try:
            r = self._session.get(self._server_uri + path, headers=headers, stream=stream)
            self._response_raise_for_status(r)

            if destfilename is not None:
                total = 0
                start = datetime.datetime.now()
                logging.debug("Transferring file %s to %s" % (self._server_uri + path, destfilename))
                for buf in r.iter_content(chunk_size=Megabyte):
                    destfile.write(buf)
                    total += len(buf)
                    if callback:
                        if not callback(progress="Downloading: %.2f MB transferred" % (total / Megabyte)):
                            destfile.close()
                            os.remove(destfilename)
                            return None
                destfile.flush()
                elapsed = datetime.datetime.now() - start
                summary = get_transfer_summary(total, elapsed)
                logging.info("File [%s] transfer successful. %s" % (destfilename, summary))
                if callback:
                    callback(summary=summary, file_path=destfilename)

                if 'Content-SHA256' in r.headers:
                    destfile.seek(0, 0)
                    logging.info("Verifying checksum for file [%s]" % destfilename)
                    fsha256 = hu.compute_hashes(destfile, hashes=['sha256'])['sha256'][1]
                    rsha256 = r.headers.get('Content-SHA256', r.headers.get('content-sha256', None))
                    if fsha256 != rsha256:
                        raise HatracHashMismatch('Content-SHA256 %s != computed sha256 %s' % (rsha256, fsha256))
                elif 'Content-MD5' in r.headers:
                    destfile.seek(0, 0)
                    logging.info("Verifying checksum for file [%s]" % destfilename)
                    fmd5 = hu.compute_hashes(destfile, hashes=['md5'])['md5'][1]
                    rmd5 = r.headers.get('Content-MD5', r.headers.get('content-md5', None))
                    if fmd5 != rmd5:
                        raise HatracHashMismatch('Content-MD5 %s != computed MD5 %s' % (rmd5, fmd5))

            return r
        finally:
            if destfile is not None:
                destfile.close()

    def put_obj(self, path, data, headers=DEFAULT_HEADERS, md5=None, sha256=None, parents=True):
        """Idempotent upload of object, returning object location URI.

           Arguments:
              path: name of object
              data: filename or seekable file-like object
              headers: additional headers
              md5: a base64 encoded md5 digest may be provided in order to skip the automatic hash computation
              sha256: a base64 encoded sha256 digest may be provided in order to skip the automatic hash computation
              parents: automatically create parent namespace(s) if missing
           Automatically computes and sends Content-MD5 if no digests provided.

           If an object-version already exists under the same name
           with the same Content-MD5, that location is returned
           instead of creating a new one.

        """
        self.check_path(path)

        headers = headers.copy()

        if hasattr(data, 'read') and hasattr(data, 'seek'):
            f = data
        else:
            f = open(data, 'rb')

        if not (md5 or sha256):
            md5 = hu.compute_hashes(f, hashes=['md5'])['md5'][1]

        f.seek(0, 0)

        try:
            r = self.head(path)
            if r.status_code == 200 and \
                    (md5 and r.headers.get('Content-MD5') == md5 or
                     sha256 and r.headers.get('Content-SHA256') == sha256):
                # object already has same content so skip upload
                f.close()
                return r.headers.get('Content-Location')
        except requests.HTTPError as e:
            pass

        # TODO: verify incoming hashes if supplied?
        headers['Content-MD5'] = md5
        headers['Content-SHA256'] = sha256

        headers['deriva-client-context'] = self.dcctx.merged(headers.get('deriva-client-context', {})).encoded()

        url = self._server_uri + path
        url = '%s%s' % (url.rstrip("/") if url.endswith("/") else url,
                        "" if not parents else "?parents=%s" % str(parents).lower())
        r = self._session.put(url, data=f, headers=headers)
        self._response_raise_for_status(r)
        loc = r.text.strip() or r.url
        if loc.startswith(self._server_uri):
            loc = loc[len(self._server_uri):]
        return loc

    def del_obj(self, path):
        """Delete an object.
        """
        self.check_path(path)
        resp = self.delete(path)
        resp.raise_for_status()
        logging.debug('Deleted object "%s%s".' % (self._server_uri, path))

    def put_loc(self,
                path,
                file_path,
                headers=DEFAULT_HEADERS,
                md5=None,
                sha256=None,
                content_type=None,
                content_disposition=None,
                chunked=False,
                chunk_size=DEFAULT_CHUNK_SIZE,
                create_parents=True,
                allow_versioning=True,
                callback=None):
        """
        :param path:
        :param file_path:
        :param headers:
        :param md5:
        :param sha256:
        :param content_type:
        :param content_disposition:
        :param chunked:
        :param chunk_size:
        :param create_parents:
        :param allow_versioning:
        :param callback:
        :return:
        """
        self.check_path(path)

        if not chunked:
            return self.put_obj(path, file_path, headers, md5, sha256)

        if not (md5 or sha256):
            md5 = hu.compute_file_hashes(file_path, hashes=['md5'])['md5'][1]

        try:
            r = self.head(path)
            if r.status_code == 200 and \
                    (md5 and r.headers.get('Content-MD5') == md5 or sha256 and r.headers.get('Content-SHA256') == sha256):
                # object already has same content so skip upload
                return r.headers.get('Content-Location')
            elif not allow_versioning:
                raise NotModified("The file [%s] cannot be uploaded because content already exists for this object "
                                  "and multiple versions are not allowed." % file_path)
        except requests.HTTPError as e:
            if e.response.status_code != 404:
                logging.debug("HEAD request failed: %s" % format_exception(e))
            pass

        try:
            job_id = self.create_upload_job(path, file_path, md5, sha256,
                                            content_type=content_type,
                                            content_disposition=content_disposition,
                                            create_parents=create_parents)
            self.put_obj_chunked(path, file_path, job_id, chunk_size, callback)
            return self.finalize_upload_job(path, job_id)
        except (requests.Timeout, MaxRetryError) as e:
            raise HatracJobTimeout(e)

    def put_obj_chunked(self, path, file_path, job_id, chunk_size=DEFAULT_CHUNK_SIZE, callback=None, start_chunk=0):
        self.check_path(path)
        if chunk_size > MAX_CHUNK_SIZE:
            chunk_size = MAX_CHUNK_SIZE
        job_info = self.get_upload_job(path, job_id).json()
        try:
            file_size = os.path.getsize(file_path)
            chunks = file_size / chunk_size
            if file_size % chunk_size:
                chunks += 1
            with open(file_path, 'rb') as f:
                total = 0
                chunk = start_chunk
                if chunk > 0:
                    total = chunk * chunk_size
                    f.seek(total)
                start = datetime.datetime.now()
                logging.debug("Transferring file %s to %s%s" % (file_path, self._server_uri, path))
                while True:
                    data = f.read(chunk_size)
                    if not data:
                        break
                    url = '%s;upload/%s/%d' % (path, job_id, chunk)
                    headers = {'Content-Type': 'application/octet-stream', 'Content-Length': '%d' % len(data)}
                    r = self.put(url, data=data, headers=headers)
                    self._response_raise_for_status(r)
                    total += len(data)
                    chunk += 1
                    if callback:
                        ret = callback(job_info=job_info,
                                       completed=chunk,
                                       total=chunks,
                                       file_path=file_path,
                                       host=self._server_uri)
                        if ret == 0:
                            self.cancel_upload_job(path, job_id)
                            raise HatracJobAborted("Upload in-progress cancelled by user.")
                        elif ret == -1:
                            raise HatracJobPaused("Upload in-progress paused by user.")
                elapsed = datetime.datetime.now() - start
                summary = get_transfer_summary(total, elapsed)
                logging.info("File [%s] upload successful. %s" % (file_path, summary))
                if callback:
                    callback(summary=summary, file_path=file_path)
        except (HatracJobAborted, HatracJobPaused):
            raise
        except:
            try:
                self.cancel_upload_job(path, job_id)
            except:
                pass
            raise

    def create_upload_job(self,
                          path,
                          file_path,
                          md5,
                          sha256,
                          create_parents=True,
                          chunk_size=DEFAULT_CHUNK_SIZE,
                          content_type=None,
                          content_disposition=None):
        self.check_path(path)
        if chunk_size > MAX_CHUNK_SIZE:
            chunk_size = MAX_CHUNK_SIZE

        url = '%s;upload%s' % (path, "" if not create_parents else "?parents=%s" % str(create_parents).lower())
        obj = {"chunk-length": chunk_size,
               "content-length": os.path.getsize(file_path)}
        if md5:
            obj["content-md5"] = md5
        if sha256:
            obj["content-sha256"] = sha256
        if content_disposition:
            obj['content-disposition'] = content_disposition
        obj['content-type'] = content_type if content_type else mu.guess_content_type(file_path)
        r = self.post(url, json=obj, headers={'Content-Type': 'application/json'})
        self._response_raise_for_status(r)
        job_id = r.text.split('/')[-1][:-1]
        logging.debug('Created job_id "%s" for url "%s".' % (job_id,  url))
        return job_id

    def get_upload_job(self, path, job_id):
        self.check_path(path)
        url = '%s;upload/%s' % (path, job_id)
        headers = {}
        r = self.get(url, headers=headers)
        r.raise_for_status()
        return r

    def finalize_upload_job(self, path, job_id):
        self.check_path(path)
        url = '%s;upload/%s' % (path, job_id)
        headers = {}
        r = self.post(url, headers=headers)
        r.raise_for_status()
        return r.text.strip()

    def cancel_upload_job(self, path, job_id):
        self.check_path(path)
        url = '%s;upload/%s' % (path, job_id)
        headers = {}
        r = self.delete(url, headers=headers)
        r.raise_for_status()

    def is_valid_namespace(self, namespace_path):
        """Check if a namespace already exists.
        """
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        resp = self.head(namespace_path, headers)
        if resp.status_code == requests.codes.ok:
            return True
        if resp.status_code == requests.codes.not_found:
            return False
        resp.raise_for_status()

    def retrieve_namespace(self, namespace_path):
        """Retrieve a namespace.
        """
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        resp = self.get(namespace_path, headers)
        if resp.status_code == requests.codes.ok:
            namespaces = resp.json()
            return namespaces
        if resp.status_code == requests.codes.not_found:
            return None
        resp.raise_for_status()

    def create_namespace(self, namespace_path, parents=True):
        """Create a namespace.
        """
        self.check_path(namespace_path)
        url = "?".join([namespace_path, "parents=%s" % str(parents).lower()])
        headers = {'Content-Type': 'application/x-hatrac-namespace', 'Accept': 'application/json'}
        resp = self.put(url, headers=headers)
        resp.raise_for_status()
        logging.debug('Created namespace "%s%s".' % (self._server_uri, namespace_path))

    def delete_namespace(self, namespace_path):
        """Delete a namespace.
        """
        self.check_path(namespace_path)
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        resp = self.delete(namespace_path, headers=headers)
        resp.raise_for_status()
        logging.debug('Deleted namespace "%s%s".' % (self._server_uri, namespace_path))

    def get_acl(self, resource_name, access=None, role=None):
        """Get the object or namespace ACL resource.
        """
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        url = resource_name + ';acl'
        if access:
            url += '/' + urlquote(access)
            if role:
                url += '/' + urlquote(role)
        elif role:
            raise ValueError('Do not specify "role" if "access" mode is not specified.')
        resp = self.get(url, headers)
        if resp.status_code == requests.codes.ok:
            if role:
                return {access: [role]}
            elif access:
                return {access: resp.json()}
            else:
                return resp.json()
        if resp.status_code == requests.codes.not_found:
            return None
        resp.raise_for_status()

    def set_acl(self, resource_name, access, roles, add_role=False):
        """Set the object or namespace ACL resource.

        if 'add_role' is True, the operation will add a single role to the ACL, else it will attempt to replace
        all of the ACL's roles. This option is only valid when a list of one role is given.
        """
        if add_role and len(roles) > 1:
            raise ValueError("Cannot add more than one role at a time.")
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        url = "%(resource_name)s;acl/%(access)s" % {'resource_name': resource_name, 'access': urlquote(access)}
        roles_obj = None
        if add_role:
            url += '/' + urlquote(roles[0])
        else:
            roles_obj = roles
        resp = self.put(url, json=roles_obj, headers=headers)
        if resp.status_code == requests.codes.no_content:
            return None
        resp.raise_for_status()

    def del_acl(self, resource_name, access, role=None):
        """Delete the object or namespace ACL resource.
        """
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        url = "%(resource_name)s;acl/%(access)s" % {'resource_name': resource_name, 'access': urlquote(access)}
        if role:
            url += '/' + urlquote(role)
        resp = self.delete(url, headers)
        if resp.status_code == requests.codes.no_content:
            return None
        resp.raise_for_status()

