import os
import hashlib
import base64
import logging


def compute_hashes(fd, hashes=frozenset(['md5'])):
    """
       Digests data read from file-like input fd.
       Compute hashes for multiple algorithms. Default is MD5.
       Returns a tuple of a hex-encoded digest string and a base64-encoded value suitable for an HTTP header.
    """
    hashers = dict()
    for alg in hashes:
        try:
            hashers[alg] = hashlib.new(alg.lower())
        except ValueError:
            logging.warning("Unable to validate file contents using unknown hash algorithm: %s", alg)

    while True:
        block = fd.read(1024 ** 2)
        if not block:
            break
        for i in hashers.values():
            i.update(block)

    hashes = dict()
    for alg, h in hashers.items():
        digest = h.hexdigest()
        base64digest = base64.b64encode(h.digest())
        # base64.b64encode returns str on python 2.7 and bytes on 3.x, so deal with that and always return a str
        if not isinstance(base64digest, str) and isinstance(base64digest, bytes):
            base64digest = base64digest.decode('ascii')
        hashes[alg] = digest, base64digest

    return hashes


def compute_file_hashes(file_path, hashes=frozenset(['md5'])):
    """
       Digests data read from file denoted by file_path.
    """
    if not os.path.exists(file_path):
        logging.warning("%s does not exist" % file_path)
        return
    else:
        logging.debug("Computing [%s] hashes for file [%s]" % (','.join(hashes), file_path))

    try:
        with open(file_path, 'rb') as fd:
            return compute_hashes(fd, hashes)
    except (IOError, OSError) as e:
        logging.warning("Error while calculating digest(s) for file %s: %s" % (file_path, str(e)))
        raise

