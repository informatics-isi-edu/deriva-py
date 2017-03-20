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
            hashers[alg] = hashlib.new(alg)
        except ValueError:
            logging.warning("Unable to validate file contents using unknown hash algorithm: %s", alg)

    while True:
        block = fd.read(1024 ** 2)
        if not block:
            break
        for i in hashers.values():
            i.update(block)

    return dict((alg, (h.hexdigest(), base64.b64encode(h.digest()))) for alg, h in hashers.items())


def compute_file_hashes(file_path, hashes=frozenset(['md5'])):
    """
       Digests data read from file denoted by file_path.
    """
    if not os.path.exists(file_path):
        logging.warn("%s does not exist" % file_path)
        return
    else:
        logging.debug("Computing [%s] hashes for file [%s]" % (','.join(hashes), file_path))

    try:
        with open(file_path, 'rb') as fd:
            return compute_hashes(fd, hashes)
    except (IOError, OSError) as e:
        logging.warn("Error while calculating digest(s) for file %s: %s" % (file_path, str(e)))
        raise

