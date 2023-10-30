import os
import hashlib
import base64
import binascii
import logging


def compute_hashes(obj, hashes=frozenset(['md5'])):
    """
       Digests input data read from file-like object fd or passed directly as bytes-like object.
       Compute hashes for multiple algorithms. Default is MD5.
       Returns a tuple of a hex-encoded digest string and a base64-encoded value suitable for an HTTP header.
    """
    if not (hasattr(obj, 'read') or isinstance(obj, bytes)):
        raise ValueError("Cannot compute hash for given input: a file-like object or bytes-like object is required")

    hashers = dict()
    for alg in hashes:
        try:
            hashers[alg] = hashlib.new(alg.lower())
        except ValueError:
            logging.warning("Unable to validate file contents using unknown hash algorithm: %s", alg)

    while True:
        if hasattr(obj, 'read'):
            block = obj.read(1024 ** 2)
        else:
            block = obj
            obj = None
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
        raise FileNotFoundError(file_path)
    else:
        logging.debug("Computing [%s] hashes for file [%s]" % (','.join(hashes), file_path))

    try:
        with open(file_path, 'rb') as fd:
            return compute_hashes(fd, hashes)
    except (IOError, OSError) as e:
        logging.warning("Error while calculating digest(s) for file %s: %s" % (file_path, str(e)))
        raise


def decodeBase64toHex(base64str):
    result = binascii.hexlify(base64.standard_b64decode(base64str))
    if isinstance(result, bytes):
        result = result.decode('ascii')

    return result
