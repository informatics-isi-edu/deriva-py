import mimetypes
import re
from deriva.core import urlunquote

if not mimetypes.inited:
    mimetypes.init()


def add_types(types):
    if not types:
        return
    for t in types.keys():
        for e in types[t]:
            mimetypes.add_type(type=t, ext=e if e.startswith(".") else "".join([".", e]))


def guess_content_type(file_path):
    mtype = mimetypes.guess_type(file_path, strict=False)
    content_type = 'application/octet-stream'
    if mtype[0] is not None and mtype[1] is not None:
        content_type = "+".join([mtype[0], mtype[1]])
    elif mtype[0] is not None:
        content_type = mtype[0]
    elif mtype[1] is not None:
        content_type = mtype[1]

    return content_type


def parse_content_disposition(value):
    m = re.match("^filename[*]=UTF-8''(?P<name>[-_.~A-Za-z0-9%]+)$", value)
    if not m:
        raise ValueError('Cannot parse content-disposition "%s".' % value)

    n = m.groupdict()['name']

    try:
        n = urlunquote(str(n))
    except Exception as e:
        raise ValueError('Invalid URL encoding of content-disposition filename component. %s.' % e)

    return n
