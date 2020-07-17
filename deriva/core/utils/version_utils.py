import re
import sys
from packaging import version


def get_installed_version(ver):
    """
    Generates an external version string from an internal one. This is currently only being used to determine
    whether we are operating in a "frozen" environment or not, but other decorators could be added.
    """
    version_str = str(ver)
    return version_str if not getattr(sys, 'frozen', False) else version_str + '-frozen'


def is_compatible(source_version, compat_versions):
    """
    Compare a source version string to a set of target version string specifications. Supports semantic versioning
    comparison via setuptools version comparison logic (http://setuptools.readthedocs.io/en/latest/setuptools.html#id7).

    :param source_version: a source version string
    :param compat_versions:
        an array of tuples with each tuple consisting of a set of strings of the form
        `<operator><version>`. The source_version is evaluated in a conjunction against each
        `<operator><version>` string in the tuple, using the packaging module version comparison
        function. The result of every tuple evaluation is then evaluated in a disjunction
        against other tuples in the array.  If any one of the tuples evaluates to True, then the
        returned disjunction is logically true, and the source version is assumed to be compatible.
    :return: boolean indicating compatibility

    Example 1:
    ::
        is_compatible("0.1.0", [[">=0.1.0", "<1.0.0"]])

    :return: `True`

    Example 2:
    ::
        is_compatible("1.1.0", [[">=0.1.0", "<1.0.0"]])

    :return: `False`

    Example 3:
    ::
        is_compatible("0.5.1-dev", [[">=0.3.5rc4", "<0.5.9-beta", "!=0.5.0"]])

    :return: `True`

    Example 4:
    ::
        is_compatible("0.7.7", [[">=0.7.0", "<0.7.6"], ["!=0.7.1"]])

    :return: `True` (disjunct of "!=0.7.1" cancelled out range ">=0.7.0", "<0.7.6" -- see below)

    Example 5:
    ::
        is_compatible("0.7.7", [[">=0.7.0", "<0.7.6", "!=0.7.1"]])

    :return: `False`

    Example 5:
    ::
        is_compatible("version-3.3", [["==version-3.0"], ["==version-3.1"], ["==version-3.3"]])

    :return: `True`

    """
    pattern = "^.*(?P<operator>(>=|<=|>|<|==|!=))(?P<version>.*)$"
    compat = None
    for version_spec in compat_versions:
        check = None
        for ver in version_spec:
            match = re.search(pattern, ver)
            if match:
                gd = match.groupdict()
                operator = gd["operator"]
                target_version = gd["version"]
                ret = eval("version.parse(source_version) %s version.parse(target_version)" % operator)
                if check is None:
                    check = ret
                else:
                    check = ret and check
        if compat is None:
            compat = check
        else:
            compat = check or compat

    return compat if compat is not None else False
