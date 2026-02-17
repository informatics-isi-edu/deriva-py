# Tests for the datapath module.
#
# Environment variables:
#  DERIVA_PY_TEST_HOSTNAME: hostname of the test server
#  DERIVA_PY_CATALOG: catalog identifier of the reusable test catalog (optional)
#  DERIVA_PY_TEST_CREDENTIAL: user credential, if none, it will attempt to get credential for given hostname (optional)
#  DERIVA_PY_TEST_VERBOSE: set for verbose logging output to stdout (optional)

import logging
import os
import datetime
import math
import json
import io
import unittest
from typing import Any, Sequence
from deriva.core import DerivaServer, get_credential, ermrest_model, tag
from deriva.core import \
    crockford_b32encode, crockford_b32decode, \
    int_to_uintX, uintX_to_int, \
    datetime_to_epoch_microseconds, epoch_microseconds_to_datetime
import deriva.core.ermrest_model as em

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
if os.getenv("DERIVA_PY_TEST_VERBOSE"):
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

hostname = os.getenv("DERIVA_PY_TEST_HOSTNAME")

_EMTypeTests_py_checked_cases: dict[Sequence[str], tuple[list[Any], list[Any]]] = {
    ('text', 'markdown', "longtext", "ermrest_curie", "ermrest_uri", "color_rgb_hex", "ermrest_rid", "ermrest_rcb", "ermrest_rmb"): (
        [None, '', 'foo', 'foo bar', 'foo\nbar'],
        [42, True, {}, [], set(), tuple()],
    ),
    ('float8', 'float4'): (
        # shared cases due to current lack of range checking
        [None, 0.0, math.pi, -1.0, 123.456E78],
        [42, True, "1.0", {}, [], set(), tuple()],
    ),
    ('int8', 'serial8'): (
        [None, -1, 0, 1, -2**63, 2**63 - 1],
        [42.0, True, "1", -2**63-1, 2**63, {}, [], set(), tuple()],
    ),
    ('int4', 'serial4'): (
        [None, -1, 0, 1, -2**31, 2**31 - 1],
        [42.0, True, "1", -2**31-1, 2**31, {}, [], set(), tuple()],
    ),
    ('int2', 'serial2'): (
        [None, -1, 0, 1, -2**15, 2**15 - 1],
        [42.0, True, "1", -2**15-1, 2**15, {}, [], set(), tuple()],
    ),
    ('boolean',): (
        [None, True, False],
        [1, 0, 1.0, 0.0, {}, [], set(), tuple()],
    ),
    ('timestamptz', "ermrest_rct", "ermrest_rmt"): (
        [
            None,
            datetime.datetime.fromisoformat('2025-12-31 12:34:56.123456+00:00'),
        ],
        [
            datetime.datetime.fromisoformat('1970-01-01 00:00:00'),
            42,
            41.9999999,
            "1970-01-01 00:00:00+00:00",
            True,
            {}, [], set(), tuple(),
        ],
    ),
    ('timestamp',): (
        [
            None,
            datetime.datetime.fromisoformat('2025-12-31 12:34:56.123456+00:00'),
            datetime.datetime.fromisoformat('1970-01-01 00:00:00'),
        ],
        [
            42,
            41.9999999,
            "1970-01-01 00:00:00+00:00",
            True,
            {}, [], set(), tuple(),
        ],
    ),
    ('date',): (
        [
            None,
            datetime.date.fromisoformat('2025-12-31'),
        ],
        [
            42,
            41.9999999,
            "1970-01-01 00:00:00+00:00",
            True,
            {}, [], set(), tuple(),
        ],
    ),
    ('json', 'jsonb'): (
        [
            None, {}, [], True, False, 0, 0.0, "foo", "foo\nbar",
            {"a": 1, "b": 2},
            {"a": [1, 2, "3"], "b": None},
            [1, 2, "3", {"a": 1, "b": 2}],
        ],
        [
            datetime.date.fromisoformat('2025-12-31'),
            set(),
            tuple(),
            {1: 5},
            [1, 2, {1,2}],
            {"a":[ {"a": [ {1: 2}, ], }, ],},
        ],
    ),
}
_EMTypeTests_py_checked_cases |= {
    (atypename,): (
        [
            None,
            [],
        ] + [
            # turn each good case (list) into a single array test case
            cases[0]
            for btypenames, cases in _EMTypeTests_py_checked_cases.items()
            if em.builtin_types[atypename].base_type.typename in set(btypenames)
        ],
        [
            # turn each bad case into an array test case
            [bad,]
            for btypenames, cases in _EMTypeTests_py_checked_cases.items()
            if em.builtin_types[atypename].base_type.typename in set(btypenames)
            for bad in cases[1]
        ],
    )
    # transform known base_type cases into new tests
    for atypename in [
        'text[]',
        'float8[]', 'float4[]',
        'int8[]', 'int4[]', 'int2[]',
        'boolean[]',
        'timestamptz[]', 'timestamp[]', 'date[]',
        'json[]', 'jsonb[]',
    ]
}

_EMTypeTests_text_convert_cases: dict[Sequence[str], dict[Any,Any]] = {
    ("text", "markdown", "longtext", "ermrest_curie", "ermrest_uri", "color_rgb_hex", "ermrest_rid", "ermrest_rcb", "ermrest_rmb"): {
        None: None,
        "": "",
        "foo": "foo",
        "foo\nbar": "foo\nbar",
    },
    ("float8", "float4"): {
        None: None,
        "0.0": 0.0,
        "-1.0": -1.0,
        "-9007199254740992.0": float(-(2**53)),
        repr(math.pi): math.pi,
    },
    ("int8", "serial8"): {
        "9223372036854775807": 2**63-1,
        "-9223372036854775808": - 2**63,
    },
    ("int4", "serial4"): {
        "2147483647": 2**31-1,
        "-2147483648": - 2**31,
    },
    ("int8", "serial8", "int4", "serial4", "int2", "serial2"): {
        None: None,
        "-1": -1,
        "0": 0,
        "1": 1,
        "32767": 2**15-1,
        "-32768": - 2**15,
    },
    ("boolean",): {
        None: None,
        "true": True,
        "false": False,
    },
    ("timestamptz", "ermrest_rct", "ermrest_rmt"): {
        None: None,
    } | {
        iso: datetime.datetime.fromisoformat(iso)
        for iso in [
            "2025-12-31 12:34:56.789012+00:00",
            "2025-12-31 12:34:56+00:00",
        ]
    },
    ("timestamp",): {
        None: None,
    } | {
        iso: datetime.datetime.fromisoformat(iso)
        for iso in [
            "2025-12-31 12:34:56.789012",
            "2025-12-31 12:34:56",
        ]
    },
    ("date",): {
        None: None,
    } | {
        iso: datetime.date.fromisoformat(iso)
        for iso in [
            "2025-12-31",
        ]
    },
    ("json", "jsonb",): {
        None: None, # handled by column rather than json coder...
        "true": True,
        "false": False,
        "-1": -1,
        repr(math.pi): math.pi,
        '"foo"': "foo",
        "[]": [],
        "{}": {},
        "[1,2,3]": [1,2,3],
        '{"a":1,"b":[1,2,3]}': {"a":1,"b":[1,2,3]},
    }
}
_EMTypeTests_text_convert_cases |= {
    (atypename,): {
        None: None,
        '[]': [],
    } | {
        # for these array types, the base_type cases have JSON serialized keys EXCEPT for None
        '[%s]' % (','.join([
            k if k is not None else 'null'
            for k in cases.keys()
        ]),): list(cases.values())
        for btypenames, cases in _EMTypeTests_text_convert_cases.items()
        if em.builtin_types[atypename].base_type.typename in set(btypenames)
    }
    for atypename in [
        'float8[]', 'float4[]',
        'int8[]', 'int4[]', 'int2[]',
        'boolean[]',
        'boolean[]',
        'json[]', 'jsonb[]',
    ]
} | {
    (atypename,): {
        None: None,
        '[]': [],
    } | {
        # for these array types, the base_type cases have string keys EXCEPT for None
        json.dumps(list(cases.keys()), separators=(',',':')): list(cases.values())
        for btypenames, cases in _EMTypeTests_text_convert_cases.items()
        if em.builtin_types[atypename].base_type.typename in set(btypenames)
    }
    for atypename in [
        'text[]', 'timestamptz[]', 'timestamp[]', 'date[]',
    ]
}

_EMTypeTests_py_pre_json_cases: dict[Sequence[str], Sequence[list[Any]]] = {
    ("text", "markdown", "longtext", "ermrest_curie", "ermrest_uri", "color_rgb_hex", "ermrest_rid", "ermrest_rcb", "ermrest_rmb"): [
        [None, None],
        ["", ""],
        ["foo", "foo"],
        ["foo\nbar", "foo\nbar"],
    ],
    ("float8", "float4"): [
        [val, val]
        for val in [None, -1.0, 0.0, 1.0, math.pi]
    ],
    ("int8", "int4", "int2", "serial8", "serial4", "serial2"): [
        [val, val]
        for val in [None, -1, 0, 1, 3453]
    ],
    ("boolean",): [
        [val, val]
        for val in [None, True, False]
    ],
    ("timestamptz", "ermrest_rct", "ermrest_rmt"): [
        [None, None]
    ] + [
        [datetime.datetime.fromisoformat(iso), iso]
        for iso in [
            "2025-12-31 12:34:56.789012+00:00",
            "2025-12-31 12:34:56+00:00",
        ]
    ],
    ("timestamp",): [
        [None, None],
    ] + [
        [datetime.datetime.fromisoformat(iso), iso]
        for iso in [
            "2025-12-31 12:34:56.789012",
            "2025-12-31 12:34:56",
        ]
    ],
    ("date",): [
        [None, None],
    ] + [
        [datetime.date.fromisoformat(iso), iso]
        for iso in [
            "2025-12-31",
        ]
    ],
}

_EMTypeTests_py_pre_json_cases |= {
    (atypename,): [
        [
            [ testcase[0] for testcase in cases ],
            [ testcase[1] for testcase in cases ],
        ]
        for btypenames, cases in _EMTypeTests_py_pre_json_cases.items()
        if em.builtin_types[atypename].base_type.typename in set(btypenames)
    ]
    # transform known base_type cases into new tests
    for atypename in [
        'text[]',
        'float8[]', 'float4[]',
        'int8[]', 'int4[]', 'int2[]',
        'boolean[]',
        'timestamptz[]', 'timestamp[]', 'date[]',
        'json[]', 'jsonb[]',
    ]
}

class EMTypeTests (unittest.TestCase):

    def test_allow_derived_types(self):
        t = em.make_type({
            "typename": "wacky[]",
            "is_array": True,
            "base_type": {
                "typename": "text",
            }
        })
        self.assertEqual('wacky[]', t.typename)
        self.assertEqual(em.builtin_types['text'], t.base_type)

        t = em.make_type({
            "typename": "wacky",
            "is_domain": True,
            "base_type": {
                "typename": "text",
            }
        })
        self.assertEqual('wacky', t.typename)
        self.assertEqual(em.builtin_types['text'], t.base_type)

    def test_require_builtin_base_types(self):
        with self.assertRaises(NotImplementedError):
            em.make_type({
                "typename": "wacky[]",
                "is_array": True,
                "base_type": {
                    "typename": "wacky",
                }
            })
        with self.assertRaises(NotImplementedError):
            em.make_type({
                "typename": "wacky",
                "is_domain": True,
                "base_type": {
                    "typename": "wacky",
                }
            })

    def test_scalar_basics(self):
        for typename in [
            'text',
            'float8', 'float4',
            'int8', 'int4', 'int2',
            'serial8', 'serial4', 'serial2',
            'boolean',
            'timestamptz', 'timestamp', 'date',
            'json', 'jsonb',
        ]:
            typ = em.builtin_types[typename]
            self.assertEqual(False, typ.is_array)
            self.assertEqual(False, typ.is_domain)
            self.assertEqual(typename, typ.typename)
            prejson = typ.prejson()
            self.assertEqual(['typename'], list(prejson.keys()))
            self.assertEqual(typename, prejson['typename'])
            ddl = typ.sqlite3_ddl()
            self.assertIn(ddl, {'text', 'real', 'integer', 'boolean', 'datetime', 'date', 'json'})

    def test_array_basics(self):
        for typename in [
            'text[]',
            'float8[]', 'float4[]',
            'int8[]', 'int4[]', 'int2[]',
            'boolean[]',
            'timestamptz[]', 'timestamp[]', 'date[]',
            'json[]', 'jsonb[]',
        ]:
            typ = em.builtin_types[typename]
            self.assertEqual(True, typ.is_array)
            self.assertEqual(False, typ.is_domain)
            self.assertEqual(typename, typ.typename)
            prejson = typ.prejson()
            self.assertEqual(['typename', 'is_array', 'base_type'], list(prejson.keys()))
            self.assertEqual(typename[:-2], typ.base_type.typename)
            ddl = typ.sqlite3_ddl()
            self.assertEqual('json', ddl)

    def test_domain_basics(self):
        for typename in [
            "markdown", "longtext",
            "ermrest_curie", "ermrest_uri",
            "color_rgb_hex",
            "ermrest_rid",
            "ermrest_rcb", "ermrest_rmb",
            "ermrest_rct", "ermrest_rmt",
        ]:
            typ = em.builtin_types[typename]
            self.assertEqual(False, typ.is_array)
            self.assertEqual(True, typ.is_domain)
            self.assertEqual(typename, typ.typename)
            prejson = typ.prejson()
            self.assertEqual(['typename', 'is_domain', 'base_type'], list(prejson.keys()))
            self.assertIn(typ.base_type.typename, {"text", "timestamptz"})
            ddl = typ.sqlite3_ddl()
            self.assertEqual(typ.base_type.sqlite3_ddl(), ddl)

    def test_py_checked(self) -> None:
        typenames: Sequence[str]
        cases: tuple[list[Any], list[Any]]
        for typenames, cases in _EMTypeTests_py_checked_cases.items():
            for typename in typenames:
                typ = em.builtin_types[typename]
                good, bad = cases
                for v in good:
                    self.assertEqual(v, typ.py_checked(v))
                for v in bad:
                    with self.assertRaises((TypeError, ValueError), msg=f"{typename=} {v=}"):
                        typ.py_checked(v)

    def test_text_convert(self):
        for typenames, cases in _EMTypeTests_text_convert_cases.items():
            for typename in typenames:
                typ = em.builtin_types[typename]
                for text, v in cases.items():
                    check = typ.py_to_text(v)
                    self.assertEqual(text, check, f"{typename=} py_to_text({v=!r}) -> {check=!r} != {text=!r}")
                    check = typ.text_to_py(text)
                    self.assertEqual(v, check, f"{typename=} text_to_py({text=}) -> {check=!r} != {text=}")

    def test_py_pre_json(self):
        for typenames, cases in _EMTypeTests_py_pre_json_cases.items():
            for typename in typenames:
                typ = em.builtin_types[typename]
                for native, prejson in cases:
                    check = typ.py_pre_json(native)
                    self.assertEqual(prejson, check, f"{typename=} py_pre_json({native=}) -> {check=} != {prejson=}")

class PgArrayTests (unittest.TestCase):

    def _test_pg_array_encode(self, pyl, pga, converter):
        guide = pga
        check = em.pgarray_encode(pyl, converter)
        self.assertEqual(guide, check)

    def _test_pg_array_decode(self, pga, pyl, converter):
        guide = pyl
        check = em.pgarray_decode(pga, converter)
        self.assertEqual(guide, check)

    _pyl_str = ["foo", "foo bar", "", None, "t", "f"]
    _pga_str = '{foo,"foo bar","",NULL,t,f}'

    def test_pg_array_encode_str(self):
        self._test_pg_array_encode(
            self._pyl_str,
            self._pga_str,
            lambda v: (v, False)
        )

    def test_pg_array_decode_str(self):
        self._test_pg_array_decode(
            self._pga_str,
            self._pyl_str,
            lambda s, q: s
        )

    _pyl_int = [-1, 0, 1, 2, 3, -1, 0, 1, 2, 3, None]
    _pga_int = '{-1,0,1,2,3,-1,0,1,2,3,NULL}'

    def test_pg_array_encode_int(self):
        self._test_pg_array_encode(
            self._pyl_int,
            self._pga_int,
            lambda v: (str(v), False)
        )

    def test_pg_array_decode_int(self):
        self._test_pg_array_decode(
            self._pga_int,
            self._pyl_int,
            lambda s, q: int(s)
        )

    _pyl_datetime = [
        datetime.datetime(2026, 1, 1, 12, 34, 56, 123456, tzinfo=datetime.timezone.utc),
        datetime.datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc),
        None,
    ]
    _pga_datetime = '{"2026-01-01 12:34:56.123456+00:00","1970-01-01 00:00:00+00:00",NULL}'

    def test_pg_array_encode_datetime(self):
        self._test_pg_array_encode(
            self._pyl_datetime,
            self._pga_datetime,
            lambda v: (em.datetime_to_timestamptz(v), False)
        )

    def test_pg_array_decode_datetime(self):
        self._test_pg_array_decode(
            self._pga_datetime,
            self._pyl_datetime,
            lambda s, q: em.timestamptz_to_datetime(s)
        )

    _pyl_json = [True, False, True, False, None, None, 1, 1.2, 1, 1.2, "one", "a b", "a\nb", {}, [], [1, 2, 3], {"a": 1, "b": 2}]
    _pga_json = r'{true,false,true,false,NULL,NULL,1,1.2,1,1.2,"\"one\"","\"a b\"","\"a\\nb\"","{}",[],"[1, 2, 3]","{\"a\": 1, \"b\": 2}"}'

    def test_pg_array_encode_json(self):
        self._test_pg_array_encode(
            self._pyl_json,
            self._pga_json,
            lambda v: (json.dumps(v), False)
        )

    def test_pg_array_decode_json(self):
        self._test_pg_array_decode(
            self._pga_json,
            self._pyl_json,
            lambda s, q: json.loads(s)
        )

class ErmrestTimestampCodingTests (unittest.TestCase):
    _basic_equivalents = [
        # datetime, isostr, usecs, snap
        (
            datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc),
            "1970-01-01 00:00:00+00:00",
            0,
            '0',
        ),
        (
            datetime.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc),
            "1969-12-31 23:59:59.999999+00:00",
            -1,
            'Z-ZZZZ-ZZZZ-ZZZY',
        ),
        (
            datetime.datetime(1900, 1, 1, tzinfo=datetime.timezone.utc),
            '1900-01-01 00:00:00+00:00',
            -2208988800000000,
            'Z-ZW2D-VXQ8-DG00',
        ),
        (
            datetime.datetime(2030, 12, 31, 12, 34, 56, 789012, tzinfo=datetime.timezone.utc),
            '2030-12-31 12:34:56.789012+00:00',
            1924950896789012,
            '3DD-EWED-7318',
        ),
    ]

    def test_fromiso_denorm(self):
        for i in range(8):
            us = 789012 // 10**i * 10**i
            dt = datetime.datetime(2030, 12, 31, 12, 34, 56, us, tzinfo=datetime.timezone.utc)
            ts = '2030-12-31 12:34:56%s+00:00' % (
                ('.%d' % us).rstrip('0') if us else ''
            )
            self.assertEqual(dt, em.timestamptz_to_datetime(ts), f"{dt=} {ts=}")

    def test_timestamptz_to_datetime(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(dt, em.timestamptz_to_datetime(ts), f"{dt=} {ts=}")

    def test_datetime_to_timestamptz(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(ts, em.datetime_to_timestamptz(dt), f"{ts=} {dt=}")

    def test_timestamptz_to_usecs(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(usecs, datetime_to_epoch_microseconds(em.timestamptz_to_datetime(ts)), f"{usecs=} {ts=}")

    def test_usecs_to_timestamptz(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(ts, em.datetime_to_timestamptz(epoch_microseconds_to_datetime(usecs)), f"{ts=} {usecs=}")

    def test_usecs_to_snaptime(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(snap, em.epoch_microseconds_to_snaptime(usecs), f"{snap=} {usecs=}")

    def test_snap_to_usecs(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(usecs, em.snaptime_to_epoch_microseconds(snap), f"{usecs=} {snap=}")

    def test_datetime_to_snaptime(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(snap, em.datetime_to_snaptime(dt), f"{snap=} {dt=}")

    def test_snaptime_to_datetime(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(dt, em.snaptime_to_datetime(snap), f"{dt=} {snap=}")

    def test_timestamptz_to_snaptime(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(snap, em.timestamptz_to_snaptime(ts), f"{snap=} {ts=}")

    def test_snaptime_to_timestamptz(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(ts, em.snaptime_to_timestamptz(snap), f"{ts=} {snap=}")

class ErmrestTableDecodeTests (unittest.TestCase):

    # NOTE: all whitespace is signficant in multiline text blocks
    # so do not reformat or pretty-indent the trailing quotes etc!

    def _test_csv_generic(self, table, csv_pgarray_str, csv_jsonarray_str, py_data):
        def check_value(rownum, cname, val):
            self.assertEqual(py_data[rownum][cname], val, f"py_data[{rownum}][{cname}]={py_data[rownum][cname]!r} != {val=!r}")

        def check_decode(csv_data_str):
            # test tuple output mode
            column_names = None
            tuple_row_count = 0
            for row in table.csv_file_decode(io.StringIO(csv_data_str, newline=''), use_dicts=False):
                column_count = len(py_data[0])
                # digest header row
                if column_names is None:
                    column_names = list(row)
                    self.assertEqual(column_count, len(column_names))
                    for cname in column_names:
                        self.assertIn(cname, table.columns.elements)
                    continue
                # regular data rows
                self.assertEqual(column_count, len(row))
                for i in range(column_count):
                    check_value(tuple_row_count, column_names[i], row[i])
                tuple_row_count += 1
            self.assertEqual(len(py_data), tuple_row_count)

            # test default use_dicts=True mode
            dict_row_count = 0
            for row in table.csv_file_decode(io.StringIO(csv_data_str, newline='')):
                self.assertIsInstance(row, dict)
                self.assertEqual(column_count, len(row.keys()))
                for cname in row.keys():
                    self.assertIn(cname, table.columns.elements)
                    check_value(dict_row_count, cname, row[cname])
                dict_row_count += 1
            self.assertEqual(len(py_data), dict_row_count)

        def check_encode(csv_data_str, use_pgarrays):
            outfile = io.StringIO('', newline='')
            table.csv_file_encode(outfile, py_data, use_pgarrays=use_pgarrays)
            outfile.seek(0)
            encoded_str = outfile.read()
            self.maxDiff = None
            self.assertEqual(csv_data_str.replace('\r\n', '\n'), encoded_str.replace('\r\n', '\n'))

        check_decode(csv_pgarray_str)
        check_decode(csv_jsonarray_str)
        check_encode(csv_pgarray_str, True)
        check_encode(csv_jsonarray_str, False)

    def _test_json_generic(self, table, json_rows, py_rows):
        def assertEqualRows(testname, guide, check):
            self.assertEqual(len(guide), len(check))
            for guide, check in zip(guide, check):
                self.assertEqual(set(guide.keys()), set(check.keys()))
                for cname in guide.keys():
                    self.assertEqual(guide[cname], check[cname])

        pre_json = list(table.rows_pre_json(py_rows))
        assertEqualRows('rows_pre_json()', json_rows, pre_json)
        post_json = list(table.rows_post_json(json_rows))
        assertEqualRows('rows_post_json()', py_rows, post_json)

    # realistic table def and data adapted from facebase public content
    _realistic_table = em.Table(
        None, 'anatomy',
        {
            "schema_name":"vocab",
            "table_name":"anatomy",
            "column_definitions":[
                {"name":"RID", "type": em.builtin_types['ermrest_rid'].prejson()},
                {"name":"RCT", "type": em.builtin_types['ermrest_rct'].prejson()},
                {"name":"RMT", "type": em.builtin_types['ermrest_rmt'].prejson()},
                {"name":"RCB", "type": em.builtin_types['ermrest_rcb'].prejson()},
                {"name":"RMB", "type": em.builtin_types['ermrest_rmb'].prejson()},
                {"name":"id", "type": em.builtin_types['ermrest_curie'].prejson()},
                {"name":"uri", "type": em.builtin_types['ermrest_uri'].prejson()},
                {"name":"name", "type": em.builtin_types['text'].prejson()},
                {"name":"description", "type": em.builtin_types['markdown'].prejson()},
                {"name":"synonyms", "type": em.builtin_types["text[]"].prejson()},
                {"name":"alternate_ids", "type": em.builtin_types["text[]"].prejson()},
            ]
        }
    )

    _realistic_data = [
        {
            "RID":"1-4FD8",
            "RCT": datetime.datetime.fromisoformat("2018-11-28T00:49:40.407895+00:00"),
            "RMT": datetime.datetime.fromisoformat("2020-11-06T22:03:19.113675+00:00"),
            "RCB":"https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f",
            "RMB":"https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f",
            "id":"UBERON:0005868",
            "uri":"http://purl.obolibrary.org/obo/UBERON_0005868",
            "name":"maxillary prominence",
            "description":"the paired dorsal prominences formed by bifurcation of the first pharyngeal arches in the embryo that unite with the ipsilateral medial nasal process to form the upper jaw",
            "synonyms":["embryonic maxillary process","maxillary process","maxillary process of embryo","prominentia maxilaris"],
            "alternate_ids":["ISBN:0-683-40008-8","MP:0010940","NCIT:C34206","EHDAA:5877","EHDAA2:0001070","EMAPA:17359","FMA:293049","http://linkedlifedata.com/resource/umls/id/C1513037","http://www.snomedbrowser.com/Codes/Details/346355001","Maxillary:prominence","UMLS:C1513037"]
        },
        {
            "RID":"1-4FBM",
            "RCT": datetime.datetime.fromisoformat("2018-11-28T00:49:40.407895+00:00"),
            "RMT": datetime.datetime.fromisoformat("2020-11-06T21:40:33.502879+00:00"),
            "RCB":"https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f",
            "RMB":"https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f",
            "id":"UBERON:0001676",
            "uri":"http://purl.obolibrary.org/obo/UBERON_0001676",
            "name":"occipital bone",
            "description":"the bone at the lower, posterior part of the skull",
            "synonyms":["occipital complex","occipital squama","os occipitale"],
            "alternate_ids":["ISBN:0-683-40008-8","MP:0005269","NCIT:C12757","EMAPA:25112","FMA:52735","GAID:227","http://linkedlifedata.com/resource/umls/id/C0028784","http://www.snomedbrowser.com/Codes/Details/181796003","MA:0001468","MESH:D009777","Occipital:bone","OpenCyc:Mx4rwQtsiZwpEbGdrcN5Y29ycA","UMLS:C0028784"]
        }
    ]

    # massaged to replace "T" with " " in ISO timestamps for easier equality tests
    _realistic_json = [{"RID":"1-4FD8","RCT":"2018-11-28 00:49:40.407895+00:00","RMT":"2020-11-06 22:03:19.113675+00:00","RCB":"https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f","RMB":"https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f","id":"UBERON:0005868","uri":"http://purl.obolibrary.org/obo/UBERON_0005868","name":"maxillary prominence","description":"the paired dorsal prominences formed by bifurcation of the first pharyngeal arches in the embryo that unite with the ipsilateral medial nasal process to form the upper jaw","synonyms":["embryonic maxillary process","maxillary process","maxillary process of embryo","prominentia maxilaris"],"alternate_ids":["ISBN:0-683-40008-8","MP:0010940","NCIT:C34206","EHDAA:5877","EHDAA2:0001070","EMAPA:17359","FMA:293049","http://linkedlifedata.com/resource/umls/id/C1513037","http://www.snomedbrowser.com/Codes/Details/346355001","Maxillary:prominence","UMLS:C1513037"]},
 {"RID":"1-4FBM","RCT":"2018-11-28 00:49:40.407895+00:00","RMT":"2020-11-06 21:40:33.502879+00:00","RCB":"https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f","RMB":"https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f","id":"UBERON:0001676","uri":"http://purl.obolibrary.org/obo/UBERON_0001676","name":"occipital bone","description":"the bone at the lower, posterior part of the skull","synonyms":["occipital complex","occipital squama","os occipitale"],"alternate_ids":["ISBN:0-683-40008-8","MP:0005269","NCIT:C12757","EMAPA:25112","FMA:52735","GAID:227","http://linkedlifedata.com/resource/umls/id/C0028784","http://www.snomedbrowser.com/Codes/Details/181796003","MA:0001468","MESH:D009777","Occipital:bone","OpenCyc:Mx4rwQtsiZwpEbGdrcN5Y29ycA","UMLS:C0028784"]}]

    _realistic_csv_pgarray = '''RID,RCT,RMT,RCB,RMB,id,uri,name,description,synonyms,alternate_ids
1-4FD8,2018-11-28 00:49:40.407895+00:00,2020-11-06 22:03:19.113675+00:00,https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f,https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f,UBERON:0005868,http://purl.obolibrary.org/obo/UBERON_0005868,maxillary prominence,the paired dorsal prominences formed by bifurcation of the first pharyngeal arches in the embryo that unite with the ipsilateral medial nasal process to form the upper jaw,"{""embryonic maxillary process"",""maxillary process"",""maxillary process of embryo"",""prominentia maxilaris""}","{ISBN:0-683-40008-8,MP:0010940,NCIT:C34206,EHDAA:5877,EHDAA2:0001070,EMAPA:17359,FMA:293049,http://linkedlifedata.com/resource/umls/id/C1513037,http://www.snomedbrowser.com/Codes/Details/346355001,Maxillary:prominence,UMLS:C1513037}"
1-4FBM,2018-11-28 00:49:40.407895+00:00,2020-11-06 21:40:33.502879+00:00,https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f,https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f,UBERON:0001676,http://purl.obolibrary.org/obo/UBERON_0001676,occipital bone,"the bone at the lower, posterior part of the skull","{""occipital complex"",""occipital squama"",""os occipitale""}","{ISBN:0-683-40008-8,MP:0005269,NCIT:C12757,EMAPA:25112,FMA:52735,GAID:227,http://linkedlifedata.com/resource/umls/id/C0028784,http://www.snomedbrowser.com/Codes/Details/181796003,MA:0001468,MESH:D009777,Occipital:bone,OpenCyc:Mx4rwQtsiZwpEbGdrcN5Y29ycA,UMLS:C0028784}"
'''
    _realistic_csv_jsonarray = '''RID,RCT,RMT,RCB,RMB,id,uri,name,description,synonyms,alternate_ids
1-4FD8,2018-11-28 00:49:40.407895+00:00,2020-11-06 22:03:19.113675+00:00,https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f,https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f,UBERON:0005868,http://purl.obolibrary.org/obo/UBERON_0005868,maxillary prominence,the paired dorsal prominences formed by bifurcation of the first pharyngeal arches in the embryo that unite with the ipsilateral medial nasal process to form the upper jaw,"[""embryonic maxillary process"",""maxillary process"",""maxillary process of embryo"",""prominentia maxilaris""]","[""ISBN:0-683-40008-8"",""MP:0010940"",""NCIT:C34206"",""EHDAA:5877"",""EHDAA2:0001070"",""EMAPA:17359"",""FMA:293049"",""http://linkedlifedata.com/resource/umls/id/C1513037"",""http://www.snomedbrowser.com/Codes/Details/346355001"",""Maxillary:prominence"",""UMLS:C1513037""]"
1-4FBM,2018-11-28 00:49:40.407895+00:00,2020-11-06 21:40:33.502879+00:00,https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f,https://auth.globus.org/b506963e-d274-11e5-99f0-67ee73dd4c3f,UBERON:0001676,http://purl.obolibrary.org/obo/UBERON_0001676,occipital bone,"the bone at the lower, posterior part of the skull","[""occipital complex"",""occipital squama"",""os occipitale""]","[""ISBN:0-683-40008-8"",""MP:0005269"",""NCIT:C12757"",""EMAPA:25112"",""FMA:52735"",""GAID:227"",""http://linkedlifedata.com/resource/umls/id/C0028784"",""http://www.snomedbrowser.com/Codes/Details/181796003"",""MA:0001468"",""MESH:D009777"",""Occipital:bone"",""OpenCyc:Mx4rwQtsiZwpEbGdrcN5Y29ycA"",""UMLS:C0028784""]"
'''
    def test_csv_realistic(self):
        self._test_csv_generic(self._realistic_table, self._realistic_csv_pgarray, self._realistic_csv_jsonarray, self._realistic_data)

    def test_json_realistic(self):
        self._test_json_generic(self._realistic_table, self._realistic_json, self._realistic_data)

    _synthetic_table = em.Table(
        None, 'test',
        {
            "schema_name":"test",
            "table_name":"test",
            "column_definitions":[
                {"name": typname, "type": em.builtin_types[typname].prejson()}
                for typname in [
                    # scalars
                    'text',
                    'float8',
                    'float4',
                    'int8',
                    'int4',
                    'int2',
                    'boolean',
                    'timestamptz',
                    'timestamp',
                    'date',
                    'json',
                    'jsonb',
                    # domains
                    'ermrest_rid', # domain over text
                    'ermrest_rct', # domain over timestamptz
                    # arrays
                    'text[]',
                    'float8[]',
                    'float4[]',
                    'int8[]',
                    'int4[]',
                    'int2[]',
                    'boolean[]',
                    'timestamptz[]',
                    'timestamp[]',
                    'date[]',
                    'json[]',
                    'jsonb[]',
                ]
            ]
        }
    )

    _synthetic_data = [
        {
            "text": "foo",
            "float8": math.pi,
            "float4": 1000.52,
            "int8": 2**63 - 1,
            "int4": 2**31 - 1,
            "int2": 2**15 - 1,
            "boolean": True,
            "timestamptz": datetime.datetime.fromisoformat("2020-01-01 12:34:56.678901+00:00"),
            "timestamp": datetime.datetime.fromisoformat("2020-01-01 12:34:56.678901"),
            "date": datetime.date.fromisoformat("2020-01-01"),
            "json": [None, 1, 1.0, "one", [2, "two"], {"three": 3}, "2020-01-01 12:34:56.678901+00:00"],
            "jsonb": [None, 1, 1.0, "one", [2, "two"], {"three": 3}, "2020-01-01 12:34:56.678901+00:00"],
            "ermrest_rid": "1-AAAA",
            "ermrest_rct": datetime.datetime.fromisoformat("2020-01-01 12:34:56.678901+00:00"),
            "text[]": [None, "foo", "foo\nbar"],
            "float8[]": [None, 1.0, math.pi],
            "float4[]": [None, 1000.52, -32.0],
            "int8[]": [None, - 2**63, -1, 0, 1, 2**63 - 1],
            "int4[]": [None, - 2**31, -1, 0, 1, 2**31 - 1],
            "int2[]": [None, - 2**15, -1, 0, 1, 2**15 - 1],
            "boolean[]": [None, True, False],
            "timestamptz[]": [None, datetime.datetime.fromisoformat("2020-01-01 12:34:56.678901+00:00")],
            "timestamp[]": [None, datetime.datetime.fromisoformat("2020-01-01 12:34:56.678901")],
            "date[]": [None, datetime.date.fromisoformat("2020-01-01")],
            "json[]": [None, "", [], {}, 1, 1.0, True, "one", [None, "", 1, 1.0, True, "one", [2, "two"], {"three": 3}, "2020-01-01 12:34:56.678901+00:00"]],
            "jsonb[]": [None, "", [], {},1, 1.0, True, "one", [None, "", 1, 1.0, True, "one", [2, "two"], {"three": 3}, "2020-01-01 12:34:56.678901+00:00"]],
        },
        {
            "text": None, # we cannot handle empty strings via csv.reader()...
            "float8": 0.0,
            "float4": 0.0,
            "int8": 0,
            "int4": 0,
            "int2": 0,
            "boolean": False,
            "timestamptz": datetime.datetime.fromisoformat("1970-01-01 00:00:00+00:00"),
            "timestamp": datetime.datetime.fromisoformat("1970-01-01 00:00:00"),
            "date": datetime.date.fromisoformat("1970-01-01"),
            "json": [],
            "jsonb": "",
            "ermrest_rid": "0",
            "ermrest_rct": datetime.datetime.fromisoformat("1970-01-01 00:00:00+00:00"),
            "text[]": [],
            "float8[]": [],
            "float4[]": [],
            "int8[]": [],
            "int4[]": [],
            "int2[]": [],
            "boolean[]": [],
            "timestamptz[]": [],
            "timestamp[]": [],
            "date[]": [],
            "json[]": [],
            "jsonb[]": [],
        },
        {
            "text": None,
            "float8": None,
            "float4": None,
            "int8": None,
            "int4": None,
            "int2": None,
            "boolean": None,
            "timestamptz": None,
            "timestamp": None,
            "date": None,
            "json": None,
            "jsonb": None,
            "ermrest_rid": None,
            "ermrest_rct": None,
            "text[]": None,
            "float8[]": None,
            "float4[]": None,
            "int8[]": None,
            "int4[]": None,
            "int2[]": None,
            "boolean[]": None,
            "timestamptz[]": None,
            "timestamp[]": None,
            "date[]": None,
            "json[]": None,
            "jsonb[]": None,
        },
    ]
    _synthetic_json = [
        {
            "text": "foo",
            "float8": math.pi,
            "float4": 1000.52,
            "int8": 2**63 - 1,
            "int4": 2**31 - 1,
            "int2": 2**15 - 1,
            "boolean": True,
            "timestamptz": "2020-01-01 12:34:56.678901+00:00",
            "timestamp": "2020-01-01 12:34:56.678901",
            "date": "2020-01-01",
            "json": [None, 1, 1.0, "one", [2, "two"], {"three": 3}, "2020-01-01 12:34:56.678901+00:00"],
            "jsonb": [None, 1, 1.0, "one", [2, "two"], {"three": 3}, "2020-01-01 12:34:56.678901+00:00"],
            "ermrest_rid": "1-AAAA",
            "ermrest_rct": "2020-01-01 12:34:56.678901+00:00",
            "text[]": [None, "foo", "foo\nbar"],
            "float8[]": [None, 1.0, math.pi],
            "float4[]": [None, 1000.52, -32.0],
            "int8[]": [None, - 2**63, -1, 0, 1, 2**63 - 1],
            "int4[]": [None, - 2**31, -1, 0, 1, 2**31 - 1],
            "int2[]": [None, - 2**15, -1, 0, 1, 2**15 - 1],
            "boolean[]": [None, True, False],
            "timestamptz[]": [None, "2020-01-01 12:34:56.678901+00:00"],
            "timestamp[]": [None, "2020-01-01 12:34:56.678901"],
            "date[]": [None, "2020-01-01"],
            "json[]": [None, "", [], {}, 1, 1.0, True, "one", [None, "", 1, 1.0, True, "one", [2, "two"], {"three": 3}, "2020-01-01 12:34:56.678901+00:00"]],
            "jsonb[]": [None, "", [], {},1, 1.0, True, "one", [None, "", 1, 1.0, True, "one", [2, "two"], {"three": 3}, "2020-01-01 12:34:56.678901+00:00"]],
        },
        {
            "text": None,
            "float8": 0.0,
            "float4": 0.0,
            "int8": 0,
            "int4": 0,
            "int2": 0,
            "boolean": False,
            "timestamptz": "1970-01-01 00:00:00+00:00",
            "timestamp": "1970-01-01 00:00:00",
            "date": "1970-01-01",
            "json": [],
            "jsonb": "",
            "ermrest_rid": "0",
            "ermrest_rct": "1970-01-01 00:00:00+00:00",
            "text[]": [],
            "float8[]": [],
            "float4[]": [],
            "int8[]": [],
            "int4[]": [],
            "int2[]": [],
            "boolean[]": [],
            "timestamptz[]": [],
            "timestamp[]": [],
            "date[]": [],
            "json[]": [],
            "jsonb[]": [],
        },
        {
            "text": None,
            "float8": None,
            "float4": None,
            "int8": None,
            "int4": None,
            "int2": None,
            "boolean": None,
            "timestamptz": None,
            "timestamp": None,
            "date": None,
            "json": None,
            "jsonb": None,
            "ermrest_rid": None,
            "ermrest_rct": None,
            "text[]": None,
            "float8[]": None,
            "float4[]": None,
            "int8[]": None,
            "int4[]": None,
            "int2[]": None,
            "boolean[]": None,
            "timestamptz[]": None,
            "timestamp[]": None,
            "date[]": None,
            "json[]": None,
            "jsonb[]": None,
        },
    ]

    _synthetic_csv_pgarray = '''text,float8,float4,int8,int4,int2,boolean,timestamptz,timestamp,date,json,jsonb,ermrest_rid,ermrest_rct,text[],float8[],float4[],int8[],int4[],int2[],boolean[],timestamptz[],timestamp[],date[],json[],jsonb[]
foo,3.141592653589793,1000.52,9223372036854775807,2147483647,32767,true,2020-01-01 12:34:56.678901+00:00,2020-01-01 12:34:56.678901,2020-01-01,"[null,1,1.0,""one"",[2,""two""],{""three"":3},""2020-01-01 12:34:56.678901+00:00""]","[null,1,1.0,""one"",[2,""two""],{""three"":3},""2020-01-01 12:34:56.678901+00:00""]",1-AAAA,2020-01-01 12:34:56.678901+00:00,"{NULL,foo,""foo\nbar""}","{NULL,1.0,3.141592653589793}","{NULL,1000.52,-32.0}","{NULL,-9223372036854775808,-1,0,1,9223372036854775807}","{NULL,-2147483648,-1,0,1,2147483647}","{NULL,-32768,-1,0,1,32767}","{NULL,true,false}","{NULL,""2020-01-01 12:34:56.678901+00:00""}","{NULL,""2020-01-01 12:34:56.678901""}","{NULL,2020-01-01}","{NULL,""\\""\\"""",[],""{}"",1,1.0,true,""\\""one\\"""",""[null,\\""\\"",1,1.0,true,\\""one\\"",[2,\\""two\\""],{\\""three\\"":3},\\""2020-01-01 12:34:56.678901+00:00\\""]""}","{NULL,""\\""\\"""",[],""{}"",1,1.0,true,""\\""one\\"""",""[null,\\""\\"",1,1.0,true,\\""one\\"",[2,\\""two\\""],{\\""three\\"":3},\\""2020-01-01 12:34:56.678901+00:00\\""]""}"
,0.0,0.0,0,0,0,false,1970-01-01 00:00:00+00:00,1970-01-01 00:00:00,1970-01-01,[],"""""",0,1970-01-01 00:00:00+00:00,{},{},{},{},{},{},{},{},{},{},{},{}
,,,,,,,,,,,,,,,,,,,,,,,,,
'''
    _synthetic_csv_jsonarray = '''text,float8,float4,int8,int4,int2,boolean,timestamptz,timestamp,date,json,jsonb,ermrest_rid,ermrest_rct,text[],float8[],float4[],int8[],int4[],int2[],boolean[],timestamptz[],timestamp[],date[],json[],jsonb[]
foo,3.141592653589793,1000.52,9223372036854775807,2147483647,32767,true,2020-01-01 12:34:56.678901+00:00,2020-01-01 12:34:56.678901,2020-01-01,"[null,1,1.0,""one"",[2,""two""],{""three"":3},""2020-01-01 12:34:56.678901+00:00""]","[null,1,1.0,""one"",[2,""two""],{""three"":3},""2020-01-01 12:34:56.678901+00:00""]",1-AAAA,2020-01-01 12:34:56.678901+00:00,"[null,""foo"",""foo\\nbar""]","[null,1.0,3.141592653589793]","[null,1000.52,-32.0]","[null,-9223372036854775808,-1,0,1,9223372036854775807]","[null,-2147483648,-1,0,1,2147483647]","[null,-32768,-1,0,1,32767]","[null,true,false]","[null,""2020-01-01 12:34:56.678901+00:00""]","[null,""2020-01-01 12:34:56.678901""]","[null,""2020-01-01""]","[null,"""",[],{},1,1.0,true,""one"",[null,"""",1,1.0,true,""one"",[2,""two""],{""three"":3},""2020-01-01 12:34:56.678901+00:00""]]","[null,"""",[],{},1,1.0,true,""one"",[null,"""",1,1.0,true,""one"",[2,""two""],{""three"":3},""2020-01-01 12:34:56.678901+00:00""]]"
,0.0,0.0,0,0,0,false,1970-01-01 00:00:00+00:00,1970-01-01 00:00:00,1970-01-01,[],"""""",0,1970-01-01 00:00:00+00:00,[],[],[],[],[],[],[],[],[],[],[],[]
,,,,,,,,,,,,,,,,,,,,,,,,,
'''

    def test_csv_synthetic(self):
        self._test_csv_generic(self._synthetic_table, self._synthetic_csv_pgarray, self._synthetic_csv_jsonarray, self._synthetic_data)

    def test_json_synthetic(self):
        self._test_json_generic(self._synthetic_table, self._synthetic_json, self._synthetic_data)

@unittest.skipUnless(hostname, "Test host not specified")
class ErmrestModelTests (unittest.TestCase):

    catalog = None

    @classmethod
    def _purgeCatalog(cls):
        model = cls.catalog.getCatalogModel()

        # exclude the 'public' schema
        schemas = [s for s in model.schemas.values() if s.name != 'public']

        # drop all fkeys
        for s in schemas:
            for t in s.tables.values():
                for fk in list(t.foreign_keys):
                    fk.drop()

        # drop all tables and schemas
        for s in list(schemas):
            for t in list(s.tables.values()):
                t.drop()
            s.drop()

    @classmethod
    def setUpClass(cls):
        credential = os.getenv("DERIVA_PY_TEST_CREDENTIAL") or get_credential(hostname)
        server = DerivaServer('https', hostname, credentials=credential)
        catalog_id = os.getenv("DERIVA_PY_TEST_CATALOG")
        if catalog_id is not None:
            logger.info(f"Reusing catalog {catalog_id} on host {hostname}")
            cls.catalog = server.connect_ermrest(catalog_id)
            cls._purgeCatalog()
        else:
            cls.catalog = server.create_ermrest_catalog()
            logger.info(f"Created catalog {cls.catalog.catalog_id} on host {hostname}")

    @classmethod
    def tearDownClass(cls):
        if cls.catalog and os.getenv("DERIVA_PY_TEST_CATALOG") is None:
            logger.info(f"Deleting catalog {cls.catalog.catalog_id} on host {hostname}")
            cls.catalog.delete_ermrest_catalog(really=True)

    def setUp(self):
        self.model = self.catalog.getCatalogModel()

    def tearDown(self):
        self._purgeCatalog()

    def _create_schema_with_fkeys(self):
        """Creates a simple schema of two tables with a fkey relationship from child to parent."""

        # build a single, low-level catalog /schema POST operation
        # should be (slightly) faster and avoids using the client APIs under test in this module
        schema_def = ermrest_model.Schema.define('schema_with_fkeys')
        schema_def["tables"] = {
            "parent": ermrest_model.Table.define(
                'parent',
                column_defs=[
                    ermrest_model.Column.define('id', ermrest_model.builtin_types.text),
                    ermrest_model.Column.define('id_extra', ermrest_model.builtin_types.text),
                ],
                key_defs=[
                    ermrest_model.Key.define(['id'], constraint_name='parent_id_key'),
                    ermrest_model.Key.define(['id', 'id_extra'], constraint_name='parent_compound_key'),
                ]
            ),
            "child": ermrest_model.Table.define(
                'child',
                column_defs=[
                    ermrest_model.Column.define('parent_id', ermrest_model.builtin_types.text),
                    ermrest_model.Column.define('parent_id_extra', ermrest_model.builtin_types.text),
                ],
                fkey_defs=[
                    ermrest_model.ForeignKey.define(
                        ['parent_id'], 'schema_with_fkeys', 'parent', ['id']
                    ),
                    ermrest_model.ForeignKey.define(
                        ['parent_id_extra', 'parent_id'], 'schema_with_fkeys', 'parent', ['id_extra', 'id']
                    )
                ]
            ),
        }
        self.catalog.post('/schema', json=[schema_def])
        # refresh the local state of the model
        self.model = self.catalog.getCatalogModel()

    def test_0a_schema_define_defaults(self):
        sname = "test_schema"
        sdef = ermrest_model.Schema.define(sname)
        self.assertEqual(sdef.get("schema_name"), sname)
        self.assertEqual(sdef.get("comment"), None)
        self.assertEqual(sdef.get("acls"), dict())
        self.assertEqual(sdef.get("annotations"), dict())

    _test_comment = "my comment"
    _test_acls = {
        "insert": ["a"],
        "update": ["b"],
    }
    _test_acl_bindings: dict[str, Any] = {
    }
    _test_annotations = {"tag1": "value1"}

    def test_0b_schema_define_custom(self):
        sname = "test_schema"
        sdef = ermrest_model.Schema.define(sname, self._test_comment, self._test_acls, self._test_annotations)
        self.assertEqual(sdef.get("schema_name"), sname)
        self.assertEqual(sdef.get("comment"), self._test_comment)
        self.assertEqual(sdef.get("acls"), self._test_acls)
        self.assertEqual(sdef.get("annotations"), self._test_annotations)

    def test_1a_column_define_defaults(self):
        cname = "test_column"
        cdef = ermrest_model.Column.define(cname, ermrest_model.builtin_types.text)
        self.assertEqual(cdef.get("name"), cname)
        ctype = cdef.get("type")
        self.assertIsInstance(ctype, dict)
        self.assertEqual(ctype.get("typename"), "text")
        self.assertEqual(cdef.get("nullok"), True)
        self.assertEqual(cdef.get("default"), None)
        self.assertEqual(cdef.get("comment"), None)
        self.assertEqual(cdef.get("acls"), dict())
        self.assertEqual(cdef.get("acl_bindings"), dict())
        self.assertEqual(cdef.get("annotations"), dict())

    def test_1b_column_define_custom(self):
        cname = "test_column"
        nullok = False
        default = "my default"
        cdef = ermrest_model.Column.define(
            cname, ermrest_model.builtin_types.text, nullok, default,
            self._test_comment, self._test_acls, self._test_acl_bindings, self._test_annotations,
        )
        self.assertEqual(cdef.get("name"), cname)
        ctype = cdef.get("type")
        self.assertIsInstance(ctype, dict)
        self.assertEqual(ctype.get("typename"), "text")
        self.assertEqual(cdef.get("nullok"), nullok)
        self.assertEqual(cdef.get("default"), default)
        self.assertEqual(cdef.get("comment"), self._test_comment)
        self.assertEqual(cdef.get("acls"), self._test_acls)
        self.assertEqual(cdef.get("acl_bindings"), self._test_acl_bindings)
        self.assertEqual(cdef.get("annotations"), self._test_annotations)

    def test_1c_key_define_defaults(self):
        cnames = ["id"]
        kdef = ermrest_model.Key.define(cnames)
        self.assertEqual(kdef.get("unique_columns"), cnames)
        self.assertEqual(not kdef.get("names"), True)
        self.assertEqual(kdef.get("comment"), None)
        self.assertEqual(kdef.get("annotations"), dict())

    def test_1d_key_define_custom(self):
        cnames = ["id"]
        constraint_name = "my_constraint"
        kdef = ermrest_model.Key.define(cnames, None, self._test_comment, self._test_annotations, constraint_name)
        self.assertEqual(kdef.get("unique_columns"), cnames)
        self.assertIsInstance(kdef.get("names"), list)
        self.assertIsInstance(kdef.get("names")[0], list)
        self.assertEqual(kdef.get("names")[0][1], constraint_name)
        self.assertEqual(kdef.get("comment"), self._test_comment)
        self.assertEqual(kdef.get("annotations"), self._test_annotations)

    def test_1e_fkey_define_defaults(self):
        fk_colnames = ["fk1"]
        pk_sname = "pk_schema"
        pk_tname = "pk_table"
        pk_colnames = ["RID"]
        fkdef = ermrest_model.ForeignKey.define(
            fk_colnames,
            pk_sname,
            pk_tname,
            pk_colnames,
        )
        self.assertEqual([ c.get("column_name") for c in fkdef.get("foreign_key_columns") ], fk_colnames)
        self.assertEqual([ c.get("column_name") for c in fkdef.get("referenced_columns") ], pk_colnames)
        self.assertEqual(all([ c.get("schema_name") == pk_sname for c in fkdef.get("referenced_columns") ]), True)
        self.assertEqual(all([ c.get("table_name") == pk_tname for c in fkdef.get("referenced_columns") ]), True)
        self.assertEqual(fkdef.get("on_update"), "NO ACTION")
        self.assertEqual(fkdef.get("on_delete"), "NO ACTION")
        self.assertEqual(not fkdef.get("names"), True)
        self.assertEqual(fkdef.get("comment"), None)
        self.assertEqual(fkdef.get("acls"), dict())
        self.assertEqual(fkdef.get("acl_bindings"), dict())
        self.assertEqual(fkdef.get("annotations"), dict())

    def test_1f_fkey_define_custom(self):
        fk_colnames = ["fk1"]
        pk_sname = "pk_schema"
        pk_tname = "pk_table"
        pk_colnames = ["RID"]
        constraint_name = "my_constraint"
        action1 = "CASCADE"
        action2 = "RESTRICT"
        fkdef = ermrest_model.ForeignKey.define(
            fk_colnames,
            pk_sname,
            pk_tname,
            pk_colnames,
            action1,
            action2,
            None,
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
            constraint_name,
        )
        self.assertEqual([ c.get("column_name") for c in fkdef.get("foreign_key_columns") ], fk_colnames)
        self.assertEqual([ c.get("column_name") for c in fkdef.get("referenced_columns") ], pk_colnames)
        self.assertEqual(all([ c.get("schema_name") == pk_sname for c in fkdef.get("referenced_columns") ]), True)
        self.assertEqual(all([ c.get("table_name") == pk_tname for c in fkdef.get("referenced_columns") ]), True)
        self.assertEqual(fkdef.get("on_update"), action1)
        self.assertEqual(fkdef.get("on_delete"), action2)
        self.assertIsInstance(fkdef.get("names"), list)
        self.assertIsInstance(fkdef.get("names")[0], list)
        self.assertEqual(fkdef.get("names")[0][1], constraint_name)
        self.assertEqual(fkdef.get("comment"), self._test_comment)
        self.assertEqual(fkdef.get("acls"), self._test_acls)
        self.assertEqual(fkdef.get("acl_bindings"), self._test_acl_bindings)
        self.assertEqual(fkdef.get("annotations"), self._test_annotations)

    def test_2a_table_define_defaults(self):
        tname = "test_table"
        tdef = ermrest_model.Table.define(
            tname,
            [
                ermrest_model.Column.define("id", ermrest_model.builtin_types.text),
            ],
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 1)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 2)
        self.assertEqual(tdef.get("comment"), None)
        self.assertEqual(tdef.get("acls"), dict())
        self.assertEqual(tdef.get("acl_bindings"), dict())
        self.assertEqual(tdef.get("annotations"), dict())
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_2b_table_define_custom(self):
        tname = "test_table"
        tdef = ermrest_model.Table.define(
            tname,
            [
                ermrest_model.Column.define("id", ermrest_model.builtin_types.text),
                ermrest_model.Column.define("fk1", ermrest_model.builtin_types.text),
            ],
            [
                ermrest_model.Key.define(["id"]),
            ],
            [
                ermrest_model.ForeignKey.define(["fk1"], "public", "ERMrest_Client", ["RID"]),
            ],
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
            provide_system_fkeys=False,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 2)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 1)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 1)
        self.assertEqual(tdef.get("comment"), self._test_comment)
        self.assertEqual(tdef.get("acls"), self._test_acls)
        self.assertEqual(tdef.get("acl_bindings"), self._test_acl_bindings)
        self.assertEqual(tdef.get("annotations"), self._test_annotations)
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_2c_table_define_with_reference(self):
        tname = "test_table"
        tdef = ermrest_model.Table.define(
            tname,
            [
                ermrest_model.Column.define("id", ermrest_model.builtin_types.text),
                self.model.schemas["public"].tables["ERMrest_Client"],
            ],
            [],
            [],
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 2)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 2 + 1)
        self.assertEqual(
            {
                (
                    fkdef["referenced_columns"][0]["schema_name"],
                    fkdef["referenced_columns"][0]["table_name"],
                    frozenset(zip(
                        ( c["column_name"] for c in fkdef["foreign_key_columns"] ),
                        ( c["column_name"] for c in fkdef["referenced_columns"] ),
                    ))
                )
                for fkdef in fkdefs
             },
            {
                ("public", "ERMrest_Client", frozenset([ ("ERMrest_Client", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RCB", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RMB", "ID",) ])),
            }
        )
        self.assertEqual(tdef.get("comment"), self._test_comment)
        self.assertEqual(tdef.get("acls"), self._test_acls)
        self.assertEqual(tdef.get("acl_bindings"), self._test_acl_bindings)
        self.assertEqual(tdef.get("annotations"), self._test_annotations)
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_3a_vocab_define_defaults(self):
        tname = "test_table"
        curie_template = "test:{RID}"
        tdef = ermrest_model.Table.define_vocabulary(
            tname,
            curie_template,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 5)
        self.assertEqual({ c["name"] for c in cdefs }, {"RID","RCT","RMT","RCB","RMB","ID","Name","Description","Synonyms","URI"})
        cdefs = { c["name"]: c for c in cdefs }
        self.assertEqual(cdefs["ID"]["default"], curie_template)
        self.assertEqual(cdefs["URI"]["default"], '/id/{RID}')
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 3)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 2)
        self.assertEqual(tdef.get("comment"), None)
        self.assertEqual(tdef.get("acls"), dict())
        self.assertEqual(tdef.get("acl_bindings"), dict())
        self.assertEqual(tdef.get("annotations"), dict())
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_3b_vocab_define_custom(self):
        tname = "test_table"
        curie_template = "test:{RID}"
        uri_template = "/id/1/{RID}"
        tdef = ermrest_model.Table.define_vocabulary(
            tname,
            curie_template,
            uri_template,
            [
                ermrest_model.Column.define("fk1", ermrest_model.builtin_types.text),
            ],
            [],
            [
                ermrest_model.ForeignKey.define(["fk1"], "public", "ERMrest_Client", ["RID"]),
            ],
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
            provide_name_key=False,
            provide_system_fkeys=False,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 5 + 1)
        self.assertEqual({ c["name"] for c in cdefs }, {"RID","RCT","RMT","RCB","RMB","ID","Name","Description","Synonyms","URI","fk1"})
        cdefs = { c["name"]: c for c in cdefs }
        self.assertEqual(cdefs["ID"]["default"], curie_template)
        self.assertEqual(cdefs["URI"]["default"], uri_template)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 2)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 1)
        self.assertEqual(tdef.get("comment"), self._test_comment)
        self.assertEqual(tdef.get("acls"), self._test_acls)
        self.assertEqual(tdef.get("acl_bindings"), self._test_acl_bindings)
        self.assertEqual(tdef.get("annotations"), self._test_annotations)
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_3c_vocab_define_with_reference(self):
        tname = "test_table"
        curie_template = "test:{RID}"
        uri_template = "/id/1/{RID}"
        target = self.model.schemas["public"].tables["ERMrest_Client"]
        self.assertIsInstance(target, ermrest_model.Table)
        tdef = ermrest_model.Table.define_vocabulary(
            tname,
            curie_template,
            uri_template,
            [ target, ],
            [],
            [],
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 5 + 1)
        self.assertEqual({ c["name"] for c in cdefs }, {"RID","RCT","RMT","RCB","RMB","ID","Name","Description","Synonyms","URI","ERMrest_Client"})
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 3)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 2 + 1)
        self.assertEqual(
            {
                (
                    fkdef["referenced_columns"][0]["schema_name"],
                    fkdef["referenced_columns"][0]["table_name"],
                    frozenset(zip(
                        ( c["column_name"] for c in fkdef["foreign_key_columns"] ),
                        ( c["column_name"] for c in fkdef["referenced_columns"] ),
                    ))
                )
                for fkdef in fkdefs
             },
            {
                ("public", "ERMrest_Client", frozenset([ ("ERMrest_Client", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RCB", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RMB", "ID",) ])),
            }
        )
        self.assertEqual(tdef.get("comment"), self._test_comment)
        self.assertEqual(tdef.get("acls"), self._test_acls)
        self.assertEqual(tdef.get("acl_bindings"), self._test_acl_bindings)
        self.assertEqual(tdef.get("annotations"), self._test_annotations)
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)  

    def test_4a_asset_define_defaults(self):
        tname = "test_table"
        tdef = ermrest_model.Table.define_asset(
            "model_define_schema",
            tname,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 5)
        self.assertEqual({ c["name"] for c in cdefs }, {"RID","RCT","RMT","RCB","RMB","Filename","Description","Length","MD5","URL"})
        cdefs = { c["name"]: c for c in cdefs }
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 1)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 2)
        self.assertEqual(tdef.get("acls"), dict())
        self.assertEqual(tdef.get("acl_bindings"), dict())
        self.assertEqual(set(tdef.get("annotations").keys()), {tag.table_display})
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_4b_asset_define_custom(self):
        tname = "test_table"
        hatrac_template = "/hatrac/foo/{{{MD5}}}.{{{Filename}}}"
        tdef = ermrest_model.Table.define_asset(
            "model_define_schema",
            tname,
            hatrac_template,
            [
                ermrest_model.Column.define("fk1", ermrest_model.builtin_types.text),
            ],
            [],
            [
                ermrest_model.ForeignKey.define(["fk1"], "public", "ERMrest_Client", ["RID"]),
            ],
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
            provide_system_fkeys=False,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 5 + 1)
        self.assertEqual({ c["name"] for c in cdefs }, {"RID","RCT","RMT","RCB","RMB","Filename","Description","Length","MD5","URL","fk1"})
        cdefs = { c["name"]: c for c in cdefs }
        self.assertEqual(cdefs["URL"]["annotations"][tag.asset]["url_pattern"], hatrac_template)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 1)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 1)
        self.assertEqual(tdef.get("comment"), self._test_comment)
        self.assertEqual(tdef.get("acls"), self._test_acls)
        self.assertEqual(tdef.get("acl_bindings"), self._test_acl_bindings)
        annotations = tdef.get("annotations")
        for k, v in self._test_annotations.items():
            self.assertEqual(annotations[k], v)
        self.assertIn(tag.table_display, annotations)
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_4c_asset_define_with_reference(self):
        tname = "test_table"
        hatrac_template = "/hatrac/foo/{{{MD5}}}.{{{Filename}}}"
        target = self.model.schemas["public"].tables["ERMrest_Client"]
        self.assertIsInstance(target, ermrest_model.Table)
        tdef = ermrest_model.Table.define_asset(
            "model_define_schema",
            tname,
            hatrac_template,
            [ target, ],
            [],
            [],
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 5 + 1)
        self.assertEqual({ c["name"] for c in cdefs }, {"RID","RCT","RMT","RCB","RMB","Filename","Description","Length","MD5","URL","ERMrest_Client"})
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 1)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 2 + 1)
        self.assertEqual(
            {
                (
                    fkdef["referenced_columns"][0]["schema_name"],
                    fkdef["referenced_columns"][0]["table_name"],
                    frozenset(zip(
                        ( c["column_name"] for c in fkdef["foreign_key_columns"] ),
                        ( c["column_name"] for c in fkdef["referenced_columns"] ),
                    ))
                )
                for fkdef in fkdefs
             },
            {
                ("public", "ERMrest_Client", frozenset([ ("ERMrest_Client", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RCB", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RMB", "ID",) ])),
            }
        )
        self.assertEqual(tdef.get("comment"), self._test_comment)
        self.assertEqual(tdef.get("acls"), self._test_acls)
        self.assertEqual(tdef.get("acl_bindings"), self._test_acl_bindings)
        annotations = tdef.get("annotations")
        for k, v in self._test_annotations.items():
            self.assertEqual(annotations[k], v)
        self.assertIn(tag.table_display, annotations)
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)  

    def test_5a_assoctable_define_defaults(self):
        tdef = ermrest_model.Table.define_association(
            [
                self.model.schemas["public"].tables["ERMrest_Client"],
                self.model.schemas["public"].tables["ERMrest_Group"],
            ],
        )
        self.assertEqual(tdef.get("table_name"), "ERMrest_Client_ERMrest_Group")
        self.assertEqual(tdef.get("comment"), None)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 2)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(
            { tuple(key["unique_columns"]) for key in kdefs },
            { ('RID',), ('ERMrest_Client', 'ERMrest_Group') },
        )
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(
            {
                (
                    fkdef["referenced_columns"][0]["schema_name"],
                    fkdef["referenced_columns"][0]["table_name"],
                    frozenset(zip(
                        tuple( c["column_name"] for c in fkdef["foreign_key_columns"] ),
                        tuple( c["column_name"] for c in fkdef["referenced_columns"] ),
                    ))
                )
                for fkdef in fkdefs
             },
            {
                ("public", "ERMrest_Client", frozenset([ ("ERMrest_Client", "ID",) ])),
                ("public", "ERMrest_Group", frozenset([ ("ERMrest_Group", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RCB", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RMB", "ID",) ])),
            }
        )
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_5b_assoctable_define_custom(self):
        tname = "client_group"
        tdef = ermrest_model.Table.define_association(
            [
                ('c_id', self.model.schemas["public"].tables["ERMrest_Client"]),
                ('g_id', self.model.schemas["public"].tables["ERMrest_Group"]),
            ],
            [],
            tname,
            self._test_comment,
            provide_system_fkeys=False,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        self.assertEqual(tdef.get("comment"), self._test_comment)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 2)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(
            { tuple(key["unique_columns"]) for key in kdefs },
            { ('RID',), ('c_id', 'g_id') },
        )
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(
            {
                (
                    fkdef["referenced_columns"][0]["schema_name"],
                    fkdef["referenced_columns"][0]["table_name"],
                    frozenset(zip(
                        tuple( c["column_name"] for c in fkdef["foreign_key_columns"] ),
                        tuple( c["column_name"] for c in fkdef["referenced_columns"] ),
                    ))
                )
                for fkdef in fkdefs
             },
            {
                ("public", "ERMrest_Client", frozenset([ ("c_id", "ID",) ])),
                ("public", "ERMrest_Group", frozenset([ ("g_id", "ID",) ])),
            }
        )
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_5c_assoctable_define_with_metadata(self):
        tname = "client_group"
        tdef = ermrest_model.Table.define_association(
            [
                ('c_id', self.model.schemas["public"].tables["ERMrest_Client"]),
                ('g_id', self.model.schemas["public"].tables["ERMrest_Group"]),
            ],
            [
                ermrest_model.Column.define("md1", ermrest_model.builtin_types.text),
            ],
            tname,
            self._test_comment,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        self.assertEqual(tdef.get("comment"), self._test_comment)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 2 + 1)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(
            { tuple(key["unique_columns"]) for key in kdefs },
            { ('RID',), ('c_id', 'g_id') },
        )
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(
            {
                (
                    fkdef["referenced_columns"][0]["schema_name"],
                    fkdef["referenced_columns"][0]["table_name"],
                    frozenset(zip(
                        tuple( c["column_name"] for c in fkdef["foreign_key_columns"] ),
                        tuple( c["column_name"] for c in fkdef["referenced_columns"] ),
                    ))
                )
                for fkdef in fkdefs
             },
            {
                ("public", "ERMrest_Client", frozenset([ ("c_id", "ID",) ])),
                ("public", "ERMrest_Group", frozenset([ ("g_id", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RCB", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RMB", "ID",) ])),
            }
        )
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_key_drop_cascading(self):
        self._create_schema_with_fkeys()
        schema = self.model.schemas['schema_with_fkeys']
        self.model.schemas['schema_with_fkeys'].tables['parent'].keys[(schema, 'parent_id_key')].drop(cascade=True)

    def test_key_reordered_drop_cascading(self):
        self._create_schema_with_fkeys()
        schema = self.model.schemas['schema_with_fkeys']
        self.model.schemas['schema_with_fkeys'].tables['parent'].keys[(schema, 'parent_compound_key')].drop(cascade=True)

    def test_key_column_drop_cascading(self):
        self._create_schema_with_fkeys()
        self.model.schemas['schema_with_fkeys'].tables['parent'].columns['id'].drop(cascade=True)

    def test_fkey_column_drop_cascading(self):
        self._create_schema_with_fkeys()
        self.model.schemas['schema_with_fkeys'].tables['child'].columns['parent_id_extra'].drop(cascade=True)

    def test_table_drop_cascading(self):
        self._create_schema_with_fkeys()
        self.model.schemas['schema_with_fkeys'].tables['parent'].drop(cascade=True)

    def test_schema_drop_cascading(self):
        self._create_schema_with_fkeys()
        self.model.schemas['schema_with_fkeys'].drop(cascade=True)

if __name__ == '__main__':
    unittest.main()
