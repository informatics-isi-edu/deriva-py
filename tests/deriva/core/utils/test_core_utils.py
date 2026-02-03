
import datetime
import unittest

from deriva.core import \
    crockford_b32encode, crockford_b32decode, \
    int_to_uintX, uintX_to_int, \
    datetime_to_epoch_microseconds, epoch_microseconds_to_datetime

class CrockfordBase32Tests(unittest.TestCase):

    _basic_equivalents = [
        (0,       '0'),
        (2**0,    '1'),
        (2**5-1,  'Z'),
        (2**5,    '10'),
        (2**10,   '100'),
        (2**15,   '1000'),
        (2**20,   '1-0000'),
        (2**25,   '10-0000'),
        (2**40,   '1-0000-0000'),
        (2**60,   '1-0000-0000-0000'),
        (2**64,   'G-0000-0000-0000'),
        (2**64-2, 'F-ZZZZ-ZZZZ-ZZZY'),
        (2**64,   'G-0000-0000-0000'),
        (2**65-1, 'Z-ZZZZ-ZZZZ-ZZZZ'),
        (2**65,   '10-0000-0000-0000'),
    ]
    
    def test_b32decode_basic(self):
        for i, s in self._basic_equivalents:
            self.assertEqual(i, crockford_b32decode(s), f"{i=} {s=}")

    def test_b32decode_alts(self):
        self.assertEqual(0, crockford_b32decode('O'))
        self.assertEqual(0, crockford_b32decode('o'))
        self.assertEqual(1, crockford_b32decode('I'))
        self.assertEqual(1, crockford_b32decode('i'))
        self.assertEqual(1, crockford_b32decode('L'))
        self.assertEqual(1, crockford_b32decode('l'))

    def test_b32decode_sep(self):
        self.assertEqual(0, crockford_b32decode('00000'))
        self.assertEqual(0, crockford_b32decode('0-0000'))
        self.assertEqual(0, crockford_b32decode('0000-0'))

    def test_b32encode_basic(self):
        for i, s in self._basic_equivalents:
            self.assertEqual(s, crockford_b32encode(i), f"{s=} {i=}")

    def test_b32decode_type_errors(self):
        for func in [
            lambda : crockford_b32decode(0),
            lambda : crockford_b32decode([]),
        ]:
            with self.assertRaises(TypeError):
                func()

    def test_b32decode_value_errors(self):
        for func in [
            lambda : crockford_b32decode('['),
            lambda : crockford_b32decode('U'),
        ]:
            with self.assertRaises(ValueError):
                func()

    def test_b32encode_type_errors(self):
        for func in [
            lambda : crockford_b32encode('foo'),
            lambda : crockford_b32encode(b'00'),
            lambda : crockford_b32encode(0, 'a'),
            lambda : crockford_b32encode(0, []),
        ]:
            with self.assertRaises(TypeError):
                func()

    def test_b32encode_value_errors(self):
        for func in [
            lambda : crockford_b32encode(-1),
            lambda : crockford_b32encode(0, -1),
        ]:
            with self.assertRaises(ValueError):
                func()

    _signed_equivalents = [
        # nbits, int, s
        (64, -2**63, '8-0000-0000-0000'),
        (64, -1,     'F-ZZZZ-ZZZZ-ZZZZ'),
        (65, 2**63,  '8-0000-0000-0000'),
        (65, -2**64, 'G-0000-0000-0000'),
        (65, -1,     'Z-ZZZZ-ZZZZ-ZZZZ'),
        (65, -2,     'Z-ZZZZ-ZZZZ-ZZZY'),
    ]
                
    def test_b32encode_signed(self):
        for nbits, i, s in self._signed_equivalents:
            self.assertEqual(s, crockford_b32encode(int_to_uintX(i, nbits)), f"{nbits=} {i=} {s=}")

    def test_b32decode_signed(self):
        for nbits, i, s in self._signed_equivalents:
            self.assertEqual(i, uintX_to_int(crockford_b32decode(s), nbits), f"{i=} {s=} {nbits=}")


class SignedIntegerCodingTests (unittest.TestCase):

    _basic_equivalents = [
        # nbits, uint, int
        (1, 0, 0),
        (2, 0, 0),
        (2, 1, 1),
        (2, 2, -2),
        (2, 3, -1),
        (8, 0, 0),
        (8, 2**7-1, 2**7-1),
        (8, 2**7, -2**7),
        (8, 2**8-1, -1),
        (64, 2**63-1, 2**63-1),
        (64, 2**63, -2**63),
        (64, 2**64-1, -1),
    ]
    
    def test_int_to_uintX_basic(self):
        for nbits, ui, i in self._basic_equivalents:
            self.assertEqual(ui, int_to_uintX(i, nbits), f"{ui=} {i=} {nbits=}")

    def test_uintX_to_int_basic(self):
        for nbits, ui, i in self._basic_equivalents:
            self.assertEqual(i, uintX_to_int(ui, nbits), f"{i=} {ui=} {nbits=}")


class TimestampCodingTests (unittest.TestCase):
    _basic_equivalents = [
        # datetime, usecs
        (
            datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc),
            0,
        ),
        (
            datetime.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc),
            -1,
        ),
        (
            datetime.datetime(1900, 1, 1, tzinfo=datetime.timezone.utc),
            -2208988800000000,
        ),
        (
            datetime.datetime(2030, 12, 31, 12, 34, 56, 789012, tzinfo=datetime.timezone.utc),
            1924950896789012,
        ),
    ]

    def test_datetime_to_usecs(self):
        for dt, usecs in self._basic_equivalents:
            self.assertEqual(usecs, datetime_to_epoch_microseconds(dt), f"{usecs=} {dt=}")

    def test_usecs_to_datetime(self):
        for dt, usecs in self._basic_equivalents:
            self.assertEqual(dt, epoch_microseconds_to_datetime(usecs), f"{dt=} {usecs=}")


if __name__ == '__main__':
    unittest.main()
