"""Tests for deriva-hatrac-cli session config handling (no live server).

The put sub-command's --no-stream-chunks option makes the CLI create its HatracStore with a session config that sets
"stream_chunked_uploads" to false. Other sub-commands, and put without the option, use the default session config.
HatracStore is patched so that no store (or session) is actually created.
"""

import unittest
from unittest.mock import patch

from deriva.core import DEFAULT_SESSION_CONFIG
from deriva.core import hatrac_cli
from deriva.core.hatrac_cli import DerivaHatracCLI


class TestHatracCliNoStreamChunks(unittest.TestCase):

    def _store_session_config(self, argv):
        cli = DerivaHatracCLI("desc", "epilog")
        args = cli.parser.parse_args(["--host", "example.org", "--token", "t"] + argv)
        with patch.object(hatrac_cli, "HatracStore") as store_cls:
            cli._post_parser_init(args)
        store_cls.assert_called_once()
        return store_cls.call_args.kwargs.get("session_config")

    def test_put_default_streams(self):
        self.assertIsNone(self._store_session_config(["put", "in.bin", "/hatrac/ns/obj"]))

    def test_put_no_stream_chunks(self):
        session_config = self._store_session_config(["put", "--no-stream-chunks", "in.bin", "/hatrac/ns/obj"])
        self.assertIs(session_config["stream_chunked_uploads"], False)
        # everything else is the default session config
        self.assertEqual({k: v for k, v in session_config.items() if k != "stream_chunked_uploads"},
                         {k: v for k, v in DEFAULT_SESSION_CONFIG.items() if k != "stream_chunked_uploads"})
        # the shared default is not mutated
        self.assertIs(DEFAULT_SESSION_CONFIG["stream_chunked_uploads"], True)

    def test_other_subcommands_unaffected(self):
        for argv in (["ls", "/hatrac/ns"], ["get", "/hatrac/ns/obj", "-"], ["del", "/hatrac/ns/obj"]):
            with self.subTest(argv=argv):
                self.assertIsNone(self._store_session_config(argv))

    def test_chunk_size_must_be_positive(self):
        cli = DerivaHatracCLI("desc", "epilog")
        args = cli.parser.parse_args(["--host", "example.org", "put", "--chunk-size", "1048576", "in.bin", "/x"])
        self.assertEqual(args.chunk_size, 1048576)
        for value in ("0", "-5", "abc"):
            with self.subTest(value=value):
                with self.assertRaises(SystemExit):
                    with patch("sys.stderr"):
                        cli.parser.parse_args(["--host", "example.org", "put", "--chunk-size", value, "in.bin", "/x"])

    def test_option_only_on_put(self):
        cli = DerivaHatracCLI("desc", "epilog")
        with self.assertRaises(SystemExit):
            with patch("sys.stderr"):
                cli.parser.parse_args(["--host", "example.org", "ls", "--no-stream-chunks", "/hatrac/ns"])


if __name__ == "__main__":
    unittest.main()
