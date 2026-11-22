"""Unit tests for tools/greet.py."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import unittest

from tools.greet import greet

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "tools" / "greet.py"


class TestGreet(unittest.TestCase):
    def test_default_greeting(self) -> None:
        self.assertEqual(greet(), "Hello, World!")

    def test_custom_name_greeting(self) -> None:
        self.assertEqual(greet("Alice"), "Hello, Alice!")

    def test_cli_default(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH)],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "Hello, World!\n")
        self.assertEqual(result.stderr, "")

    def test_cli_custom_name(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--name", "Alice"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "Hello, Alice!\n")
        self.assertEqual(result.stderr, "")

    def test_cli_unknown_flag_exits_nonzero(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--unknown"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unrecognized arguments", result.stderr)


if __name__ == "__main__":
    unittest.main()
