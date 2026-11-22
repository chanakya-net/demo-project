"""Minimal greeting CLI utility."""

from __future__ import annotations

import argparse
from typing import Sequence


def greet(name: str = "World") -> str:
    """Return a greeting message for the given name."""
    return f"Hello, {name}!"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the greeting utility."""
    parser = argparse.ArgumentParser(description="Print a greeting message.")
    parser.add_argument(
        "--name",
        default="World",
        help="Name to greet (default: World).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    print(greet(args.name))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
