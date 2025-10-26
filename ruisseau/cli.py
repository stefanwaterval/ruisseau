import argparse
import sys
from typing import Optional, Sequence


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ruisseau", description="Minimal DAG orchestration tool"
    )
    subparsers = parser.add_subparsers()
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("path")
    validate_parser.set_defaults(command="validate")

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = build_parser()
    parser.parse_args(argv)

    return 0
