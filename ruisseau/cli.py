import argparse
import sys
from typing import Optional, Sequence


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ruisseau", description="Minimal DAG orchestration tool"
    )

    parser.add_argument("-V", "--version", action="version", version="%(prog)s 0.1.0")

    subparsers = parser.add_subparsers()
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("path")
    validate_parser.add_argument("--quiet", action="store_true", default=False)
    validate_parser.add_argument(
        "--format", choices=["auto", "yaml", "py"], default="auto"
    )
    validate_parser.set_defaults(command="validate")

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "validate":
        return 0

    print("ruisseau validate <path> [--quiet] [--format {auto,yaml,py}]")
    return 0
