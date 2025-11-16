import argparse
import sys
from importlib.metadata import PackageNotFoundError, version
from typing import Optional, Sequence

from ruisseau.executor import LocalExecutor

from .io.paths import (
    ensure_readable_file,
    load_dag_from_python,
    load_runnables_from_python,
    resolve_file_suffix,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ruisseau", description="Minimal DAG orchestration tool"
    )
    # version
    current_version = get_current_version()
    parser.add_argument(
        "-V", "--version", action="version", version=f"%(prog)s {current_version}"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # validate
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("path")
    validate_parser.add_argument("--quiet", action="store_true", default=False)
    validate_parser.add_argument(
        "--format", choices=["auto", "yaml", "yml", "py"], default="auto"
    )
    validate_parser.set_defaults(command="validate")

    # run
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("path")
    run_parser.add_argument("--quiet", action="store_true", default=False)
    run_parser.add_argument(
        "--format", choices=["auto", "yaml", "yml", "py"], default="auto"
    )
    run_parser.add_argument("--json", action="store_true", default=False)
    run_parser.set_defaults(command="run")

    return parser


def get_current_version() -> str:
    try:
        current_version = version("ruisseau")
    except PackageNotFoundError:
        return "0.0.0"

    return current_version


def validate_command(args: argparse.Namespace) -> int:
    try:
        path = ensure_readable_file(args.path)
        resolve_file_suffix(path, args.format)
        dag = load_dag_from_python(path)
        dag.validate()
    except (
        FileNotFoundError,
        IsADirectoryError,
        PermissionError,
        TypeError,
        ValueError,
        AttributeError,
    ) as e:
        print(f"Input error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Validation error: {e}", file=sys.stderr)
        return 1

    if not args.quiet:
        print(f"DAG loaded and validated: {args.path}")
    return 0


def run_command(args: argparse.Namespace) -> int:
    try:
        path = ensure_readable_file(args.path)
        resolve_file_suffix(path, args.format)
        dag = load_dag_from_python(path)
        dag.validate()
        runnables = load_runnables_from_python(path)
    except (
        FileNotFoundError,
        IsADirectoryError,
        PermissionError,
        TypeError,
        ValueError,
        AttributeError,
    ) as e:
        print(f"Input error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Validation error: {e}", file=sys.stderr)
        return 1

    if not args.quiet and not args.json:
        print(f"DAG loaded and validated: {args.path} (ready to run)")

    executor = LocalExecutor()
    try:
        dag_result = executor.execute(dag, runnables)
    except (TypeError, ValueError) as e:
        print(f"Input error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Run error: {e}", file=sys.stderr)
        return 1

    if not args.quiet and not args.json:
        print(
            f"DAG '{dag_result.name}' finished with status: {dag_result.overall} "
            f"({len(dag_result.results)} task(s) run)"
        )

    if args.json:
        print(dag_result.to_json())

    return 0 if dag_result.is_success() else 1


def main(argv: Optional[Sequence[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "validate":
        return validate_command(args)

    if args.command == "run":
        return run_command(args)

    return 0


if __name__ == "__main__":
    sys.exit(main())
