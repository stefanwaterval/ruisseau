import argparse
import importlib.util
import sys
from pathlib import Path
from typing import Optional, Sequence

from .dag import DAG
from .io.paths import ensure_readable_file


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ruisseau", description="Minimal DAG orchestration tool"
    )

    parser.add_argument("-V", "--version", action="version", version="%(prog)s 0.1.0")

    subparsers = parser.add_subparsers(dest="command", required=True)
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("path")
    validate_parser.add_argument("--quiet", action="store_true", default=False)
    validate_parser.add_argument(
        "--format", choices=["auto", "yaml", "yml", "py"], default="auto"
    )
    validate_parser.set_defaults(command="validate")

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "validate":
        try:
            path = resolve_input_file(args.path)
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
            print(f"Input error\n{e}", file=sys.stderr)
            return 2
        except Exception as e:
            print(f"Validation failed\n{e}", file=sys.stderr)
            return 1

        if not args.quiet:
            print(f"DAG in {args.path} successfully loaded and validated!")
        return 0
    return 0


def resolve_input_file(path: str) -> Path:
    return ensure_readable_file(path)


def resolve_file_suffix(path: Path, format: str) -> None:
    if format == "py":
        if not path.suffix == ".py":
            raise ValueError(f"Format {format} requires .py file")
    if format in ["yaml", "yml"]:
        raise ValueError("YAML not yet supported")
    if format == "auto":
        if not path.suffix == ".py":
            raise ValueError("Only .py files accepted")


def load_dag_from_python(path: Path) -> DAG:
    module_name = path.stem

    if not path.suffix == ".py":
        raise ValueError(f"Invalid extension: {path} should be a python file.")

    spec = importlib.util.spec_from_file_location(name=module_name, location=path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module {module_name!r} from {path}")

    module = importlib.util.module_from_spec(spec=spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "dag"):
        raise AttributeError(f"{module_name!r} does not define a top-level 'dag'")

    dag = module.dag

    if not isinstance(dag, DAG):
        raise TypeError(f"{dag} is not of type DAG")

    return dag


if __name__ == "__main__":
    sys.exit(main())
