import argparse
import importlib.util
import sys
from pathlib import Path
from typing import Optional, Sequence

from .dag import DAG


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


def load_dag_from_python(path: str | Path) -> DAG:
    if isinstance(path, str):
        path = Path(path)

    module_name = path.stem

    if not path.exists():
        raise FileNotFoundError(f"Invalid path: cannot find {path}.")

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
