import importlib.util
import os
from pathlib import Path

from ruisseau.dag import DAG


def ensure_readable_file(path: str | Path) -> Path:
    if not isinstance(path, (str, Path)):
        raise TypeError(f"Invalid path: {path} should be a string or Path object.")

    if isinstance(path, str):
        path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Invalid path: {path} does not exist.")

    if path.is_dir():
        raise IsADirectoryError(f"Invalid path: {path} cannot be a directory.")

    if not os.access(path, os.R_OK):
        raise PermissionError(
            f"Invalid permissions: {path} is not readable. Adjust file permissions and try again."
        )

    return path


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
