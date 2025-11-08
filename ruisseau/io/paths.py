import os
from pathlib import Path


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
            f"Invalide mode: {path} cannot be read, please insure reading mode."
        )

    return path
