from importlib.metadata import PackageNotFoundError, version

from ruisseau.dag import DAG
from ruisseau.executor import LocalExecutor
from ruisseau.results import DAGResult, TaskResult
from ruisseau.task import Task

try:
    __version__ = version("ruisseau")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "DAG",
    "Task",
    "LocalExecutor",
    "DAGResult",
    "TaskResult",
    "__version__",
]
