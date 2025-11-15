from typing import Callable, Mapping, TypeAlias

from .dag import DAG
from .results import DAGResult, TaskResult

RunnableSpec: TypeAlias = (
    Callable | tuple[Callable, tuple] | tuple[Callable, tuple, dict]
)


class LocalExecutor:
    def __init__(self) -> None:
        pass

    def execute(self, dag: DAG, runnables: Mapping[str, RunnableSpec]) -> DAGResult:
        dag_name: str = dag.name
        order: list[str] = dag.topological_order()

        results: list[TaskResult] = []
        for task_id in order:
            spec = runnables.get(task_id)

            func: Callable
            args: tuple
            kwargs: dict

            # Normalize into (func, args, kwargs)
            if callable(spec):
                func, args, kwargs = spec, (), {}
            elif isinstance(spec, tuple):
                if len(spec) == 2:
                    func, args = spec
                    kwargs = {}
                elif len(spec) == 3:
                    func, args, kwargs = spec
                else:
                    raise ValueError(
                        f"Invalid runnable spec for task '{task_id}': {spec}"
                    )
            else:
                raise TypeError(
                    f"Runnable for task '{task_id}' must be a function or tuple"
                )

            try:
                func(*args, **kwargs)
                results.append(TaskResult(task_id, "pass"))
            except Exception as e:
                results.append(TaskResult(task_id, "fail", f"{e}"))
                return DAGResult(dag_name, results)

        return DAGResult(dag_name, results)
