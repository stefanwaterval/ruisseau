from typing import Callable

from .dag import DAG


class LocalExecutor:
    def __init__(self) -> None:
        pass

    def execute(self, dag: DAG, runnables: dict) -> dict[str, str]:
        order = dag.topological_order()
        summary: dict[str, str] = {}

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
                summary[task_id] = "pass"
            except Exception:
                summary[task_id] = "fail"
                summary["overall"] = "fail"
                return summary

        summary["overall"] = "pass"
        return summary
