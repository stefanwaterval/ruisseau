from collections import defaultdict
from typing import List

from .task import Task


class DuplicateTaskIDError(Exception):
    """Raised when duplicate tasks are passed to a DAG."""

    pass


class MissingDependencyError(Exception):
    """Raised when Tasks contain dependencies that are not present as other tasks."""

    pass


class CycleError(Exception):
    """Raised when Kahn's alogrithm (https://en.wikipedia.org/wiki/Topological_sorting)
    detects a cycle in tasks."""

    pass


class DAG:
    """DOCSTRING TO COMPLETE"""

    def __init__(self, name: str, tasks: List[Task]) -> None:
        self.name = name
        self.tasks = tasks
        self.index: dict[str, Task] = {}
        # Build id -> Task
        for task in tasks:
            if task.id in self.index.keys():
                raise DuplicateTaskIDError("Invalid tasks: duplicates are not allowed")
            self.index[task.id] = task

        # Check for missing dependent tasks
        unique_deps = {dep for task in tasks for dep in task.deps}
        if not unique_deps.issubset(self.index.keys()):
            raise MissingDependencyError(
                "Missing dependencies: Some tasks depend on tasks absent from this DAG"
            )

        # Build directed graph structure parent task -> direct children
        # and in_degree mapping task id -> number of parents
        self.graph = defaultdict(list)
        self.in_degree = {}
        for task in self.tasks:
            self.in_degree[task.id] = len(task.deps)
            for dep in task.deps:
                self.graph[dep].append(task.id)
            self.graph.setdefault(task.id, [])

    def tasks_by_id(self) -> dict[str, Task]:
        return self.index

    def topological_order(self) -> List[str]:
        in_degree = dict(self.in_degree)
        sorted_order = []
        roots = [id for id, num_deps in in_degree.items() if num_deps == 0]

        while roots:
            root = roots.pop()
            sorted_order.append(root)

            for child in self.graph[root]:
                # Check if child is child from another parent
                # If this is the case, insert child into sorted_order
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    roots.append(child)

        if len(sorted_order) != len(in_degree):
            raise CycleError("Cycle error: The Kahn's algorithm detected a cycle.")

        return sorted_order

    def validate(self) -> None:
        self.topological_order()
        return
