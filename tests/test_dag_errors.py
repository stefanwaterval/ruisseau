import pytest

from ruisseau.dag import DAG, CycleError, DuplicateTaskIDError, MissingDependencyError
from ruisseau.task import Task


def test_duplicate_ids_raise():
    task1 = Task(id="A")
    task2 = Task(id="A")

    with pytest.raises(DuplicateTaskIDError):
        DAG(name="duplicate_dag", tasks=[task1, task2])


def test_missing_dep_raises():
    task1 = Task(id="A")
    task2 = Task(id="B", deps=("C",))

    with pytest.raises(MissingDependencyError):
        DAG(name="missing_dep_dag", tasks=[task1, task2])


def test_cycle_raises():
    task1 = Task(id="A", deps=("B",))
    task2 = Task(id="B", deps=("A",))
    dag = DAG(name="cycle_dag", tasks=[task1, task2])

    with pytest.raises(CycleError):
        dag.topological_order()
