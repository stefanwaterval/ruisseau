import pytest

from ruisseau.dag import DAG, CycleError, DuplicateTaskIDError, MissingDependencyError
from ruisseau.task import Task


def test_duplicate_ids_raise():
    task1 = Task(id="A")
    task2 = Task(id="A")

    with pytest.raises(
        DuplicateTaskIDError, match="Invalid tasks: duplicates are not allowed"
    ):
        DAG(name="duplicate_dag", tasks=[task1, task2])


def test_missing_dep_raises():
    task1 = Task(id="A")
    task2 = Task(id="B", deps=("C",))

    with pytest.raises(
        MissingDependencyError,
        match="Missing dependencies: Some tasks depend on tasks absent from this DAG",
    ):
        DAG(name="missing_dep_dag", tasks=[task1, task2])


def test_cycle_raises():
    task1 = Task(id="A", deps=("B",))
    task2 = Task(id="B", deps=("A",))
    dag = DAG(name="cycle_dag", tasks=[task1, task2])

    with pytest.raises(
        CycleError, match="Cycle error: The Kahn's algorithm detected a cycle."
    ):
        dag.topological_order()


def test_topo_simple_chain():
    task1 = Task(id="A")
    task2 = Task(id="B", deps=("A",))
    task3 = Task(id="C", deps=("B",))

    dag = DAG(name="simple_chain_dag", tasks=[task1, task2, task3])

    assert dag.topological_order() == ["A", "B", "C"]


def test_topo_diamond():
    task1 = Task(id="A")
    task2 = Task(id="B", deps=("A",))
    task3 = Task(id="C", deps=("A",))
    task4 = Task(id="D", deps=("B", "C"))

    dag = DAG(name="diamond_dag", tasks=[task1, task2, task3, task4])

    order = dag.topological_order()
    assert order == ["A", "B", "C", "D"] or order == ["A", "C", "B", "D"]
