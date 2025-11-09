import pytest

from ruisseau.dag import DAG
from ruisseau.executor import LocalExecutor
from ruisseau.results import DAGResult
from ruisseau.task import Task


def noop():
    """Function that always succeeds."""
    return None


def fail():
    """Function that always raises."""
    raise RuntimeError("boom")


def add(a, b):
    return a + b


def combine(a, b=0):
    return a + b


def test_all_success_callable():
    dag = DAG(name="some dag", tasks=[Task(id="A"), Task(id="B", deps=("A",))])
    runnables = {"A": noop, "B": noop}
    executor = LocalExecutor()

    dag_result = executor.execute(dag, runnables)
    assert isinstance(dag_result, DAGResult)
    assert dag_result.name == dag.name
    assert isinstance(dag_result.results, list)
    assert len(dag_result.results) == 2
    assert dag_result.overall == "pass"

    result_a, result_b = dag_result.results
    assert result_a.id == "A"
    assert result_a.status == "pass"
    assert result_a.message is None
    assert result_b.id == "B"
    assert result_b.status == "pass"
    assert result_b.message is None


def test_runnable_raises_exception_stops_early():
    dag = DAG(
        name="some dag",
        tasks=[Task(id="A"), Task(id="B", deps=("A",)), Task(id="C", deps=("B",))],
    )
    runnables = {"A": noop, "B": fail, "C": noop}
    executor = LocalExecutor()

    dag_result = executor.execute(dag, runnables)
    task_results = dag_result.results
    assert len(task_results) == 2
    assert (task_results[0].id, task_results[0].status) == ("A", "pass")
    assert (task_results[1].id, task_results[1].status) == ("B", "fail")
    assert dag_result.overall == "fail"


def test_tuple_form_two_args():
    dag = DAG(name="some dag", tasks=[Task(id="A")])
    runnables = {"A": (add, (1, 2))}
    executor = LocalExecutor()

    dag_result = executor.execute(dag, runnables)
    task_result = dag_result.results[0]
    assert (task_result.id, task_result.status) == ("A", "pass")
    assert dag_result.overall == "pass"


def test_tuple_form_args_and_kwargs():
    dag = DAG(name="some dag", tasks=[Task(id="A")])
    runnables = {"A": (combine, (1,), {"b": 2})}
    executor = LocalExecutor()

    dag_result = executor.execute(dag, runnables)
    task_result = dag_result.results[0]
    assert (task_result.id, task_result.status) == ("A", "pass")
    assert dag_result.overall == "pass"


def test_invalid_tuple_length_raises_valueerror():
    dag = DAG(name="some dag", tasks=[Task(id="A")])
    runnables = {"A": (noop, (), {}, {})}  # invalid length 4
    executor = LocalExecutor()

    with pytest.raises(ValueError):
        executor.execute(dag, runnables)


def test_invalid_type_raises_typeerror():
    dag = DAG(name="some dag", tasks=[Task(id="A")])
    runnables = {"A": 123}  # not callable or recognized tuple
    executor = LocalExecutor()

    with pytest.raises(TypeError):
        executor.execute(dag, runnables)


def test_missing_runnable_for_task_current_behavior():
    dag = DAG(name="some dag", tasks=[Task(id="A")])
    runnables = {}  # no entry for A -> spec=None -> TypeError in current code
    executor = LocalExecutor()

    with pytest.raises(TypeError):
        executor.execute(dag, runnables)
