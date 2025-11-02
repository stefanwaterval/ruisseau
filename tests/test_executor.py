import pytest

from ruisseau.dag import DAG
from ruisseau.executor import LocalExecutor
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
    dag = DAG(name="some dag", tasks=[Task(id="A"), Task(id="B")])
    runnables = {"A": noop, "B": noop}
    executor = LocalExecutor()

    summary = executor.execute(dag, runnables)
    assert summary == {"A": "pass", "B": "pass", "overall": "pass"}


def test_runnable_raises_exception_stops_early():
    dag = DAG(
        name="some dag",
        tasks=[Task(id="A"), Task(id="B", deps=("A",)), Task(id="C", deps=("B",))],
    )
    runnables = {"A": noop, "B": fail, "C": noop}
    executor = LocalExecutor()

    summary = executor.execute(dag, runnables)

    # should stop at B
    assert summary["A"] == "pass"
    assert summary["B"] == "fail"
    assert summary["overall"] == "fail"
    assert "C" not in summary


def test_tuple_form_two_args():
    dag = DAG(name="some dag", tasks=[Task(id="A")])
    runnables = {"A": (add, (1, 2))}
    executor = LocalExecutor()

    summary = executor.execute(dag, runnables)
    assert summary == {"A": "pass", "overall": "pass"}


def test_tuple_form_args_and_kwargs():
    dag = DAG(name="some dag", tasks=[Task(id="A")])
    runnables = {"A": (combine, (1,), {"b": 2})}
    executor = LocalExecutor()

    summary = executor.execute(dag, runnables)
    assert summary == {"A": "pass", "overall": "pass"}


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
