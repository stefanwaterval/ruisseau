import pytest

from ruisseau.results import DAGResult, TaskResult


def test_dagresult_all_pass_overall_pass():
    tasks = {
        "A": TaskResult("A", "pass"),
        "B": TaskResult("B", "pass"),
    }
    dr = DAGResult("MyDAG", tasks)
    assert dr.name == "MyDAG"
    assert dr.tasks is tasks  # same reference kept
    assert dr.overall == "pass"


def test_dagresult_any_fail_overall_fail():
    tasks = {
        "A": TaskResult("A", "pass"),
        "B": TaskResult("B", "fail"),
        "C": TaskResult("C", "pass"),
    }
    dr = DAGResult("MyDAG", tasks)
    assert dr.overall == "fail"


def test_dagresult_validation_errors():
    # Empty name
    with pytest.raises(ValueError):
        DAGResult("", {"A": TaskResult("A", "pass")})
