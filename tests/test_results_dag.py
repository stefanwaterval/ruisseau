import pytest

from ruisseau.results import DAGResult, TaskResult


def test_dagresult_all_pass_overall_pass():
    results = [
        TaskResult("A", "pass"),
        TaskResult("B", "pass"),
    ]
    dr = DAGResult("MyDAG", results)
    assert dr.name == "MyDAG"
    assert dr.results is results  # same reference kept
    assert dr.overall == "pass"


def test_dagresult_any_fail_overall_fail():
    tasks = [
        TaskResult("A", "pass"),
        TaskResult("B", "fail"),
        TaskResult("C", "pass"),
    ]
    dr = DAGResult("MyDAG", tasks)
    assert dr.overall == "fail"


def test_dagresult_validation_errors():
    # Empty name
    with pytest.raises(ValueError):
        DAGResult("", [TaskResult("A", "pass")])
