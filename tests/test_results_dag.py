import json

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


def test_dagresult_to_dict():
    tasks = [
        TaskResult("A", "pass"),
        TaskResult("B", "fail", "boom"),
    ]
    dr = DAGResult("MyDAG", tasks)
    dr_dict = dr.to_dict()
    assert dr_dict == {
        "name": dr.name,
        "overall": dr.overall,
        "results": [
            {"id": "A", "status": "pass", "message": None},
            {"id": "B", "status": "fail", "message": "boom"},
        ],
    }


def test_dagresult_to_json():
    tasks = [
        TaskResult("A", "pass"),
        TaskResult("B", "fail", "boom"),
    ]
    dr = DAGResult("MyDAG", tasks)
    assert json.loads(dr.to_json()) == dr.to_dict()


def test_dagresult_is_success():
    tasks_pass = [
        TaskResult("A", "pass"),
        TaskResult("B", "pass"),
    ]
    dr_pass = DAGResult("MyDAG", tasks_pass)
    assert dr_pass.is_success()

    tasks_fail = [
        TaskResult("A", "pass"),
        TaskResult("B", "fail", "boom"),
    ]
    dr_fail = DAGResult("MyDAG", tasks_fail)
    assert not dr_fail.is_success()
