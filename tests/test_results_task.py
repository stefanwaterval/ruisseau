from dataclasses import FrozenInstanceError

import pytest

from ruisseau.results import TaskResult


def test_taskresult_happy_path():
    t1 = TaskResult("A", "pass")
    t2 = TaskResult("B", "fail", "boom")

    assert t1.id == "A"
    assert t1.status == "pass"
    assert t1.message is None

    assert t2.id == "B"
    assert t2.status == "fail"
    assert t2.message == "boom"


def test_taskresult_is_immutable():
    t = TaskResult("A", "pass")
    with pytest.raises(FrozenInstanceError):
        t.status = "fail"


def test_taskresult_validation_errors():
    with pytest.raises(ValueError):
        TaskResult("", "pass")
    with pytest.raises(ValueError):
        TaskResult("A", "unknown")
