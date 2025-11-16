from ruisseau.cli import main


def test_validate_success_returns_zero(capsys):
    rc = main(["validate", "examples/example_dag.py"])
    out, err = capsys.readouterr()

    assert rc == 0
    assert "DAG loaded and validated" in out
    assert err == ""


def test_validate_quiet_success_returns_zero(capsys):
    rc = main(["validate", "examples/example_dag.py", "--quiet"])
    out, err = capsys.readouterr()

    assert rc == 0
    assert out == ""
    assert err == ""


def test_validate_failure_returns_one(capsys):
    rc = main(["validate", "examples/example_dag_with_cycle.py"])
    out, err = capsys.readouterr()

    assert rc == 1
    assert out == ""
    assert "Cycle error" in err


def test_validate_quiet_failure_returns_one(capsys):
    rc = main(["validate", "examples/example_dag_with_cycle.py", "--quiet"])
    out, err = capsys.readouterr()

    assert rc == 1
    assert out == ""
    assert "Cycle error" in err


def test_validate_missing_file_exits_2(capsys):
    rc = main(["validate", "non_existent_file"])
    out, err = capsys.readouterr()

    assert rc == 2
    assert out == ""
    assert "Input error" in err


def test_validate_invalid_extension_exits_2(capsys):
    rc = main(["validate", "examples/dummy.txt"])
    out, err = capsys.readouterr()

    assert rc == 2
    assert out == ""
    assert "Input error" in err
