from ruisseau.cli import main


def test_run_success_returns_zero(capsys):
    rc = main(["run", "examples/example_dag.py"])
    out, err = capsys.readouterr()

    assert rc == 0
    assert "finished with status: pass" in out
    assert "DAG 'example'" in out
    assert err == ""


def test_run_quiet_success_returns_zero(capsys):
    rc = main(["run", "examples/example_dag.py", "--quiet"])
    out, err = capsys.readouterr()

    assert rc == 0
    assert out == ""
    assert err == ""


def test_run_missing_runnables_returns_error(tmp_path, capsys):
    # Create a DAG file without runnables
    dfile = tmp_path / "bad_dag.py"
    dfile.write_text(
        "from ruisseau.dag import DAG\n"
        "from ruisseau.task import Task\n"
        "dag = DAG(name='bad', tasks=[Task(id='A')])\n"
    )

    rc = main(["run", str(dfile)])
    out, err = capsys.readouterr()

    # Input error because runnables are missing → exit 2
    assert rc == 2
    assert "Input error" in err
    assert "does not define a top-level 'runnables'" in err


def test_run_failure_returns_one(tmp_path, capsys):
    # DAG with failing step
    dfile = tmp_path / "failing_dag.py"
    dfile.write_text(
        "from ruisseau.dag import DAG\n"
        "from ruisseau.task import Task\n"
        "dag = DAG(name='failing', tasks=[Task(id='A')])\n"
        "def run_A():\n"
        "    raise RuntimeError('boom')\n"
        "runnables = {'A': run_A}\n"
    )

    rc = main(["run", str(dfile)])
    out, err = capsys.readouterr()

    assert rc == 1  # task failure → return 1
    assert "status: fail" in out
    assert "boom" not in out  # repr does not show message text
    assert err == ""
