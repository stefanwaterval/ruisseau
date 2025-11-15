from ruisseau.cli import main


def test_run_success_returns_zero(capsys):
    rc = main(["run", "examples/example_dag.py"])
    out, err = capsys.readouterr()

    assert rc == 0
    assert "run command not implemented yet" in out
    assert err == ""


def test_run_quiet_success_returns_zero(capsys):
    rc = main(["run", "examples/example_dag.py", "--quiet"])
    out, err = capsys.readouterr()

    assert rc == 0
    assert out == ""
    assert err == ""
