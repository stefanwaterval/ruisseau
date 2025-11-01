import pytest

from ruisseau.cli import build_parser


def test_version(capsys):
    parser = build_parser()

    with pytest.raises(SystemExit) as exit_info:
        parser.parse_args(["-V"])
    assert exit_info.value.code == 0

    out, err = capsys.readouterr()
    assert "ruisseau 0.1.0" in out
    assert err == ""

    with pytest.raises(SystemExit) as exit_info:
        parser.parse_args(["--version"])
    assert exit_info.value.code == 0

    out, err = capsys.readouterr()
    assert "ruisseau 0.1.0" in out
    assert err == ""


def test_help(capsys):
    parser = build_parser()

    with pytest.raises(SystemExit) as exit_info:
        parser.parse_args(["-h"])
    assert exit_info.value.code == 0

    _, err = capsys.readouterr()
    assert err == ""

    with pytest.raises(SystemExit) as exit_info:
        parser.parse_args(["--help"])
    assert exit_info.value.code == 0

    _, err = capsys.readouterr()
    assert err == ""


def test_happy_path():
    parser = build_parser()
    args = parser.parse_args(["validate", "path/to/file"])

    assert args.command == "validate"
    assert args.path == "path/to/file"
    assert not args.quiet
    assert args.format == "auto"


def test_flag_permutations():
    parser = build_parser()
    args1 = parser.parse_args(["validate", "--quiet", "--format", "yaml", "file.yaml"])

    assert args1.command == "validate"
    assert args1.quiet
    assert args1.format == "yaml"
    assert args1.path == "file.yaml"

    args2 = parser.parse_args(["validate", "file.py", "--quiet", "--format", "py"])

    assert args2.command == "validate"
    assert args2.quiet
    assert args2.format == "py"
    assert args2.path == "file.py"

    args3 = parser.parse_args(["validate", "file.py", "--quiet"])

    assert args3.command == "validate"
    assert args3.quiet
    assert args3.format == "auto"
    assert args3.path == "file.py"


def test_missing_path():
    parser = build_parser()

    with pytest.raises(SystemExit) as exit_info:
        parser.parse_args(["validate"])
    assert exit_info.value.code == 2


def test_bad_format():
    parser = build_parser()

    with pytest.raises(SystemExit) as exit_info:
        parser.parse_args(["validate", "file", "--format", "nope"])
    assert exit_info.value.code == 2


def test_unknown_option():
    parser = build_parser()

    with pytest.raises(SystemExit) as exit_info:
        parser.parse_args(["validate", "file", "--nope"])
    assert exit_info.value.code == 2
