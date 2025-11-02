import pytest

from ruisseau.cli import main


def test_no_subcommand_shows_help_and_exits_2(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main([])

    out, err = capsys.readouterr()

    assert exit_info.value.code == 2
    assert out == ""
    assert "usage:" in err
