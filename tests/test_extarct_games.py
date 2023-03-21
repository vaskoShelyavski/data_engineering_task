import pytest

from extract_games import *


@pytest.fixture
def sample_game_file(tmp_path):
    file_path = tmp_path / "team-test-matches-export.csv"
    file_path.write_text("StartTm,EndTm\n2022-03-22 20:00:00,2022-03-22 21:00:00\n")
    pre_result = str(file_path).split("/")
    return f"./{pre_result[-2]}/{pre_result[-1]}"


def test_get_game_details(sample_game_file):
    team_name, game_type = get_game_details(sample_game_file)
    assert team_name == "team-test"
    assert game_type == "match"


def test_format_date():
    assert format_date("2022-03-22 20:00:00") == "2022-03-22T20:00:00Z"


def test_extract_games_from_file(sample_game_file):
    df = extract_games_from_file(sample_game_file)
    assert len(df) == 1
    assert df.loc[0]["QueryTm"] == "Timestamp:[2022-03-22T20:00:00Z TO 2022-03-22T21:00:00Z]"
    assert df.loc[0]["TeamName"] == "game-0"
    assert df.loc[0]["GameType"] == "1"
