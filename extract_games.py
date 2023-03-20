import logging
import os
from pathlib import Path

import pandas as pd
from pysolr import Solr

# ------------> LOGGING CONFIG
logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:%(message)s')


# ------------> FUNCTIONS
def get_game_details(file_name: str) -> tuple[str, str]:
    """
    Get the team name and the type of the game from the filename
    Args:
        file_name: ./{SCHEDULES_DIR}/{file}

    Returns:

    """
    logging.debug(f"get_game_details({file_name})")
    token = file_name.split('/')
    token = token[2].split('-')
    return f'{token[0]}-{token[1]}', token[2].replace('s', '').replace('e', '')


# Convert original timestamps to proper format.
def format_date(date: str) -> str:
    """
    Reformat the date and return a string representation of dates, expressed in Coordinated Universal Time (UTC).
    Args:
        date:
            Examples: 2021-04-29 13:26:32

    Returns:
        Examples: 1972-05-20T17:33:18Z
    """
    logging.debug(f"")
    date = date.split(date)
    return f"{date[0]}T{date[1]}Z"


def extract_games_from_file(file_name: str) -> pd.DataFrame:
    """

    Args:
        file_name: {SCHEDULES_DIR}/{file}
    Returns:
        A dataframe containing the game ids and query filter.
    """
    logging.debug(f"extract_games_from_file({file_name}")
    # Maps the read methods. If a new file format is needed, add it here.
    suffix_mapping = {
        ".csv": pd.read_csv,
        ".xlsx": pd.read_excel,
    }
    file_suffix = Path(file_name).suffix

    df = suffix_mapping[file_suffix](file_name, dtype=str)
    df["QueryTm"] = df.apply(lambda x: f"Timestamp:[{format_date(x['StartTm'])} TO {format_date(x['EndTm'])}]", axis=1)
    df.drop(columns=['StartTm', 'EndTm'], inplace=True)

    team_name, game_type = get_game_details(file_name)
    df["TeamName"] = team_name
    df["GameType"] = game_type

    print(df.loc[0]["TeamName"])

    return df


def query_db(client: Solr, timestamp: str) -> object:
    """
    Query the database.
    Args:
        timestamp: QueryTm from extract_games_from_file dataframe
        client (Solr): The Solr client.
    """
    logging.debug(f"query_db(client, {timestamp})")
    return client.search('*:*',
                         **{
                             'fq': timestamp,
                             'wt': 'csv',
                             'indent': True,
                             'rows': 100_000,
                         })


def process_game(client: Solr, row: pd.Series):
    """
    Args:
        client: Solr client
        row: Row from dataframe containing game id, query time, team name, and game type

    Send a query to Solr with the specified timestamp -> Transform result into Dataframe -> Save as .csv
    """
    logging.debug(f"process_game({row})")
    df = pd.DataFrame(query_db(client=client, timestamp=row[1]))
    df.to_csv(path_or_buf=f'./games/{row[2]}_{row[3]}_{row[0]}.csv', index=False)


def process_file(file: str):
    """
    Applies the process_game function to all games in a file.
    Args:
        file: ./{SCHEDULES_DIR}/{file}
    """
    logging.debug(f"process_file({file})")
    games_df = extract_games_from_file(file)

    # Instantiate Solr client. Timeout can be increased for large queries.
    solr = Solr(f"http://localhost:8987/solr/{games_df.loc[0]['TeamName']}", timeout=30)

    [process_game(solr, game) for game in games_df]


def main():
    SCHEDULES_DIR = "./schedules"
    list_of_schedule_files: list[str] = [f"{SCHEDULES_DIR}/{file}" for file in os.listdir(SCHEDULES_DIR)]
    [process_file(file) for file in list_of_schedule_files]


# ------------> MAIN
if __name__ == "__main__":
    main()
