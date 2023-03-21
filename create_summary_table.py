import os
import pandas as pd


def main():
    DIR = './games'
    DESTINATION = './analysis/summary.csv'

    # Create the DF
    df_result = pd.DataFrame([], columns=['TeamID', 'GameType', 'GameID', 'NumPlayers', 'PlayersList'])

    for file in os.listdir(DIR):
        if file.endswith(".csv"):
            # Read the csv
            df_input = pd.read_csv(os.path.join(DIR, file), low_memory=False)

            # Get the TeamID, GameType, and GameID from the name of the file
            team_name, game_type, game_id = file.replace(".csv", "").split("_")

            # Get the number of players
            num_players = df_input['PlayerID'].nunique()

            # Get a sorted list of the unique PlayerIDs
            players_list = sorted(df_input['PlayerID'].unique())
            players_list_formatted = ', '.join(map(str, players_list))

            # Map the column names
            new_row = pd.Series(
                [team_name, game_type, game_id, num_players, players_list_formatted],
                index=df_result.columns
            )
            df_result.loc[len(df_result)] = new_row

    df_result.to_csv(path_or_buf=DESTINATION, index=False)


if __name__ == "__main__":
    main()
