"""Script for getting runs and such from the src API"""

from datetime import datetime
import pandas as pd
import requests

from speedrun_board import DATA_PATH, SRC_API_URL, PT_ID, SpeedrunBoard
from utils import query_api


"""Loading different datasets"""
def get_full_game(board_id, file_prefix=None, fetch_runs=True, save_path=None):
    """Download and enrich all categories, levels, variables, and optionally runs for a board.
    Save them in save_path. If board_prefix is provided, saved files will start with it. Otherwise,
    they will be prefixed by board_id."""

    print(f"Fetching data for {board_id}")
    game = query_api(f"{SRC_API_URL}/games/{board_id}")
    file_prefix = file_prefix or board_id

    # Extract links and download the categories, levels, variables, and runs
    game_links = {link['rel']: link['uri'] for link in game['links']}

    # TODO fill in vars on categories/levels/runs
    print("Fetching Variables...")
    variables = load_data(
        game_links['variables'],
        save_path=save_path / f"{file_prefix}_variables.parquet" if save_path else None
    ).set_index('id')

    print("Fetching Categories...")
    categories = load_data(
        game_links['categories'],
        save_path=save_path / f"{file_prefix}_categories.parquet" if save_path else None
    )

    print("Fetching Levels...")
    levels = load_data(
        game_links['levels'],
        save_path=save_path / f"{file_prefix}_levels.parquet" if save_path else None
    )

    runs = None
    if fetch_runs:
        # The runs str needs to have max pagination added to it
        print("Fetching Runs...")
        runs = load_data(
            game_links['runs'],
            api_args={'max': 200},
            save_path=save_path / f"{file_prefix}_runs.parquet" if save_path else None
        )

    return SpeedrunBoard(
        game=game,
        categories=categories,
        levels=levels,
        variables=variables,
        runs=runs)


def get_leaderboards():
    """Get the current leaderboards for Any%, True Ending, 100%, and 101%
    
    This method is currently unupdated for the new versions of the app"""
    categories = ["Any", "True_Ending", "100", "101",]

    current_date = datetime.utcnow().strftime("%Y-%m-%d")

    for category in categories:
        print(f"Fetching leaderboard for category {category}")
        lb_response = requests.get(f"{SRC_API_URL}/leaderboards/{PT_ID}/category/{category}")

        run_list = lb_response.json()['data']['runs']
        run_list_flattened = []
        for run in run_list:
            run_flat = {}
            run_flat['place'] = run['place']
            for k, v in run['run'].items():
                run_flat[k] = v
            run_list_flattened.append(run_flat)

        leaderboard_df = run_list_flattened

        leaderboard_df.to_parquet(path=DATA_PATH / f"PT_leaderboard_{category}_{current_date}.parquet")


"""Data Loading Functions, separate from enrichment"""

def load_data(
        api_endpoint,
        api_args=None,
        save_path=None):
    """Fetch data with the SRC API, convert it to a DataFrame, and optionally save it."""
    
    api_return = query_api(api_endpoint, api_args)

    data_df = pd.DataFrame(api_return)

    # Cache the results on local disk
    if save_path:
        data_df.to_parquet(path=save_path)

    return data_df
