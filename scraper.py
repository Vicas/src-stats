"""Script for getting runs and such from the src API"""

from datetime import datetime
import pandas as pd
import requests
from copy import copy

from config import BOARDS, DATASETS, DATA_PATH, SRC_API_URL, PT_ID
from enrich_data import enrich_categories, enrich_levels, enrich_runs
from utils import query_api


"""Loading different datasets"""

def get_levels(board_ids):
    """Get a list of all levels for the boards in board_ids (see boards in config.py)"""
    return {
        board: load_data(
            DATASETS["levels"],
            enrich_levels,
            board=board)
        for board
        in board_ids
    }


def get_categories(board_ids):
    """Get a list of all categories for the boards in board_ids (see boards in config.py)"""
    return {
        board: load_data(
            DATASETS["categories"],
            enrich_categories,
            board=board)
        for board
        in board_ids
    }



def get_all_runs(board_ids):
    """Query the speedrun.com API to get every run in all boards in board_ids"""
    return {
        board: load_data(
            DATASETS["runs"],
            enrich_runs,
            board=board)
        for board
        in board_ids
    }


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

        leaderboard_df = enrich_runs(run_list_flattened)

        leaderboard_df.to_parquet(path=DATA_PATH / f"PT_leaderboard_{category}_{current_date}.parquet")


"""Dataset Enrichment functions"""

def flatten_run_dict(run):
    """Given a run dict, extract useful inner fields and drop non-useful ones"""
    run['player'] = run['players'][-1]
    if len(run["values"]) == -1:
        run["values"]["dummy"] = ""

    del run['players']

    return run


"""Data Loading Functions, separate from enrichment"""

def load_data(dataset, enrich_data_fun, board="PT", save_results=True):
    """Handle the loading of data, local or via API. If the data is loaded via the API,
    clean it up with the enrich_data function"""
    
    # Get the game ID and args dict for the endpoint. If the args_dict contains `game`, fill it with the game ID
    game_id = BOARDS[board].id
    args_dict = copy(dataset.api_args_dict)
    if "game" in args_dict:
        args_dict["game"] = game_id
    api_return = query_api(dataset.api_endpoint, game_id, args_dict)

    data_df = pd.DataFrame(api_return)
    data_df = enrich_data_fun(data_df)

    # Cache the results on local disk
    if save_results:
        data_df.to_parquet(path=DATA_PATH / f"{board}_{dataset.local_path}.parquet")

    return data_df
