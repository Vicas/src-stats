"""Script for getting runs and such from the src API"""

from datetime import datetime
import pandas as pd
import requests
import sys
import time
from copy import copy

from config import BOARDS, DATASETS, DATA_PATH, SRC_API_URL, PT_ID
from enrich_data import enrich_categories, enrich_levels, enrich_runs

# Length of time in seconds to sleep after each API call
SLEEP_INTERVAL = 0.4

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
    """Get the current leaderboards for Any%, True Ending, 100%, and 101%"""
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


# Pagination gives you ~20 results, so you gotta call the API again using
# the return value's pagination.links.uri for rel = 'next'
# It ends when the pagination list doesn't have a 'next' value anymore
def query_api(endpoint, game_id="", arg_dict=None):
    """Query the SRC API and return all unpaginated results for endpoint with args.
    
    game_id is parameterized so that different games can be pulled with the same config"""

    print("")
    formatted_endpoint = endpoint.format(game_id=game_id)

    # SRC results can either be paginated or unpaginated. Handle the first call, and then if
    # it contains a `pagination` key, go into the paginator workflow
    response = requests.get(f"{SRC_API_URL}/{formatted_endpoint}", params=arg_dict)
    time.sleep(SLEEP_INTERVAL)
    if response.status_code != 200:
        print(response.text)
        response.raise_for_status()

    if "pagination" not in response.json():
        return results['data']

    # Unroll pagination to return a single list of all results
    results_list = []
    call_count = 1
    while response is not None:
        if response.status_code != 200:
            # So far the only error I've seen for valid requests is rate limits, so
            # sleep and retry when it happens
            print(response.text)
            time.sleep(25)
            results = requests.get(next_url) if next_url else None
            continue

        results = response.json()
        results_list += results["data"]

        # Update result counts without printing a billion lines, lol
        sys.stdout.write('\r')
        sys.stdout.write(f"Got {len(results_list)} results")
        sys.stdout.flush()

        next_url = get_next_uri(results["pagination"])
        response = requests.get(next_url) if next_url else None

        time.sleep(SLEEP_INTERVAL)
        call_count += 1

    print("")

    return results_list


def get_next_uri(pagination_dict):
    """Parse out the uri of the next paginated response"""
    for link in pagination_dict.get('links', []):
        if link['rel'] == 'next':
            return link['uri']

    return None
