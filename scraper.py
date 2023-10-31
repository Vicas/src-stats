"""Script for getting runs and such from the src API"""

from datetime import datetime
import pandas as pd
import requests
import sys
import time

from config import datasets, DATA_PATH, SRC_API_URL, PT_ID
from utils import map_short_name



# My src account ID
VICAS_ID = "v81v7558"

# Names to remove from the runs dataset, mostly to get rid of Stupid Rat
REMOVE_NAMES = ['Stupid Rat']


"""Loading different datasets"""

def get_levels(local_load=True):
    """Get a list of all levels for pizza tower"""
    return load_data(datasets["levels"], enrich_levels, local_load)


def get_categories(local_load=True):
    """Get a list of all categories for pizza tower"""
    return load_data(datasets["categories"], enrich_categories, local_load)


def get_all_runs(local_load=True):
    """Query the speedrun.com API to get every single pizza tower run."""
    return load_data(datasets["runs"], enrich_runs, local_load)


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

def enrich_levels(api_return):
    """Convert the API results to a dataframe and add short_names for display purposes"""
    levels = format_results(api_return)
    lev_df = pd.DataFrame(levels)

    # Add short names for graph display purposes
    lev_df["short_name"] = lev_df['name'].apply(lambda x: map_short_name(x))

    return lev_df


def enrich_categories(api_return):
    """Convert the API results to a dataframe, and no other enrichments for now"""
    categories = format_results(api_return)
    cat_df = pd.DataFrame(categories)

    return cat_df


def flatten_run_dict(run):
    """Given a run dict, extract useful inner fields and drop non-useful ones"""
    run['player'] = run['players'][0]

    del run['players']
    del run['values']

    return run


def enrich_runs(api_return):
    """Perform standard flattening/cleaning to runs so we can more easily use them in dataframes"""
    run_list = format_results(api_return)
    run_list = [flatten_run_dict(run) for run in run_list]

    run_df = pd.DataFrame(run_list)

    # Make dates into, well, dates
    run_df['date'] = pd.to_datetime(run_df['date'])

    # Tag Stupid Rat runs
    run_df['is_rat'] = run_df.apply(lambda x: x['player']['rel'] == 'guest' and x['player']['name'] == 'Stupid Rat', axis=1)

    # Make primary time a first-level column
    run_df['primary_t'] = run_df.apply(lambda x: x['times']['primary_t'], axis=1)

    # Tag ILs
    run_df['is_il'] = run_df['level'].apply(lambda x: x is not None)

    # Extract playerids, if you want 'em
    run_df['pid'] = run_df['player'].apply(lambda x: x['id'] if 'id' in x else None)

    # Extract run status
    run_df['status_judgment'] = run_df['status'].apply(lambda x: x['status'])

    return run_df


"""Data Loading Functions, separate from enrichment"""

def load_data(dataset, enrich_data_fun, local=True, save_results=True):
    """Handle the loading of data, local or via API. If the data is loaded via the API,
    clean it up with the enrich_data function"""
    if local:
        return load_local_data(dataset)
    
    arg_str = build_arg_str(dataset.api_args_dict) if dataset.api_args_dict else ""
    api_return = query_api(dataset.api_endpoint, arg_str)
    data_df = enrich_data_fun(api_return)

    # Cache the results on local disk
    if save_results:
        data_df.to_parquet(path=DATA_PATH / f"{dataset.local_path}.parquet")

    return data_df


def load_local_data(dataset):
    """Read data locally instead of pulling it down"""
    return pd.read_parquet(DATA_PATH / f"{dataset.local_path}.parquet")


# Pagination gives you ~20 results, so you gotta call the API again using
# the return value's pagination.links.uri for rel = 'next'
# It ends when the pagination list doesn't have a 'next' value anymore
def query_api(endpoint, arg_str=""):
    """Query the SRC API and return all unpaginated results for endpoint with args"""

    # print a newline so we can get a line to overwrite statuses on
    print("")
    results = requests.get(f"{SRC_API_URL}/{endpoint}?{arg_str}")
    results_list = []
    call_count = 1
    result_count = 0
    while results is not None:
        if results.status_code != 200:
            print(results.text)
            time.sleep(25)
            results = requests.get(next_url) if next_url else None
            continue

        results_list.append(results)

        # Update result counts without printing a billion lines, lol
        js = results.json()
        result_count += len(js['data'])
        sys.stdout.write('\r')
        sys.stdout.write(f"Got {result_count} results")
        sys.stdout.flush()

        next_url = get_next_uri(results)
        results = requests.get(next_url) if next_url else None

        # Avoid the API call limit in the hackiest way possible, lol
        if call_count % 25 == 0:
            time.sleep(5)

        call_count += 1

    return results_list


def format_results(results):
    """Given a list of responses, extract data"""
    data_list = []
    for response in results:
        for data in response.json()["data"]:
            data_list.append(data)

    return data_list

def get_next_uri(result):
    """Parse out the uri of the next paginated response, if it exists"""
    js = result.json()
    if 'pagination' in js and 'links' in js['pagination']:
        for l in js['pagination']['links']:
            if l['rel'] == 'next':
                return l['uri']

    return None

def build_arg_str(args):
    """Convert an arg dict into an arg str for the SRC url"""
    return "&".join([f"{k}={v}" for k,v in args.items()])