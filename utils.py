"""Random useful util functions"""

import pickle
import time
import sys

import requests
from requests.adapters import HTTPAdapter, Retry

from config import DATA_PATH, CHART_PATH, SRC_API_URL

# Store user id-to-name mappings in dict, but only load it in when get_user_name is called
USER_PICKLE_PATH = DATA_PATH / "SRC_users.pkl"
USER_PICKLE = None

# Length of time in seconds to sleep after each API call (currently unused but keeping it here for now)
SLEEP_INTERVAL = 0.0

SHORT_NAME_MAP = {
    "Tutorial": "Tutorial",
    "F1 - John Gutter": "John Gutter",
    "F1 - Pizzascape": "Pizzascape",
    "F1 - Ancient Cheese": "Ancient Cheese",
    "F1 - Bloodsauce Dungeon": "Bloodsauce Dungeon",
    "F2 - Oregano Desert": "Oregano Desert",
    "F2 - Wasteyard": "Wasteyard",
    "F2 - Fun Farm": "Fun Farm",
    "F2 - Fastfood Saloon": "Fastfood Saloon",
    "F3 - Crust Cove": "Crust Cove",
    "F3 - Gnome Forest": "Gnome Forest",
    "F3 - GOLF": "GOLF",
    "F3 - Deep-Dish 9": "Deep-Dish 9",
    "F4 - The Pig City": "The Pig City",
    "F4 - Oh Shit!": "Oh Shit!",
    "F4 - Peppibot Factory": "Peppibot Factory",
    "F4 - Refrigerator-Refrigerador-Freezerator": "Freezerator",
    "F5 - Pizzascare": "Pizzascare",
    "F5 - Don't Make a Sound": "DMaS",
    "F5 - WAR": "WAR",
    "F5 - The Crumbling Tower of Pizza": "Crumbling Tower",
    "Pepperman": "Pepperman",
    "The Vigilante": "The Vigilante",
    "The Noise": "The Noise",
    "The Noise/The Doise": "Noise/Doise",
    "Fake Peppino": "Fake Peppino",
    "Pizzaface": "Pizzaface",
    "Secrets of the World": "Secrets of the World",
    "Tricky Treat": "Tricky Treat",
    "Pizzascape": "Pizzascape (SAGE)",
    "The Ancient Cheese": "The Ancient Cheese (SAGE)",
    "Bloodsauce Dungeon": "Bloodsauce Dungeon (SAGE)",
    "Pizzascare": "Pizzascare (SAGE)",
    "Strongcold": "Strongcold (SAGE)",
}


def init_folders():
    """Initialize folders for data/pngs in the project"""
    DATA_PATH.mkdir(parents=True, exist_ok=True)
    CHART_PATH.mkdir(parents=True, exist_ok=True)


def get_user_name(pid):
    """Perform a lookup on a src player ID to get their username (english)"""
    global USER_PICKLE
    if USER_PICKLE is None:
        if USER_PICKLE_PATH.exists():
            with open(USER_PICKLE_PATH, "rb") as pickle_file:
                USER_PICKLE = pickle.load(pickle_file)
        else:
            USER_PICKLE = {}

    if pid in USER_PICKLE:
        return USER_PICKLE[pid]['international']

    # Re-save the pickle file every time we grab a new name, this should be pretty expensive at
    # first but fall off very quickly as we get all the regulars
    api_return = query_api(f"users/{pid}")

    api_return = api_return["names"]
    print(f"Fetched new user {api_return['international']}")
    USER_PICKLE[pid] = api_return
    with open(USER_PICKLE_PATH, "wb") as pickle_file:
        pickle.dump(USER_PICKLE, pickle_file)
    return api_return['international']


def map_short_name(official_name):
    """Make a mapping between level names on SRC and the shortened forms I want to use on my charts"""
    if not official_name in SHORT_NAME_MAP:
        raise Exception(f"Level Name Not Mapped: {official_name}")
    return SHORT_NAME_MAP[official_name]

# Pagination gives you at most 200 results, so you gotta call the API again using
# the return value's pagination.links.uri for rel = 'next'
# It ends when the pagination list doesn't have a 'next' value anymore
def query_api(endpoint, game_id="", arg_dict=None):
    """Query the SRC API and return all unpaginated results for endpoint with args.
    
    game_id is parameterized so that different games can be pulled with the same config"""

    formatted_endpoint = endpoint.format(game_id=game_id)

    s = requests.Session()
    retries = Retry(total=10,
                    backoff_factor=.25,
                    status_forcelist=[420])
    s.mount("http://", HTTPAdapter(max_retries=retries))

    # SRC results can either be paginated or unpaginated. Handle the first call, and then if
    # it contains a `pagination` key, go into the paginator workflow
    response = s.get(f"{SRC_API_URL}/{formatted_endpoint}", params=arg_dict)
    time.sleep(SLEEP_INTERVAL)
    if response.status_code != 200:
        print(response.text)
        response.raise_for_status()

    if "pagination" not in response.json():
        if isinstance(response.json()['data'], list):
            print(f"Got {len(response.json()['data'])} results")
        return response.json()['data']

    # Unroll pagination to return a single list of all results
    results_list = []
    call_count = 1
    while response is not None:
        if response.status_code != 200:
            # So far the only error I've seen for valid requests is rate limits, so
            # sleep and retry when it happens
            print(response.text)
            time.sleep(25)
            results = s.get(next_url) if next_url else None
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