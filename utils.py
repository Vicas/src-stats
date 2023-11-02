"""Random useful util functions"""

from functools import lru_cache
import pickle
import time

import requests

from config import DATA_PATH, CHART_PATH, SRC_API_URL

# Store user id-to-name mappings in dict, but only load it in when get_user_name is called
USER_PICKLE_PATH = DATA_PATH / "SRC_users.pkl"
USER_PICKLE = None


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
    "Fake Peppino": "Fake Peppino",
    "Pizzaface": "Pizzaface",
    "Secrets of the World": "Secrets of the World",
    "Tricky Treat": "Tricky Treat",
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
    retry_count = 0
    while retry_count < 3:
        api_return = requests.get(f"{SRC_API_URL}/users/{pid}").json()
        if not 'data' in api_return:
            print(api_return)
            time.sleep(20)
            retry_count += 1
        else:
            break

    api_return = api_return["data"]["names"]
    USER_PICKLE[pid] = api_return
    with open(USER_PICKLE_PATH, "wb") as pickle_file:
        pickle.dump(USER_PICKLE, pickle_file)
    return api_return['international']


def map_short_name(official_name):
    """Make a mapping between level names on SRC and the shortened forms I want to use on my charts"""
    if not official_name in SHORT_NAME_MAP:
        raise Exception(f"Level Name Not Mapped: {official_name}")
    return SHORT_NAME_MAP[official_name]
