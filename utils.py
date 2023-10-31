"""Random useful util functions"""

from functools import lru_cache

import requests

from config import DATA_PATH, CHART_PATH, SRC_API_URL


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


lru_cache
def get_user_name(pid):
    """Perform a lookup on a src player ID to get their username (english)"""
    return requests.get(f"{SRC_API_URL}/users/{pid}").json()['data']['names']['international']


def map_short_name(official_name):
    """Make a mapping between level names on SRC and the shortened forms I want to use on my charts"""
    if not official_name in SHORT_NAME_MAP:
        raise Exception(f"Level Name Not Mapped: {official_name}")
    return SHORT_NAME_MAP[official_name]
