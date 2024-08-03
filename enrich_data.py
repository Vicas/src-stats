"""Script for adding fields to the raw dataframes for easier/better graph generation.

Fields added by these transformations will have e_ prepended to them to denote they
aren't base API fields."""

from utils import map_short_name, get_user_name

import pandas as pd

def mark_level_era(level_name):
    """For now, if this is tricky treat or Secrets of the world, it's 2023 Halloween,
    otherwise it's Main Game. I guess we should mark SAGE too"""
    if level_name in ("Tricky Treat", "Secrets of the World"):
        return "2023 Halloween"
    if "(SAGE)" in level_name:
        return "SAGE Demo"
    return "Main Game"


def enrich_levels(lev_df):
    """Convert the API results to a dataframe and add short_names for display purposes"""

    # Add short names for graph display purposes
    lev_df["e_short_name"] = lev_df['name'].apply(lambda x: map_short_name(x))
    lev_df["e_era"] = lev_df['e_short_name'].apply(lambda x: mark_level_era(x))

    return lev_df


def enrich_categories(cat_df):
    """Convert the API results to a dataframe, and no other enrichments for now"""
    return cat_df


def enrich_runs(run_df):
    """Perform standard flattening/cleaning to runs so we can more easily use them in dataframes"""

    # Make dates into, well, dates
    run_df['date'] = pd.to_datetime(run_df['date'])

    # Tag Stupid Rat runs
    run_df['e_is_rat'] = run_df['players'].apply(lambda x: x[0]['rel'] == 'guest' and x[0]['name'] == 'Stupid Rat')

    # Make primary time a top-level column
    run_df['e_primary_t'] = run_df['times'].apply(lambda x: x['primary_t'])

    # Tag ILs
    run_df['e_is_il'] = run_df['level'].apply(lambda x: x is not None)

    # Extract playerids, if you want 'em
    run_df['e_pid'] = run_df['players'].apply(lambda x: x[0]['id'] if 'id' in x[0] else None)

    # Extract run status
    run_df['e_status_judgment'] = run_df['status'].apply(lambda x: x['status'])

    # Commenting out the runner's username here so we don't do an expensive lookup for all
    # usernames who've ever submitted. Sub in pid for now so it doesn't totally break graphing
    run_df['e_runner_name'] = run_df['e_pid']
    '''
    # Get the runner's username
    run_df['e_runner_name'] = run_df['e_pid'].apply(
        lambda x: get_user_name(x) if x else "Guest"
    )
    '''

    return run_df