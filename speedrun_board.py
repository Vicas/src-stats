"""Class for storing and enriching SRC board data"""

from dataclasses import dataclass
from typing import Optional, List

import pandas as pd

from utils import mark_level_era
from utils import map_short_name


# Pizza Tower & CE IDs on speedrun.com
PT_ID = "o6gnpox1"
PT_CE_ID = "pdv99xv1"
PT_DEMO_ID = "j1ne2ex1"


@dataclass
class SpeedrunBoard:
    game: pd.DataFrame
    categories: List[pd.DataFrame]
    levels: List[pd.DataFrame]
    variables: List[pd.DataFrame]
    runs: Optional[List[pd.DataFrame]]


    def __post_init__(self):
        """Unpack the subcategory variables. If no variable exists, return None."""
        for id in self.subcategory_list():
            var_row = self.variables.loc[id]
            self.runs[id] = self.runs['values'].apply(lambda x: var_row['values']['values'][x.get(id)]['label'] if x.get(id) else None)

        self.enrich_levels()
        self.enrich_runs()


    def subcategory_list(self):
        return list(self.variables[self.variables['is-subcategory']].index)


    def enrich_levels(self):
        """Convert the API results to a dataframe and add short_names for display purposes"""
        lev_df = self.levels

        # Add short names for graph display purposes
        lev_df["e_short_name"] = lev_df['name'].apply(lambda x: map_short_name(x))
        lev_df["e_era"] = lev_df['e_short_name'].apply(lambda x: mark_level_era(x))

        self.levels = lev_df


    def enrich_runs(self):
        """Perform standard flattening/cleaning to runs so we can more easily use them in dataframes"""
        run_df = self.runs

        # Make dates into, well, dates
        run_df['date'] = pd.to_datetime(run_df['date'])

        # Tag Stupid Rat runs
        run_df['e_is_rat'] = run_df['players'].apply(lambda x: x[0]['rel'] == 'guest' and x[0]['name'] == 'Stupid Rat')

        # Make primary time a top-level column
        run_df['e_primary_t'] = run_df['times'].apply(lambda x: x['primary_t'])

        # Tag ILs
        run_df['e_is_il'] = run_df['level'].apply(lambda x: 'IL' if x else "Full Game")

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

        # Sort runs by category in order to mark world records
        run_df.sort_values(["date", "submitted"], inplace=True)
        run_df["wr_t"] = run_df.groupby(["level", "category"] + self.subcategory_list())['e_primary_t'].cummin()
        run_df["was_wr"] = run_df.apply(lambda x: x.e_primary_t == x.wr_t, axis=1)

        self.runs = run_df


        def get_world_records() -> pd.DataFrame:
            """Filter on whether or not a run was a WR and return that"""
            return self.runs.loc[self.runs["was_wr"]]
