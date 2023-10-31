
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from utils import DATA_PATH, CHART_PATH, get_user_name


plt.tight_layout()

# Common Data Transformations

def join_all_data():
    """Perform a mega-join of all of our data so we can label levels, categories, users, whatever"""
    runs = pd.read_parquet(DATA_PATH / "PT_runs.parquet")
    levels = pd.read_parquet(DATA_PATH / "PT_levels.parquet")
    categories = pd.read_parquet(DATA_PATH / "PT_categories.parquet")


    # Join runs to levels to get level names for axes
    runs_level = pd.merge(runs, levels, left_on='level', right_on='id', how='left', suffixes=('_runs', '_levels'))
    runs_level = pd.merge(runs_level, categories, left_on='category', right_on='id', how='left', suffixes=(None, '_categories'))
    runs_level['Categories'] = runs_level['name_categories']

    runs_level = runs_level.loc[runs_level['is_rat'] == False]
    runs_level = runs_level.loc[runs_level['status_judgment'] != 'rejected']

    return runs_level


def get_il_counts():
    """Load in, join, and group data to get counts of IL runs per level/category"""
    runs_level = join_all_data()

    # Get run counts broken up by level and category
    return runs_level.groupby(['short_name', 'Categories']).count().unstack('Categories').loc[:, ('id_runs')].fillna(0)


def get_verifier_stats():
    """This one's just for me, get a list of who's verified the most runs, lol"""
    runs = pd.read_parquet(DATA_PATH / "PT_runs.parquet")
    runs['examiner'] = runs['status'].apply(lambda x: x['examiner'])

    z = runs.groupby('examiner').count()
    z['verifier_name'] = z.apply(lambda x: get_user_name(x.name), axis=1)
    z = z[['id','verifier_name']].sort_values('id',ascending=False)
    return z


# Actual Graphing Functions

def plot_minute_histogram(run_filepath, category_name, minute_cutoff=1000, color='C0'):
    """Plot the number of runs on the leaderboard on per-minute buckets, with a cutoff for runs slower than minute_cutoff"""
    runs = pd.read_parquet(run_filepath)

    # Sort the runs into minute buckets
    runs['minute_time'] = np.floor(runs['primary_t'] / 60)

    # Cutoff runs
    run_cutoff = runs[runs['minute_time'] <= minute_cutoff]

    run_minutes = run_cutoff.groupby(['minute_time']).count()

    # Build da graph
    curr_date = datetime.utcnow().strftime('%Y-%m-%d')
    rp = run_minutes['id'].plot.bar(title=f"{category_name} Minute Barriers", color=color)
    rp.legend(['Players'])
    rp.bar_label(rp.containers[0])
    rp.annotate(f"Generated on {curr_date}", xy=(1.0,-0.2), xycoords="axes fraction", ha="right", va="center", fontsize=8)
    plt.savefig(
        CHART_PATH / f"{category_name}_minute_barriers_{curr_date}.png", format="png", bbox_inches="tight")
    return rp


def plot_runs_per_week():
    """Plot the number of runs per week, split by fullgame/IL"""
    runs = join_all_data()
    runs['run_week'] = runs['date'].dt.to_period('W').dt.start_time
    runs['run_week_str'] = runs['run_week'].apply(lambda x: x.strftime('%Y-%m-%d'))

    runs_per_week = runs.groupby(['run_week_str', 'is_il'])['id'].count().unstack('is_il')

    curr_date = datetime.utcnow().strftime('%Y-%m-%d')
    rp = runs_per_week.plot.bar(stacked=True, title="Runs Per Week")
    rp.legend(["Full Game","Individual Level"])
    rp.annotate(f"Generated on {curr_date}", xy=(1.0,-0.35), xycoords="axes fraction", ha="right", va="center", fontsize=8)
    plt.savefig(
        CHART_PATH / f"runs_per_week_{curr_date}.png", format="png", bbox_inches="tight")
    return rp


def plot_il_graph():
    """Create a full stacked IL graph, ordered by total number of runs"""
    il_run_count = get_il_counts()

    # Use total number of runs to sort the graph
    il_run_count.loc[:, ('total_runs')] = il_run_count['Any%']+il_run_count['All Toppins']+il_run_count['100%']

    # Get plottin'
    curr_date = datetime.utcnow().strftime('%Y-%m-%d')
    x = il_run_count.sort_values('total_runs', ascending=False)[['Any%','All Toppins', '100%']].plot.bar(stacked=True, title="Runs Per Level")
    x.annotate(f"Generated on {curr_date}", xy=(1.0,-0.5), xycoords="axes fraction", ha="right", va="center", fontsize=8)
    plt.savefig(
        CHART_PATH / f"runs_per_level_{curr_date}.png", format="png", bbox_inches="tight")
    return x


def plot_single_il(category, color):
    """Generate a graph for the given IL category"""
    il_run_count = get_il_counts()
    single_graph = il_run_count.sort_values(category, ascending=False)[category].plot.bar(color=color)

    # Add a legend and label the bars
    single_graph.legend()
    single_graph.bar_label(single_graph.containers[0])

    return single_graph
