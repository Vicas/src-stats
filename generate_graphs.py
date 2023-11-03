
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from utils import DATA_PATH, CHART_PATH, get_user_name

# So we don't need to rejoin the data every time we run a graph, we cache it
RUN_JOIN_CACHE = None

plt.tight_layout()

# Common Data Transformations

def join_all_data(filter_users=True, refresh=False):
    """Perform a mega-join of all of our data so we can label levels, categories, users, whatever
    
    filter_users removes Stupid Rat and Rejected runs from the dataset
    refresh recreates the data instead of re-using RUN_JOIN_CACHE
    """
    global RUN_JOIN_CACHE
    if not (refresh or RUN_JOIN_CACHE is None):
        return RUN_JOIN_CACHE

    runs = pd.read_parquet(DATA_PATH / "PT_runs.parquet")
    levels = pd.read_parquet(DATA_PATH / "PT_levels.parquet")
    categories = pd.read_parquet(DATA_PATH / "PT_categories.parquet")


    # Join runs to levels to get level names for axes
    runs_level = pd.merge(runs, levels, left_on='level', right_on='id', how='left', suffixes=('_runs', '_levels'))
    runs_level = pd.merge(runs_level, categories, left_on='category', right_on='id', how='left', suffixes=(None, '_categories'))
    runs_level['Categories'] = runs_level['name_categories']

    # Remove Stupid Rat and Rejected runs
    if filter_users:
        runs_level = runs_level.loc[runs_level['is_rat'] == False]
        runs_level = runs_level.loc[runs_level['status_judgment'] != 'rejected']

    RUN_JOIN_CACHE = runs_level

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


# CSV export, for XBC

def export_joined_runs_csv(refresh=False):
    """Export a CSV with the nested fields removed"""
    joined_run_list = join_all_data(refresh)
    scrubbed_run_list = joined_run_list[[
        'id_runs', 'weblink_runs', 'game', 'level', 'short_name', 'category', 'name_categories',
        'date', 'submitted', 'primary_t', 'pid', 'is_il', 'status_judgment', 
        ]]
    scrubbed_run_list.to_csv(
        DATA_PATH / f"joined_runs_export_{datetime.utcnow().strftime('%Y-%m-%d')}.csv",
        sep="|",
        header=True,
        date_format="%Y-%m-%d %H:%M:%S")


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

def plot_top_ils():
    """Create graphs for the top levels per each IL category"""
    runs = join_all_data(filter_users=True, refresh=True)
    il_counts = runs.groupby(["short_name", "Categories"])["id_runs"].count().unstack("Categories").fillna(0)
    curr_date = datetime.utcnow().strftime('%Y-%m-%d')

    def plot_top_il_helper(category, color):
        """Helper fun for plotting all 3 categories. Order by the category, take the top 10, label the bars, and add a date"""
        top_runs = il_counts[category].sort_values(ascending=False)[:10].sort_values(ascending=True).plot.barh(title=f"Top {category} ILs", color=color)
        top_runs.bar_label(top_runs.containers[0])
        top_runs.annotate(f"Generated on {curr_date}", xy=(1.0,-0.1), xycoords="axes fraction", ha="right", va="center", fontsize=8)

        # Save the figure and close it so the next one doesn't stack
        plt.savefig(CHART_PATH / f"Top_IL_{category}_{curr_date}.png", format="png", bbox_inches="tight")
        plt.close()
        
    plot_top_il_helper("Any%", "C0")
    plot_top_il_helper("All Toppins", "C1")
    plot_top_il_helper("100%", "C2")


def plot_single_il(category, color):
    """Generate a graph for the given IL category
    
    Largely deprecated in favor of plot_top_ils"""
    il_run_count = get_il_counts()
    single_graph = il_run_count.sort_values(category, ascending=False)[category].plot.bar(color=color)

    # Add a legend and label the bars
    single_graph.legend()
    single_graph.bar_label(single_graph.containers[0])

    return single_graph


def plot_top_submitters():
    """Create graphs for both top IL and top fullgame submitters"""
    runs = join_all_data(filter_users=True, refresh=False)

    # Group runs by runners and count up ILs/Fullgame runs
    runner_count = runs.groupby(["runner_name", "is_il"])["id_runs"].count().unstack("is_il").fillna(0)
    runner_count.rename({True: "IL", False: "Full Game"}, axis=1, inplace=True)
    runner_count.loc[:, ("total_count")] = runner_count["IL"] + runner_count["Full Game"]

    # Graph Fullgame submissions
    rc = runner_count['Full Game'].sort_values(ascending=False)[:10].sort_values(ascending=True).plot.barh(title="Top Fullgame Submitters")
    curr_date = datetime.utcnow().strftime('%Y-%m-%d')
    rc.bar_label(rc.containers[0])
    rc.annotate(f"Generated on {curr_date}", xy=(1.0,-0.1), xycoords="axes fraction", ha="right", va="center", fontsize=8)
    plt.savefig(
        CHART_PATH / f"Top_Fullgame_Submitters_{curr_date}.png", format="png", bbox_inches="tight")
    plt.close()

    # Graph IL submissions
    rc = runner_count['IL'].sort_values(ascending=False)[:10].sort_values(ascending=True).plot.barh(title="Top IL Submitters", color='C1')
    curr_date = datetime.utcnow().strftime('%Y-%m-%d')
    rc.bar_label(rc.containers[0])
    rc.annotate(f"Generated on {curr_date}", xy=(1.0,-0.1), xycoords="axes fraction", ha="right", va="center", fontsize=8)
    plt.savefig(
        CHART_PATH / f"Top_IL_Submitters_{curr_date}.png", format="png", bbox_inches="tight")
