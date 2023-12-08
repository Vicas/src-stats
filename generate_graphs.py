
from datetime import datetime
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from utils import DATA_PATH, CHART_PATH, get_user_name

# So we don't need to rejoin the data every time we run a graph, we cache it
RUN_JOIN_CACHE = None

plt.tight_layout()

# Common Data Transformations

def join_all_data(filter_users=True, refresh=True):
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

    # Mark runs without a short_name as full game, for convenience
    runs_level["short_name"] = runs_level["short_name"].fillna("Full Game")

    # Remove Stupid Rat and Rejected runs
    if filter_users:
        runs_level = runs_level.loc[runs_level['is_rat'] == False]
        runs_level = runs_level.loc[runs_level['status_judgment'] == 'verified']

    RUN_JOIN_CACHE = runs_level

    return runs_level


def get_il_counts():
    """Load in, join, and group data to get counts of IL runs per level/category"""
    runs_level = join_all_data()
    runs_level = runs_level[runs_level["is_il"]]

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


def get_wr_runs(filter_users=True):
    """Filter the run set to runs that were WR at the time they happened"""
    runs = join_all_data(filter_users=filter_users, refresh=True)
    runs.sort_values(["date", "submitted"], inplace=True)
    runs["wr_t"] = runs.groupby(["Categories", "short_name"])['primary_t'].cummin()
    runs["was_wr"] = runs.apply(lambda x: x.primary_t == x.wr_t, axis=1)

    return runs[runs["was_wr"]].copy()
    

def get_longest_standing_wrs(longest_active=False, fullgame_only=False, filter_users=True, result_count=20):
    """Get the longest-standing WRs"""
    wr_runs = get_wr_runs(filter_users=filter_users)

    # Get the date of the next WR after this one
    wr_runs.loc[:, "next_wr_date"] = wr_runs.groupby(["Categories", "short_name"])['date'].shift(-1)

    # Mark currently-standing WRs, then fill in blank dates for time comparisons
    wr_runs.loc[:, "is_active"] = wr_runs["next_wr_date"].isna()
    wr_runs.loc[:, "next_wr_date"] = wr_runs["next_wr_date"].fillna(np.datetime64("today"))

    # Get how long the record stood for, up to today for active ones
    wr_runs.loc[:, "stood_for"] = (wr_runs['next_wr_date'] - wr_runs['date']).dt.days

    if longest_active:
        wr_runs = wr_runs[wr_runs['is_active']]

    if fullgame_only:
        wr_runs = wr_runs[~wr_runs['is_il']]

    # Return the top 20 longest-standing WRs
    return wr_runs[
        ['id_runs', 'runner_name',
         'short_name', 'Categories',
         'primary_t', 'date', 'next_wr_date',
         'stood_for', 'is_active']
        ].sort_values('stood_for', ascending=False).head(result_count)


def get_leaderboard(category, level, refresh=True):
    """Pull out current active runs for all users on the board and order it by time
    to get the current leaderboard"""
    runs = join_all_data(filter_users=True, refresh=refresh)
    runs = runs[(runs["Categories"] == category) & (runs["short_name"] == level)].sort_values('date')
    latest_runs = runs.groupby("pid").tail(1)
    return latest_runs.sort_values("primary_t")


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

def plot_minute_histogram(
        leaderboard,
        category_name,
        minute_cutoff=1000,
        fill_minutes=False,
        color='C0',
        transparent=False):
    """Plot the number of runs on the leaderboard on per-minute buckets,
    with a cutoff for runs slower than minute_cutoff"""

    # Sort the runs into minute buckets
    leaderboard['minute_time'] = np.floor(leaderboard['primary_t'] / 60)

    # Cutoff runs
    lb_cutoff = leaderboard[leaderboard['minute_time'] <= minute_cutoff]

    lb_minutes = lb_cutoff.groupby(['minute_time']).count()

    if fill_minutes:
        # Fill in missing minutes with 0
        lb_times = range(int(lb_minutes.index.min()), int(lb_minutes.index.max()+1))
        lb_minutes = lb_minutes.reindex(index=lb_times, fill_value=0)

    # Build da graph
    curr_date = datetime.utcnow().strftime('%Y-%m-%d')
    rp = lb_minutes['id'].plot.bar(title=f"{category_name} Minute Barriers", color=color)
    rp.legend(['Players'])
    rp.bar_label(rp.containers[0])
    rp.annotate(f"Generated on {curr_date}", xy=(1.0,-0.2), xycoords="axes fraction", ha="right", va="center", fontsize=8)
    plt.savefig(
        CHART_PATH / f"{category_name}_minute_barriers_{curr_date}.png",
        format="png",
        bbox_inches="tight", 
        transparent=transparent)
    return rp


def plot_runs_per_week(transparent=False):
    """Plot the number of runs per week, split by fullgame/IL"""
    runs = join_all_data(refresh=True)
    runs['run_week'] = pd.to_datetime(runs['date'].dt.to_period('W').dt.start_time)

    runs_per_week = runs.groupby(['run_week', 'is_il'])['id'].count().unstack('is_il')
    runs_per_week["run_week"] = pd.to_datetime(runs_per_week.index)
    runs_per_week['run_week_str'] = runs_per_week['run_week'].apply(lambda x: x.strftime('%b-%d'))

    curr_date = datetime.utcnow().strftime('%Y-%m-%d')
    rp = runs_per_week[["run_week_str", False, True]].plot.bar(x="run_week_str", stacked=True, title="Runs Per Week")
    rp.legend(["Full Game","Individual Level"])

    # TODO: Hilarious hack for getting background stripes per month. We can definitely make this read the data
    plt.axvspan(-0.5, 1.5, facecolor='0.2', alpha=0.2)
    plt.axvspan(5.5, 9.5, facecolor='0.2', alpha=0.2)
    plt.axvspan(13.5, 18.5, facecolor='0.2', alpha=0.2)
    plt.axvspan(22.5, 27.5, facecolor='0.2', alpha=0.2)
    plt.axvspan(31.5, 35.5, facecolor='0.2', alpha=0.2)
    plt.axvspan(40.5, 41.5, facecolor='0.2', alpha=0.2)
    rp.annotate(
        f"Generated on {curr_date}",
        xy=(1.0,-0.2),
        xycoords="axes fraction",
        ha="right",
        va="center",
        fontsize=8)
    plt.savefig(
        CHART_PATH / f"runs_per_week_{curr_date}.png",
        format="png",
        bbox_inches="tight",
        transparent=transparent)
    return rp


def plot_il_graph(transparent=False):
    """Create a full stacked IL graph, ordered by total number of runs"""
    il_run_count = get_il_counts()

    # Use total number of runs to sort the graph
    il_run_count.loc[:, ('total_runs')] = il_run_count['Any%']+il_run_count['All Toppins']+il_run_count['100%']

    # Get plottin'
    curr_date = datetime.utcnow().strftime('%Y-%m-%d')
    x = il_run_count.sort_values('total_runs', ascending=False)[['Any%','All Toppins', '100%']].plot.bar(stacked=True, title="Runs Per Level")
    x.annotate(
        f"Generated on {curr_date}",
        xy=(1.0,-0.5),
        xycoords="axes fraction",
        ha="right",
        va="center",
        fontsize=8)
    plt.savefig(
        CHART_PATH / f"runs_per_level_{curr_date}.png",
        format="png",
        bbox_inches="tight",
        transparent=transparent)
    return x


def plot_top_ils(transparent=False):
    """Create graphs for the top levels per each IL category"""
    il_counts = get_il_counts()
    curr_date = datetime.utcnow().strftime('%Y-%m-%d')

    def plot_top_il_helper(category, color):
        """Helper fun for plotting all 3 categories. Order by the category, take the top 10, label the bars, and add a date"""
        top_runs = il_counts[category].sort_values(ascending=False)[:10].sort_values(ascending=True).plot.barh(title=f"Top {category} ILs", color=color)
        top_runs.bar_label(top_runs.containers[0])
        top_runs.annotate(
            f"Generated on {curr_date}",
            xy=(1.0,-0.1),
            xycoords="axes fraction",
            ha="right",
            va="center",
            fontsize=8)

        # Save the figure and close it so the next one doesn't stack
        plt.savefig(CHART_PATH / f"Top_IL_{category}_{curr_date}.png",
                    format="png",
                    bbox_inches="tight",
                    transparent=transparent)
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


def plot_top_submitters(transparent=False):
    """Create graphs for both top IL and top fullgame submitters"""
    runs = join_all_data(filter_users=True, refresh=False)

    # Group runs by runners and count up ILs/Fullgame runs
    runner_count = runs.groupby(["runner_name", "is_il"])["id_runs"].count().unstack("is_il").fillna(0)
    runner_count.rename({True: "IL", False: "Full Game"}, axis=1, inplace=True)
    runner_count.loc[:, ("total_count")] = runner_count["IL"] + runner_count["Full Game"]

    def plot_top_submitters_helper(count_field, title, color="C0"):
        rc = runner_count[count_field].sort_values(ascending=False)[:10]\
            .sort_values(ascending=True).plot.barh(title=title, color=color)
        curr_date = datetime.utcnow().strftime('%Y-%m-%d')
        rc.bar_label(rc.containers[0])
        rc.annotate(
            f"Generated on {curr_date}",
            xy=(1.0,-0.1),
            xycoords="axes fraction",
            ha="right",
            va="center",
            fontsize=8)
        plt.savefig(
            CHART_PATH / f"{title}_{curr_date}.png",
            format="png",
            bbox_inches="tight",
            transparent=transparent)
        plt.close()

    plot_top_submitters_helper("Full Game", title="Top Full Game Submitters", color="C1")
    plot_top_submitters_helper("IL", title="Top IL Submitters", color="C2")
    plot_top_submitters_helper("total_count", title="Top Submitters", color="C0")


def plot_long_standing_wrs(
        wr_list,
        full_game,
        title="Longest Standing World Records",
        color="C0",
        legend=True,
        transparent=False):
    """Given a list of WRs and how long they've stood, plot 'em"""
    # Create a title col out of the other cols
    format_str = "{runner_name}'s {Category}\n({date} to {next_wr_date})'"\
         if full_game else "{runner_name}'s {short_name} {Category}\n({date} to {next_wr_date})'"
    wr_list["Title"] = wr_list.apply(
        lambda x: format_str\
            .format(
                runner_name=x.runner_name,
                short_name=x.short_name,
                Category=x.Categories,
                date=x.date.strftime("%y-%m-%d"),
                next_wr_date="Now" if x.is_active else x.next_wr_date.strftime("%y-%m-%d")),
            axis=1)

    wr_list["Days"] = wr_list["stood_for"]

    curr_date = datetime.utcnow().strftime('%Y-%m-%d')

    lwr = wr_list[:10].sort_values("stood_for", ascending=True).plot.barh(x='Title', y='Days', title=title, color=color, legend=legend)
    lwr.bar_label(lwr.containers[0])
    lwr.annotate(
        f"Generated on {curr_date}",
        xy=(1.0,-0.1),
        xycoords="axes fraction",
        ha="right",
        va="center",
        fontsize=8)
    plt.savefig(
        CHART_PATH / f"{title}_{curr_date}.png",
        format="png",
        bbox_inches="tight",
        transparent=transparent)