
from datetime import datetime, timezone
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from config import BoardInfo
from utils import DATA_PATH, CHART_PATH, get_user_name


plt.tight_layout()

# Common Data Transformations

def join_all_data(board_info: BoardInfo, filter_users=True):
    """Perform a mega-join of all of our data so we can label levels, categories, users, whatever
    
    filter_users removes Stupid Rat and Rejected runs from the dataset
    """

    runs = pd.read_parquet(DATA_PATH / "PT_runs.parquet")
    levels = pd.read_parquet(DATA_PATH / "PT_levels.parquet")
    categories = pd.read_parquet(DATA_PATH / "PT_categories.parquet")


    # Join runs to levels to get level names for axes
    runs_level = pd.merge(runs, levels, left_on='level', right_on='id', how='left', suffixes=('_runs', '_levels'))
    runs_level = pd.merge(runs_level, categories, left_on='category', right_on='id', how='left', suffixes=(None, '_categories'))
    runs_level['Categories'] = runs_level['name_categories']

    # Mark runs without a short_name as full game, for convenience
    runs_level["e_short_name"] = runs_level["e_short_name"].fillna("Full Game")

    # Remove Stupid Rat and Rejected runs
    if filter_users:
        runs_level = runs_level.loc[not runs_level['e_is_rat']]
        runs_level = runs_level.loc[runs_level['e_status_judgment'] == 'verified']

    return runs_level


def get_il_counts():
    """Load in, join, and group data to get counts of IL runs per level/category"""
    runs_level = join_all_data()
    runs_level = runs_level[runs_level["e_is_il"]]

    # Get run counts broken up by level and category
    return runs_level.groupby(['e_short_name', 'Categories']).count().unstack('Categories').loc[:, ('id_runs')].fillna(0)


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
    runs = join_all_data(filter_users=filter_users)
    runs.sort_values(["date", "submitted"], inplace=True)
    runs["wr_t"] = runs.groupby(["Categories", "e_short_name"])['e_primary_t'].cummin()
    runs["was_wr"] = runs.apply(lambda x: x.e_primary_t == x.wr_t, axis=1)

    return runs[runs["was_wr"]].copy()
    

def get_longest_standing_wrs(longest_active=False, fullgame_only=False, filter_users=True, result_count=20):
    """Get the longest-standing WRs"""
    wr_runs = get_wr_runs(filter_users=filter_users)

    # Get the date of the next WR after this one
    wr_runs.loc[:, "next_wr_date"] = wr_runs.groupby(["Categories", "e_short_name"])['date'].shift(-1)

    # Mark currently-standing WRs, then fill in blank dates for time comparisons
    wr_runs.loc[:, "is_active"] = wr_runs["next_wr_date"].isna()
    wr_runs.loc[:, "next_wr_date"] = wr_runs["next_wr_date"].fillna(np.datetime64("today"))

    # Get how long the record stood for, up to today for active ones
    wr_runs.loc[:, "stood_for"] = (wr_runs['next_wr_date'] - wr_runs['date']).dt.days

    if longest_active:
        wr_runs = wr_runs[wr_runs['is_active']]

    if fullgame_only:
        wr_runs = wr_runs[~wr_runs['e_is_il']]

    # Return the top 20 longest-standing WRs
    return wr_runs[
        ['id_runs', 'e_runner_name',
         'e_short_name', 'Categories',
         'e_primary_t', 'date', 'next_wr_date',
         'stood_for', 'is_active']
        ].sort_values('stood_for', ascending=False).head(result_count)


def get_leaderboard(category, level, barrier_cutoff_date=None):
    """Pull out current active runs for all users on the board and order it by time
    to get the current leaderboard"""
    runs = join_all_data(filter_users=True)
    runs = runs[(runs["Categories"] == category) & (runs["e_short_name"] == level)].sort_values('date')

    latest_runs = runs.groupby("e_pid").tail(1)

    # If I'm marking new minute barriers, get a copy of the leaderboard as of the barrier_cutoff_date
    if barrier_cutoff_date:
        runs_before_cutoff = runs[runs['date'] <= barrier_cutoff_date]
        latest_before_cutoff = runs_before_cutoff.groupby("e_pid").tail(1)
        latest_runs = pd.merge(
            latest_runs,
            latest_before_cutoff[['e_pid', 'e_primary_t']],
            left_on="e_pid",
            right_on="e_pid",
            how="left",
            suffixes=(None, "_prior"))

    return latest_runs.sort_values("e_primary_t")


# CSV export, for XBC

def export_joined_runs_csv():
    """Export a CSV with the nested fields removed"""
    joined_run_list = join_all_data()
    scrubbed_run_list = joined_run_list[[
        'id_runs', 'weblink_runs', 'game', 'level', 'e_short_name', 'category', 'name_categories',
        'date', 'submitted', 'e_primary_t', 'e_pid', 'e_is_il', 'e_status_judgment', 
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
    leaderboard['minute_time'] = np.floor(leaderboard['e_primary_t'] / 60)

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

def plot_minute_histogram_with_new_runs(
        leaderboard,
        category_name,
        minute_cutoff=1000,
        fill_minutes=False,
        color='C0',
        new_run_color='C1',
        transparent=False):
    """Try out the new subplot-based process for creating graphs"""

    # Sort the runs into minute buckets, both old and new
    leaderboard['minute_time'] = np.floor(leaderboard['e_primary_t'] / 60)
    leaderboard['last_month_minute_time'] = np.floor(leaderboard['e_primary_t_prior'] / 60).fillna(900000000.0)
    leaderboard['new_minute_barrier'] = leaderboard['minute_time'] < leaderboard['last_month_minute_time']

    # Cutoff runs
    lb_cutoff = leaderboard[leaderboard['minute_time'] <= minute_cutoff]
    lb_minutes = lb_cutoff.groupby(['minute_time', 'new_minute_barrier']).count().unstack('new_minute_barrier').fillna(0)

    if fill_minutes:
        # Fill in missing minutes with 0
        lb_times = range(int(lb_minutes.index.min()), int(lb_minutes.index.max()+1))
        lb_minutes = lb_minutes.reindex(index=lb_times, fill_value=0)

    minutes = lb_minutes['id'].copy()
    minutes['total'] = minutes[False] + minutes[True]
    minutes['total'] = minutes['total'].fillna(0)

    # Build da graph
    curr_date = datetime.utcnow().strftime('%Y-%m-%d')
    fig, ax = plt.subplots()

    p = ax.bar(minutes.index.values, minutes['total'])
    ax.bar(minutes.index.values, minutes[False], color=color, label="Older Runs")
    ax.bar(
        minutes.index.values,
        minutes[True],
        bottom=minutes[False],
        color=new_run_color,
        label="New Runs")
    
    ax.bar_label(p)
    ax.legend()
    plt.xticks(minutes.index.values, rotation=90)

    plt.ylim(0, max(minutes['total']+2))
    plt.xlim(min(minutes.index.values)-1, max(minutes.index.values)+1)
    plt.title(f"{category_name} Minute Barriers")
    plt.xlabel("Run Minutes")
    plt.ylabel("Players")

    ax.annotate(f"Generated on {curr_date}", xy=(1.0,-0.15), xycoords="axes fraction", ha="right", va="center", fontsize=8)
    plt.savefig(
        CHART_PATH / f"{category_name}_minute_barriers_{curr_date}.png",
        format="png",
        bbox_inches="tight", 
        transparent=transparent)


def plot_runs_per_week(
        board_info: BoardInfo,
        start_date:datetime,
        end_date:datetime=None,
        il_split=False,
        save_fig_path=None,
        transparent=False):
    """Plot the number of runs per week, split by fullgame/IL"""
    runs = board_info.runs
    runs['run_week'] = pd.to_datetime(runs['date'].dt.to_period('W').dt.start_time)

    if il_split:
        runs_per_week = runs.groupby(['run_week', 'e_is_il'])['id'].count().unstack('e_is_il')
    else:
        runs_per_week = pd.DataFrame(runs.groupby('run_week')['id'].count())

    runs_per_week["run_week"] = pd.to_datetime(runs_per_week.index)
    runs_per_week['run_week_str'] = runs_per_week['run_week'].apply(lambda x: x.strftime('%b-%d'))

    # Select down to the start/end dates provided
    end_date = end_date or datetime.now()
    mask = (runs_per_week['run_week'] >= start_date) & (runs_per_week['run_week'] <= end_date)
    runs_per_week = runs_per_week.loc[mask]

    curr_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    title = f"{board_info.game['names']['international']} Runs Per Week"
    if il_split:
        rp = runs_per_week.plot.bar(x="run_week_str", y=["Full Game", "IL"], stacked=True, title=title)
    else:
        rp = runs_per_week.plot.bar(x="run_week_str", y='id', title=title)
        rp.legend(["Runs"])

    rp.annotate(
        f"Generated on {curr_date}",
        xy=(1.0,-0.2),
        xycoords="axes fraction",
        ha="right",
        va="center",
        fontsize=8)

    if save_fig_path:
        plt.savefig(
            save_fig_path,
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
    runs = join_all_data(filter_users=True)

    # Group runs by runners and count up ILs/Fullgame runs
    runner_count = runs.groupby(["e_runner_name", "e_is_il"])["id_runs"].count().unstack("e_is_il").fillna(0)
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
    format_str = "{e_runner_name}'s {Category}\n({date} to {next_wr_date})'"\
         if full_game else "{e_runner_name}'s {e_short_name} {Category}\n({date} to {next_wr_date})'"
    wr_list["Title"] = wr_list.apply(
        lambda x: format_str\
            .format(
                e_runner_name=x.e_runner_name,
                e_short_name=x.e_short_name,
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