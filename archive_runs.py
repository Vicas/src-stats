"""Given a board, download all runs or all world records"""

from run_download import RunDownload, DLStatus
from speedrun_board import SpeedrunBoard
from scraper import get_full_game

import click
from pandas import DataFrame
from pathlib import Path
from typing import List
from yt_dlp import YoutubeDL


@click.command
@click.option("--board", help="Board ID or short-name (in the URL on speedrun.com)")
@click.option("--all-runs/--only-wrs", default=False)
def archive_runs(board_id: SpeedrunBoard, all_runs: bool):
    board_info = get_full_game(board_id)


def download_runs(run_statuses: List[RunDownload], save_folder: Path):
    """Go through a list of RunDownloads and try to download any undownloaded ones into the save_path dir"""
    ydl_options = {
        "format": "bestvideo+bestaudio/best",
        "outtmpl": f"{str(save_folder)}/%(title)s_%(webpage_url)s.%(ext)s"
    }
    url_list = [run.video_url for run in run_statuses]
    with YoutubeDL(ydl_options) as ydl:
        ydl.download(url_list)


def generate_run_downloads(run_list: DataFrame, status_file=None):
    """Generate a download list JSON file of all WR runs"""
    links_flattened = [
        RunDownload(
            run_id=id,
            video_url=uri.get('uri'),
            download_status=DLStatus.NOT_STARTED) 
        for id, link_list 
        in run_list["videos"].apply(
            lambda x: x.get('links',[]) if x else []).items()
        for uri
        in link_list
    ]

    if status_file:
        with open(status_file, "w") as f:
            for link_obj in links_flattened:
                f.write(link_obj.to_json() + "\n")

    return links_flattened


def get_run_downloads_from_file(status_json: Path):
    """Read the status JSON in to make a list of RunDownloads"""
    link_list = []
    with open(status_json, "r") as f:
        for line in f:
            link_list.append(RunDownload.from_json(line))

    return link_list


if __name__ == "__main__":
    archive_runs()