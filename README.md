# Pizza Tower Stats And Graphs

A Python/Pandas library for analyzing and creating graphs about runs on [speedrun.com](https://speedrun.com). Uses the [Speedrun.com API](https://github.com/speedruncomorg/api) to pull runs and game info and builds visualizations of run submissions over time, minute-barriers, and much more.

This tool was originally written for the Pizza Tower speedrun board, but it should be able to handle most boards (though actually building and testing that is a WIP).

**!TODO: Add some example graphs**

## Getting Started

This repo expects you to be a bit familiar with Python and Pandas, and currently installs on Linux/WSL since that's where I felt like putting it. Native Windows functionality shouldn't be too hard but will require a once-over for incompatible code.

### Prerequisites
If you're running this on Windows, you'll want to start by [installing WSL](https://learn.microsoft.com/en-us/windows/wsl/install). This tutorial assumes zero python/development experience, so if you're used to git and venvs, feel free to skip a few steps and handle this how you usually do.

### Installing
In a WSL terminal, navigate to your home folder, clone the repo, and create a venv to run in:
```
cd ~
git clone https://github.com/Vicas/src-stats.git
cd src-stats
pip install virtualenv
python -m virtualenv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

This will copy the code into a new folder, create a [Python virtualenv](https://docs.python.org/3/library/venv.html), and install the necesary Python libraries in that virtualenv.

## Usage
While you can run these libraries directly in your terminal, since these libraries generate images and you may want to tweak your graphs until they look right, I highly recommend using [Jupyter](https://docs.python.org/3/library/venv.html) to interact with them.

To run Jupyter locally, with your virtualenv activated (the `source .venv/bin/activate` call above), run:

```
jupyter notebook
```

This will start a local Jupyter server on your machine that you can access in a browser with the URL in your terminal. It should look similar to http://localhost/tree?token=token_string. The file-tree on this page should be all the files in this repo, so you should see and open `sample_notebook.ipynb`. This notebook contains code for downloading runs of your game of choice and creating some graphs based on it. You can execute individual cells in Jupyter Notebooks by selecting the cell and pressing `Shift+Enter`, and even change the code and re-run it, so try playing with the code to see how it works.