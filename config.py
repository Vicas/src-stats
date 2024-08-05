"""Finally making a script for storing configs"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, List


DATA_PATH = Path(__file__).parent / "data"
CHART_PATH = Path(__file__).parent / "charts"

SRC_API_URL = "https://www.speedrun.com/api/v1"

# Pizza Tower & CE IDs on speedrun.com
PT_ID = "o6gnpox1"
PT_CE_ID = "pdv99xv1"
PT_DEMO_ID = "j1ne2ex1"

@dataclass
class BoardInfo:
    game: Dict
    categories: List[Dict]
    levels: List[Dict]
    variables: List[Dict]
    runs: Optional[List[Dict]]
