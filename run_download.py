import dataclasses
from enum import Enum
import json
from pathlib import Path


class EnhancedJSONEncoder(json.JSONEncoder):
        def default(self, o):
            if dataclasses.is_dataclass(o):
                return dataclasses.asdict(o)
            return super().default(o)


class DLStatus(str, Enum):
    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"
    UNDEFINED = "UNDEFINED"


@dataclasses.dataclass
class RunDownload:
    run_id: str     # Potentially many-to-one, if a run has multiple videos
    video_url: str
    download_status: DLStatus
    local_file_path: Path = None

    def to_json(self):
      return json.dumps(self, cls=EnhancedJSONEncoder)

    @classmethod
    def from_json(cls, json_str):
        json_obj = json.loads(json_str)
        return cls(
            json_obj.get("run_id"),
            json_obj.get("video_url"),
            DLStatus(json_obj.get("download_status", "UNDEFINED")),
            json_obj.get("local_file_path"))
