from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional


@dataclass
class ChannelInfo:
    display_name: str = ""
    subscribers: int = 0
    description: str = ""


@dataclass
class VideoTask:
    video_id: str
    title: str = ""
    duration: Optional[int] = None
    uploader: str = ""
    url: str = ""

    def resolved_url(self) -> str:
        from .helpers import make_watch_url  # lazy import to avoid cycles
        return self.url or make_watch_url(self.video_id)


@dataclass
class ProgressState:
    label: str = "Idle"
    detail: str = ""
    downloaded_bytes: float = 0.0
    total_bytes: Optional[float] = None
    speed: Optional[float] = None
    eta: Optional[int] = None
    show_transfer: bool = False
    last_percent: Optional[int] = None
    last_emit: float = 0.0
    last_render_len: int = 0
    inline_active: bool = False
    batch_index: int = 0
    batch_total: int = 0


@dataclass
class CurrentVideoState:
    tmp_file: Optional[Path] = None
    tmp_dir: Optional[Path] = None
    folder: str = ""
    vid: str = ""
    video_dir: Optional[Path] = None
    ext: str = ""
    output_root: Path = field(default_factory=Path.cwd)
    channel_info: ChannelInfo = field(default_factory=ChannelInfo)
    current_stage: str = "Idle"
    stage_detail: str = ""
    filter_videos_only: bool = False

    def configure(self, output_root: Path, channel_info: ChannelInfo, filter_videos_only: bool = False):
        self.output_root = output_root
        self.channel_info = channel_info
        self.filter_videos_only = filter_videos_only

    def clear(self):
        self.tmp_file = None
        self.tmp_dir = None
        self.folder = ""
        self.vid = ""
        self.video_dir = None
        self.ext = ""
        self.current_stage = "Idle"
        self.stage_detail = ""


def serialize_video_task(task: VideoTask) -> dict:
    return {
        "video_id": task.video_id,
        "title": task.title,
        "duration": task.duration,
        "uploader": task.uploader,
        "url": task.url,
    }


def deserialize_video_task(payload: Mapping[str, Any]) -> VideoTask:
    return VideoTask(
        video_id=str(payload.get("video_id", "")),
        title=str(payload.get("title", "")),
        duration=payload.get("duration"),
        uploader=str(payload.get("uploader", "")),
        url=str(payload.get("url", "")),
    )
