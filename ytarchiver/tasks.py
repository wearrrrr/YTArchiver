import logging
from typing import Iterable, List

from yt_dlp import YoutubeDL

from .helpers import make_watch_url
from .state import VideoTask

LOG = logging.getLogger("ytarchiver")


def extract_video_tasks(entries, default_uploader: str = "") -> List[VideoTask]:
    tasks: List[VideoTask] = []
    for entry in entries or []:
        entry_type = entry.get("_type")
        if entry_type == "url" and entry.get("id"):
            ie_key = (entry.get("ie_key", "") or "").lower()
            if "youtube" not in ie_key:
                continue
            video_id = entry["id"]
            title = (entry.get("title") or entry.get("fulltitle") or "").strip()
            url = entry.get("url") or entry.get("webpage_url") or ""
            uploader = entry.get("uploader") or entry.get("channel") or default_uploader
            tasks.append(
                VideoTask(
                    video_id=video_id,
                    title=title,
                    duration=entry.get("duration"),
                    uploader=uploader,
                    url=url,
                )
            )
        elif entry.get("entries"):
            tasks.extend(extract_video_tasks(entry["entries"], default_uploader))
    return tasks


def fetch_video_listing(target_url: str) -> tuple[dict, List[VideoTask]]:
    with YoutubeDL({"extract_flat": True, "quiet": True}) as ydl:
        info = ydl.extract_info(target_url, download=False)
    entries = info.get("entries") or []
    default_uploader = str(info.get("channel") or info.get("uploader") or "")
    tasks = extract_video_tasks(entries, default_uploader)
    return info, tasks


def fetch_tasks_for_video_ids(video_ids: Iterable[str]) -> List[VideoTask]:
    tasks: List[VideoTask] = []
    ids = list(video_ids)
    if not ids:
        return tasks

    opts = {
        "quiet": True,
        "remote_components": "ejs:github",
        "cookiesfrombrowser": ("brave", None, None, None)
    }
    with YoutubeDL(opts) as ydl:
        for video_id in ids:
            video_url = make_watch_url(video_id)
            info = {}
            try:
                info = ydl.extract_info(video_url, download=False)
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Metadata lookup failed for %s (%s)", video_url, exc)
            tasks.append(
                VideoTask(
                    video_id=video_id,
                    title=(info.get("title") or "").strip() if info else "",
                    duration=info.get("duration") if info else None,
                    uploader=(info.get("uploader") or info.get("channel") or "") if info else "",
                    url=info.get("webpage_url") if info else video_url,
                )
            )
    return tasks
