import logging
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Sequence

from yt_dlp import YoutubeDL

from .console import render_task_banner
from .context import channel_info, video_state
from .helpers import normalize_video_ids
from .postprocess import on_postprocess, postprocess_subs
from .progress import progress_hook, progress_state, reset_progress_state, set_stage
from .state import VideoTask
from .tasks import fetch_tasks_for_video_ids, fetch_video_listing

LOG = logging.getLogger("ytarchiver")
ARCHIVE_LOCK = threading.Lock()


class JobInterrupted(RuntimeError):
    """Raised when a running job is interrupted via pause/stop."""

    def __init__(self, reason: str = "stopped"):
        super().__init__(reason)
        self.reason = reason or "stopped"


class JobControl:
    """Lightweight control channel for cooperative job interruption."""

    def __init__(self):
        self._lock = threading.Lock()
        self._reason: str | None = None

    def request_pause(self):
        self._set_reason("paused")

    def request_stop(self):
        self._set_reason("stopped")

    def _set_reason(self, reason: str):
        with self._lock:
            if self._reason is None:
                self._reason = reason

    def pending_reason(self) -> str | None:
        with self._lock:
            return self._reason


@dataclass
class ArchiveConfig:
    command: str
    handle: str | None = None
    video_ids: Sequence[str] = field(default_factory=list)
    out: str = "yt"
    subs: bool = False
    no_cache: bool = False
    log_file: str = "logs/ytarchiver.log"
    log_level: str = "INFO"
    clear_screen: bool = True


def _configure_logging(log_file: Path, level_name: str) -> logging.Logger:
    level = getattr(logging, level_name.upper(), logging.INFO)

    logger = logging.getLogger("ytarchiver")
    logger.setLevel(level)
    logger.handlers.clear()
    logger.propagate = False

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(console_handler)

    ytdlp_logger = logging.getLogger("ytarchiver.ytdlp")
    ytdlp_logger.setLevel(level)
    ytdlp_logger.handlers.clear()
    ytdlp_logger.propagate = True

    return ytdlp_logger


def _normalize_handle(handle: str) -> str:
    cleaned = handle.strip()
    if not cleaned.startswith("@"):
        cleaned = f"@{cleaned}"
    return cleaned


def _build_channel_url(handle: str, shorts: bool) -> str:
    base = f"https://www.youtube.com/{handle}"
    return f"{base}/shorts" if shorts else base


def _build_ydl_options(download_archive: Path | None, download_subs: bool, ytdlp_logger: logging.Logger) -> dict:
    opts = {
        "ignoreerrors": True,
        "outtmpl": "%(id)s.%(ext)s",
        "remux_video": "mkv",
        "merge_output_format": "mkv",
        "postprocessors": [
            {"key": "FFmpegMetadata"},
            {"key": "EmbedThumbnail"},
        ],
        "postprocessor_hooks": [on_postprocess, postprocess_subs],
        "progress_hooks": [progress_hook],
        "logger": ytdlp_logger,
    }

    if download_archive:
        opts["download_archive"] = str(download_archive)

    if download_subs:
        opts.update(
            {
                "writesubtitles": True,
                "subtitleslangs": ["all"],
                "subtitlesformat": "srv3",
                "live_chat": True,
            }
        )

    return opts


def _capture_channel_meta() -> dict | None:
    if not (channel_info.display_name or channel_info.description or channel_info.subscribers):
        return None
    return {
        "display_name": channel_info.display_name,
        "description": channel_info.description,
        "subscribers": channel_info.subscribers,
    }


def _apply_channel_meta(meta: dict | None):
    if not meta:
        channel_info.display_name = ""
        channel_info.description = ""
        channel_info.subscribers = 0
        return
    channel_info.display_name = str(meta.get("display_name") or "")
    channel_info.description = str(meta.get("description") or "")
    channel_info.subscribers = int(meta.get("subscribers") or 0)


def _queue_tasks(config: ArchiveConfig) -> List[VideoTask]:
    if config.command in {"channel", "shorts"}:
        if not config.handle:
            raise RuntimeError("A channel handle is required for this command.")
        handle = _normalize_handle(config.handle)
        target_url = _build_channel_url(handle, shorts=config.command == "shorts")
        LOG.info("Fetching %s list for %s", config.command, handle)
        info, tasks = fetch_video_listing(target_url)
        if not tasks:
            raise RuntimeError(f"No videos found for {target_url}")
        channel_info.display_name = str(info.get("channel") or info.get("uploader") or handle).strip()
        channel_info.subscribers = int(info.get("channel_follower_count") or 0)
        channel_info.description = str(info.get("description") or "")
        LOG.info("Queued %s video(s) for %s", len(tasks), channel_info.display_name or handle)
        return tasks

    if config.command == "video":
        ids = normalize_video_ids(config.video_ids)
        if not ids:
            raise RuntimeError("No valid video IDs or URLs provided.")
        tasks = fetch_tasks_for_video_ids(ids)
        LOG.info("Queued %s provided video(s).", len(tasks))
        return tasks

    raise RuntimeError(f"Unsupported command: {config.command}")


def prepare_tasks(config: ArchiveConfig) -> tuple[List[VideoTask], dict | None]:
    tasks = _queue_tasks(config)
    return tasks, _capture_channel_meta()


def _run_downloads(
    tasks: List[VideoTask],
    ydl_opts: dict,
    clear_screen: bool,
    start_index: int = 1,
    checkpoint_cb: Callable[[int, VideoTask], None] | None = None,
    job_control: JobControl | None = None,
):
    if not tasks:
        LOG.warning("No videos matched the provided criteria.")
        return

    total = len(tasks)
    start = max(1, start_index)
    if start > total:
        LOG.info("All queued videos already processed.")
        return

    for index in range(start, total + 1):
        if job_control:
            reason = job_control.pending_reason()
            if reason:
                raise JobInterrupted(reason)

        task = tasks[index - 1]
        video_state.clear()
        reset_progress_state(detail=f"Waiting on {task.video_id}")
        progress_state.batch_index = index
        progress_state.batch_total = total
        render_task_banner(index, total, task, task.uploader, clear_screen)
        video_url = task.resolved_url()
        LOG.info("[%s/%s] Downloading %s", index, total, video_url)
        try:
            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
            target_path = str(video_state.video_dir) if video_state.video_dir else video_url
            set_stage("Completed", f"Saved to {target_path}", show_transfer=False)
        except Exception as exc:  # noqa: BLE001
            set_stage("Error", str(exc), show_transfer=False)
            LOG.error("Failed to download %s (%s)", video_url, exc)
        finally:
            if checkpoint_cb:
                checkpoint_cb(index, task)

        if job_control:
            reason = job_control.pending_reason()
            if reason:
                raise JobInterrupted(reason)


def run_archive(
    config: ArchiveConfig,
    tasks: List[VideoTask] | None = None,
    start_index: int = 1,
    job_control: JobControl | None = None,
    checkpoint_cb: Callable[[int, VideoTask], None] | None = None,
    channel_meta: dict | None = None,
):
    with ARCHIVE_LOCK:
        log_path = Path(config.log_file).expanduser()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        ytdlp_logger = _configure_logging(log_path, config.log_level)

        output_root = Path(config.out).expanduser()
        output_root.mkdir(parents=True, exist_ok=True)
        if channel_meta is not None:
            _apply_channel_meta(channel_meta)
        video_state.configure(output_root, channel_info)

        download_archive = None if config.no_cache else output_root / "downloaded.txt"
        if download_archive:
            download_archive.parent.mkdir(parents=True, exist_ok=True)

        if tasks is None:
            tasks = _queue_tasks(config)
        ydl_opts = _build_ydl_options(download_archive, config.subs, ytdlp_logger)
        _run_downloads(
            tasks,
            ydl_opts,
            config.clear_screen,
            start_index=start_index,
            checkpoint_cb=checkpoint_cb,
            job_control=job_control,
        )
