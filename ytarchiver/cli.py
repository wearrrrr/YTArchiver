import argparse
import logging
from pathlib import Path
from typing import List

from yt_dlp import YoutubeDL

from .console import render_task_banner
from .constants import ASCII_ART, LOG_LEVEL_CHOICES
from .context import channel_info, video_state
from .helpers import make_watch_url, normalize_video_ids
from .postprocess import on_postprocess, postprocess_subs
from .progress import progress_hook, reset_progress_state, set_stage
from .state import VideoTask
from .tasks import fetch_tasks_for_video_ids, fetch_video_listing

LOG = logging.getLogger("ytarchiver")


class YTArcArgumentParser(argparse.ArgumentParser):
    def print_help(self):  # pragma: no cover - formatting only
        print(ASCII_ART)
        super().print_help()


def build_parser() -> argparse.ArgumentParser:
    parser = YTArcArgumentParser(
        description="Archive channels, Shorts, or specific videos with yt-dlp.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--out", default="yt", help="Base output directory (default: %(default)s)")
    parser.add_argument("--subs", action="store_true", help="Download and convert subtitles (srv3 + ASS)")
    parser.add_argument("--no-cache", action="store_true", help="Disable the download archive cache")
    parser.add_argument(
        "--log-file",
        default="logs/ytarchiver.log",
        help="Path to store yt-dlp logs (default: %(default)s)",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Do not clear the console between downloads",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=LOG_LEVEL_CHOICES,
        help="Log level for file output (default: %(default)s)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    channel_parser = subparsers.add_parser("channel", help="Download every upload from a channel handle")
    channel_parser.add_argument("handle", help="YouTube channel handle (with or without @)")

    shorts_parser = subparsers.add_parser("shorts", help="Download the Shorts feed for a channel handle")
    shorts_parser.add_argument("handle", help="YouTube channel handle (with or without @)")

    video_parser = subparsers.add_parser("video", help="Download one or more individual videos")
    video_parser.add_argument("video_ids", nargs="+", help="Video IDs or URLs")

    return parser


def configure_logging(log_file: Path, level_name: str) -> logging.Logger:
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


def build_ydl_options(download_archive: Path | None, download_subs: bool, ytdlp_logger: logging.Logger) -> dict:
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


def queue_tasks(args) -> List[VideoTask]:
    if args.command in {"channel", "shorts"}:
        handle = args.handle.strip()
        if not handle.startswith("@"):
            handle = f"@{handle}"
        target_url = f"https://www.youtube.com/{handle}{'/shorts' if args.command == 'shorts' else ''}"
        LOG.info("Fetching %s list for %s", args.command, handle)
        info, tasks = fetch_video_listing(target_url)
        if not tasks:
            raise RuntimeError(f"No videos found for {target_url}")
        channel_info.display_name = str(info.get("channel") or info.get("uploader") or handle).strip()
        channel_info.subscribers = int(info.get("channel_follower_count") or 0)
        channel_info.description = str(info.get("description") or "")
        LOG.info("Queued %s video(s) for %s", len(tasks), channel_info.display_name or handle)
        return tasks

    normalized_ids = normalize_video_ids(args.video_ids)
    if not normalized_ids:
        raise RuntimeError("No valid video IDs or URLs provided.")
    tasks = fetch_tasks_for_video_ids(normalized_ids)
    LOG.info("Queued %s provided video(s).", len(tasks))
    return tasks


def run_downloads(tasks: List[VideoTask], ydl_opts: dict, clear_screen: bool):
    if not tasks:
        LOG.warning("No videos matched the provided criteria.")
        return

    for index, task in enumerate(tasks, start=1):
        video_state.clear()
        reset_progress_state(detail=f"Waiting on {task.video_id}")
        render_task_banner(index, len(tasks), task, task.uploader, clear_screen)
        video_url = task.resolved_url()
        LOG.info("[%s/%s] Downloading %s", index, len(tasks), video_url)
        try:
            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
            target_path = str(video_state.video_dir) if video_state.video_dir else video_url
            set_stage("Completed", f"Saved to {target_path}", show_transfer=False)
        except Exception as exc:  # noqa: BLE001
            set_stage("Error", str(exc), show_transfer=False)
            LOG.error("Failed to download %s (%s)", video_url, exc)


def main():
    parser = build_parser()
    args = parser.parse_args()

    log_file = Path(args.log_file).expanduser()
    log_file.parent.mkdir(parents=True, exist_ok=True)
    ytdlp_logger = configure_logging(log_file, args.log_level)
    LOG.info("yt-dlp logs will be stored in %s", log_file)

    output_root = Path(args.out).expanduser()
    output_root.mkdir(parents=True, exist_ok=True)
    video_state.configure(output_root, channel_info)

    download_archive = None if args.no_cache else output_root / "downloaded.txt"
    if download_archive:
        download_archive.parent.mkdir(parents=True, exist_ok=True)

    try:
        tasks = queue_tasks(args)
    except RuntimeError as exc:
        LOG.error(str(exc))
        return

    ydl_opts = build_ydl_options(download_archive, args.subs, ytdlp_logger)
    run_downloads(tasks, ydl_opts, clear_screen=not args.no_clear)


if __name__ == "__main__":  # pragma: no cover
    main()
