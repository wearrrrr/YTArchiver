import argparse
import logging
import sys
from pathlib import Path

from .constants import ASCII_ART, LOG_LEVEL_CHOICES
from .service import ArchiveConfig, run_archive
from .watcher import WatchDaemon
from .watchlist import WatchlistStore

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

    watch_parser = subparsers.add_parser("watch", help="Run the background daemon to monitor channels")
    watch_parser.add_argument("--poll-interval", type=int, default=300, help="Seconds between watchlist checks (default: %(default)s)")
    watch_parser.add_argument("--batch-size", type=int, default=5, help="Max channels to check per cycle (default: %(default)s)")
    watch_parser.add_argument("--db-path", default="logs/watchlist.db", help="Path to watchlist database (default: %(default)s)")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "watch":
        from ytarchiver.webui.job_manager import JobManager
        job_manager = JobManager(Path("logs/webui-jobs.json"))
        watchlist = WatchlistStore(Path(args.db_path))
        daemon = WatchDaemon(
            job_manager=job_manager,
            watchlist=watchlist,
            poll_interval=args.poll_interval,
            batch_size=args.batch_size,
        )
        try:
            daemon.run_forever()
        except KeyboardInterrupt:
            LOG.info("Watch daemon stopped by user")
        return

    config = ArchiveConfig(
        command=args.command,
        handle=getattr(args, "handle", None),
        video_ids=getattr(args, "video_ids", []) or [],
        out=args.out,
        no_cache=args.no_cache,
        log_file=args.log_file,
        log_level=args.log_level,
        clear_screen=not args.no_clear,
    )

    try:
        run_archive(config)
    except RuntimeError as exc:
        LOG.error(str(exc))
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
