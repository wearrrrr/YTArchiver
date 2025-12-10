import argparse
import logging
import sys

from .constants import ASCII_ART, LOG_LEVEL_CHOICES
from .service import ArchiveConfig, run_archive

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


def main():
    parser = build_parser()
    args = parser.parse_args()

    config = ArchiveConfig(
        command=args.command,
        handle=getattr(args, "handle", None),
        video_ids=getattr(args, "video_ids", []) or [],
        out=args.out,
        subs=args.subs,
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
