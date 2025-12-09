import os
import sys
from typing import Optional

from .state import VideoTask

ENABLE_TTY = sys.stdout.isatty()
RESET = "\033[0m"
BOLD = "\033[1m"
CYAN = "\033[96m"
MAGENTA = "\033[95m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
DIM = "\033[2m"


def colorize(text: str, color: str) -> str:
    if not ENABLE_TTY:
        return text
    return f"{color}{text}{RESET}"


def maybe_clear_console(enable_clear: bool):
    if enable_clear and ENABLE_TTY:
        os.system("cls" if os.name == "nt" else "clear")


def format_duration(duration: Optional[float]) -> str:
    if duration is None or duration < 0:
        return "Unknown"
    total_seconds = int(duration)
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:d}h {minutes:02d}m {seconds:02d}s"
    return f"{minutes:d}m {seconds:02d}s"


def format_bytes(value: Optional[float]) -> str:
    if value is None:
        return "Unknown"
    absolute = float(value)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    for unit in units:
        if absolute < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(absolute)} {unit}"
            return f"{absolute:.1f} {unit}"
        absolute /= 1024


def format_eta(seconds: Optional[int]) -> str:
    if seconds is None or seconds < 0:
        return "--:--"
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{sec:02d}"
    return f"{minutes:02d}:{sec:02d}"


def build_progress_bar(progress: float, width: int = 26) -> str:
    width = max(10, width)
    filled = min(width, int(width * progress))
    empty = width - filled
    return "█" * filled + "░" * empty


def render_task_banner(index: int, total: int, task: VideoTask, channel_label: str, clear_screen: bool):
    from .context import channel_info

    maybe_clear_console(clear_screen)
    ratio = index / total if total else 0
    bar = build_progress_bar(ratio)
    progress_tag = colorize(f"[{index}/{total}]", BOLD + CYAN)
    title = task.title or "Untitled Video"
    header = f"{progress_tag} {colorize(title, BOLD + GREEN)}"

    bar_line = colorize(bar, MAGENTA)
    meta_lines = [
        f"{colorize('Channel', YELLOW)}: {channel_label or channel_info.display_name or 'Unknown'}",
        f"{colorize('Title', YELLOW)}: {title}",
        f"{colorize('Video ID', YELLOW)}: {task.video_id}",
        f"{colorize('Duration', YELLOW)}: {format_duration(task.duration)}",
        f"{colorize('URL', YELLOW)}: {task.resolved_url()}",
    ]

    print(header)
    print(f"{bar_line} {int(ratio * 100):3d}%")
    for line in meta_lines:
        print(line)
    print(colorize("-" * 50, DIM))
