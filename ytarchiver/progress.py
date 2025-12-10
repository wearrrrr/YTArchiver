import logging
import time
from threading import Lock
from typing import Callable, Optional

from yt_dlp.utils import DownloadCancelled

from .console import ENABLE_TTY, colorize, format_bytes, format_eta
from .context import video_state
from .helpers import short_name
from .state import ProgressState

CYAN = "\033[96m"
LOG = logging.getLogger("ytarchiver.progress")

progress_state = ProgressState()
_progress_sinks: list[Callable[[dict], None]] = []
_sinks_lock = Lock()
_interrupt_probe: Callable[[], str | None] | None = None


def register_progress_sink(callback: Callable[[dict], None]):
    with _sinks_lock:
        _progress_sinks.append(callback)


def unregister_progress_sink(callback: Callable[[dict], None]):
    with _sinks_lock:
        if callback in _progress_sinks:
            _progress_sinks.remove(callback)


def bind_interrupt_probe(callback: Callable[[], str | None] | None):
    LOG.info("Binding interrupt probe")
    global _interrupt_probe
    _interrupt_probe = callback


def _broadcast_progress(payload: dict):
    with _sinks_lock:
        sinks = list(_progress_sinks)
    for sink in sinks:
        try:
            sink(payload)
        except Exception as exc:
            LOG.debug("Progress sink failed: %s", exc)


def reset_progress_state(label: str = "Queued", detail: str = ""):
    progress_state.label = label
    progress_state.detail = detail
    progress_state.downloaded_bytes = 0.0
    progress_state.total_bytes = None
    progress_state.speed = None
    progress_state.eta = None
    progress_state.show_transfer = False
    progress_state.last_percent = None
    progress_state.last_emit = 0.0
    progress_state.last_render_len = 0
    progress_state.inline_active = False
    progress_state.batch_index = 0
    progress_state.batch_total = 0


def _emit_progress_line(force: bool = False, final: bool = False):
    now = time.time()
    percent = None
    if progress_state.show_transfer and progress_state.total_bytes:
        try:
            percent = int((progress_state.downloaded_bytes / progress_state.total_bytes) * 100)
        except ZeroDivisionError:
            percent = None

    if not force:
        if percent is not None and progress_state.last_percent == percent and (now - progress_state.last_emit) < 0.75:
            return
        if percent is None and (now - progress_state.last_emit) < 1.5:
            return

    progress_state.last_emit = now
    if percent is not None:
        progress_state.last_percent = percent

    label = progress_state.label or "Status"
    label_display = f"{label}..." if progress_state.show_transfer else label
    parts: list[str] = []
    if progress_state.batch_total:
        idx = max(1, progress_state.batch_index or 1)
        parts.append(f"[{idx}/{progress_state.batch_total}]")
    parts.append(colorize(f"[{label_display}]", CYAN))

    if progress_state.show_transfer:
        percent_str = f"{percent:3d}%" if percent is not None else "--%"
        downloaded = format_bytes(progress_state.downloaded_bytes)
        total = format_bytes(progress_state.total_bytes)
        parts.append(percent_str)
        parts.append(f"({downloaded} / {total})")
        if progress_state.speed:
            parts.append(f"at {format_bytes(progress_state.speed)}/s")
        if progress_state.eta is not None:
            parts.append(f"ETA {format_eta(progress_state.eta)}")

    if progress_state.detail:
        parts.append(f"— {progress_state.detail}")

    line = " ".join(parts)
    payload = {
        "label": progress_state.label,
        "detail": progress_state.detail,
        "percent": percent,
        "downloaded": progress_state.downloaded_bytes,
        "total": progress_state.total_bytes,
        "speed": progress_state.speed,
        "eta": progress_state.eta,
        "show_transfer": progress_state.show_transfer,
        "batch_index": progress_state.batch_index,
        "batch_total": progress_state.batch_total,
        "timestamp": now,
    }

    if ENABLE_TTY and progress_state.show_transfer:
        padding = max(0, progress_state.last_render_len - len(line))
        print("\r" + line + " " * padding, end="", flush=True)
        progress_state.last_render_len = len(line)
        progress_state.inline_active = True
        if final:
            print()
            progress_state.inline_active = False
            progress_state.last_render_len = 0
    else:
        if progress_state.inline_active:
            print()
            progress_state.inline_active = False
            progress_state.last_render_len = 0
        print(line)

    _broadcast_progress(payload)


def set_stage(label: str, detail: str = "", show_transfer: Optional[bool] = None, force: bool = True):
    if show_transfer is not None:
        progress_state.show_transfer = show_transfer

    updated = False
    if progress_state.label != label:
        progress_state.label = label
        video_state.current_stage = label
        updated = True
    if progress_state.detail != detail:
        progress_state.detail = detail
        video_state.stage_detail = detail
        updated = True

    if updated or force:
        _emit_progress_line(force=True)


def progress_hook(status: dict):
    if _interrupt_probe:
        try:
            reason = _interrupt_probe()
            LOG.debug(reason)
        except Exception as exc:  # pragma: no cover - diagnostic path
            LOG.debug("Interrupt probe callable failed: %s", exc)
            reason = None
        if reason:
            LOG.info("Cancelling download via interrupt probe (%s)", reason)
            raise DownloadCancelled(reason)
    state = status.get("status")
    filename = short_name(status.get("filename"))
    if state == "downloading":
        progress_state.show_transfer = True
        progress_state.label = "Downloading"
        progress_state.detail = filename or progress_state.detail
        progress_state.downloaded_bytes = status.get("downloaded_bytes") or 0.0
        progress_state.total_bytes = status.get("total_bytes") or status.get("total_bytes_estimate")
        progress_state.speed = status.get("speed")
        progress_state.eta = status.get("eta")
        _emit_progress_line()
    elif state == "finished":
        progress_state.downloaded_bytes = progress_state.total_bytes or progress_state.downloaded_bytes
        progress_state.speed = None
        progress_state.eta = None
        _emit_progress_line(force=True, final=True)
        progress_state.show_transfer = False
        progress_state.label = "Processing"
        progress_state.detail = "Download complete, finalizing media"
        _emit_progress_line(force=True)
