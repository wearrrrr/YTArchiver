import logging
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from .context import channel_info, video_state
from .helpers import sanitize
from .progress import set_stage
from .metadata import MetadataStore

LOG = logging.getLogger("ytarchiver")


def categorize(info: dict) -> str:
    live_status = info.get("live_status")
    media_type = (info.get("media_type") or "").lower()
    if "short" in media_type:
        return "shorts"
    if live_status == "was_live":
        return "vods"
    return "videos"


def on_postprocess(info: dict):
    if info.get("status") != "finished" or info.get("postprocessor") != "MoveFiles":
        return

    data = info.get("info_dict", {})
    if not data:
        return

    set_stage("Transcoding", "Finalizing media container", show_transfer=False)

    video_state.tmp_file = Path(data["filepath"]).resolve()
    video_state.tmp_dir = video_state.tmp_file.parent
    folder = categorize(data)

    if video_state.filter_videos_only and folder in ("shorts", "vods"):
        LOG.info("Skipping %s (filter_videos_only enabled): %s", folder, data.get("id", ""))
        try:
            if video_state.tmp_file and video_state.tmp_file.exists():
                video_state.tmp_file.unlink()
                LOG.debug("Deleted filtered file: %s", video_state.tmp_file)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Failed to delete filtered file %s (%s)", video_state.tmp_file, exc)
        return

    raw_date = data.get("upload_date")
    date = datetime.strptime(raw_date, "%Y%m%d").strftime("%m-%d-%y") if raw_date else "unknown-date"

    title = sanitize(data.get("title") or "unknown-title")
    video_state.vid = data.get("id", "")
    video_state.ext = video_state.tmp_file.suffix.lstrip(".")

    channel_name = sanitize(
        data.get("channel")
        or data.get("uploader")
        or video_state.channel_info.display_name
        or "unknown-channel"
    )

    video_dir_name = f"{date} - {title} [{video_state.vid}]"
    video_dir = video_state.output_root / channel_name / folder / video_dir_name
    video_dir.mkdir(parents=True, exist_ok=True)
    video_state.video_dir = video_dir

    video_filename = f"{title}.{video_state.ext}" if video_state.ext else title
    new_video_path = video_dir / video_filename

    try:
        shutil.move(str(video_state.tmp_file), str(new_video_path))
        LOG.info("Saved video -> %s", new_video_path)
    except Exception as exc:  # noqa: BLE001
        LOG.error("Failed to move video %s -> %s (%s)", video_state.tmp_file, new_video_path, exc)

    # Save metadata to database
    try:
        channel_dir = video_state.output_root / channel_name
        metadata_store = MetadataStore(channel_dir)

        # Find and copy thumbnail file (embedded thumbnail remains in video, but we also keep a copy)
        thumbnail_path = None
        if video_state.tmp_dir and video_state.vid:
            for ext in ['.jpg', '.png', '.webp']:
                thumb = video_state.tmp_dir / f"{video_state.vid}{ext}"
                if thumb.exists():
                    # Copy thumbnail to video directory (don't move, since it's embedded in mkv)
                    new_thumb_path = video_dir / f"thumbnail{ext}"
                    try:
                        shutil.copy2(str(thumb), str(new_thumb_path))
                        thumbnail_path = str(new_thumb_path)
                        LOG.info("Saved thumbnail -> %s", new_thumb_path)
                        thumb.unlink()
                    except Exception as thumb_exc:  # noqa: BLE001
                        LOG.warning("Failed to copy thumbnail %s -> %s (%s)", thumb, new_thumb_path, thumb_exc)
                    break

        metadata_store.save_video_metadata(data, str(new_video_path), thumbnail_path)
        LOG.info("Saved metadata for %s to database", video_state.vid)
    except Exception as exc:  # noqa: BLE001
        LOG.error("Failed to save metadata for %s (%s)", video_state.vid, exc)


    if video_state.tmp_dir and video_state.vid:
        info_json_file = video_state.tmp_dir / f"{video_state.vid}.info.json"
        if info_json_file.exists():
            try:
                info_json_file.unlink()
                LOG.info("Removed info.json file -> %s", info_json_file)
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Failed to remove info.json %s (%s)", info_json_file, exc)

    if video_state.tmp_dir and video_state.vid:
        live_chat_file = video_state.tmp_dir / f"{video_state.vid}.live_chat.json"
        if live_chat_file.exists():
            live_chat_path = video_dir / "live_chat.json"
            try:
                shutil.move(str(live_chat_file), str(live_chat_path))
                LOG.info("Saved live chat -> %s", live_chat_path)
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Failed to move live chat %s -> %s (%s)", live_chat_file, live_chat_path, exc)


def postprocess_subs(_info: dict):
    if not video_state.tmp_dir or not video_state.vid or not video_state.video_dir:
        return

    set_stage("Subtitles", "Organizing subtitle tracks", show_transfer=False)
    subtitle_files = list(video_state.tmp_dir.glob(f"{video_state.vid}.*.srv3"))

    if not subtitle_files:
        set_stage("Subtitles", "No subtitle tracks found", show_transfer=False)
        return

    for sub_path in subtitle_files:
        filename = sub_path.name
        parts = filename.split(".")
        if len(parts) < 3:
            LOG.debug("Skipping malformed subtitle filename: %s", filename)
            continue
        lang = parts[-2]
        set_stage("Subtitles", f"Processing {lang}", show_transfer=False)

        ass_tmp_path = video_state.tmp_dir / f"{video_state.vid}.{lang}.ass"
        try:
            subprocess.run(["ytsubconverter", str(sub_path), str(ass_tmp_path)], check=True)  # noqa: S603
            LOG.info("Converted %s -> %s", sub_path, ass_tmp_path)
        except subprocess.CalledProcessError as exc:
            LOG.warning("ytsubconverter failed for %s (%s)", sub_path, exc)
            ass_tmp_path = None
        except FileNotFoundError:
            LOG.warning("ytsubconverter not found; skipping conversion.")
            ass_tmp_path = None

        subs_dir = video_state.video_dir / "subtitles"
        subs_dir.mkdir(parents=True, exist_ok=True)

        new_srv3_path = subs_dir / f"{lang}.srv3"
        try:
            shutil.move(str(sub_path), str(new_srv3_path))
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Failed to move %s -> %s (%s)", sub_path, new_srv3_path, exc)

        if ass_tmp_path and ass_tmp_path.exists():
            new_ass_path = subs_dir / f"{lang}.ass"
            try:
                shutil.move(str(ass_tmp_path), str(new_ass_path))
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Failed to move %s -> %s (%s)", ass_tmp_path, new_ass_path, exc)

    set_stage("Subtitles", "Completed", show_transfer=False)
