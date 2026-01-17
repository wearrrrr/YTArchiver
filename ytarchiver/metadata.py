import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

LOG = logging.getLogger("ytarchiver")


class MetadataStore:
    """SQLite database for storing video metadata per channel."""

    def __init__(self, channel_dir: Path):
        self.db_path = channel_dir / "metadata.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS videos (
                    video_id TEXT PRIMARY KEY,
                    title TEXT,
                    uploader TEXT,
                    upload_date TEXT,
                    duration INTEGER,
                    view_count INTEGER,
                    like_count INTEGER,
                    comment_count INTEGER,
                    description TEXT,
                    categories TEXT,
                    tags TEXT,
                    live_status TEXT,
                    media_type TEXT,
                    width INTEGER,
                    height INTEGER,
                    fps REAL,
                    video_codec TEXT,
                    audio_codec TEXT,
                    filesize INTEGER,
                    download_date TEXT,
                    video_path TEXT,
                    thumbnail_path TEXT,
                    subtitles_available TEXT,
                    full_metadata TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_upload_date ON videos(upload_date)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_download_date ON videos(download_date)"
            )
            conn.commit()

    def save_video_metadata(
        self,
        info: dict,
        video_path: Path | str,
        thumbnail_path: Optional[Path | str] = None,
    ) -> None:
        """Persist the metadata captured from a yt-dlp info dict."""
        try:
            video_id = info.get("id", "")
            title = info.get("title", "")
            uploader = info.get("uploader") or info.get("channel", "")
            upload_date = info.get("upload_date", "")
            duration = info.get("duration")
            view_count = info.get("view_count")
            like_count = info.get("like_count")
            comment_count = info.get("comment_count")
            description = info.get("description", "")

            categories = json.dumps(info.get("categories", []))
            tags = json.dumps(info.get("tags", []))

            live_status = info.get("live_status", "")
            media_type = info.get("media_type", "")
            width = info.get("width")
            height = info.get("height")
            fps = info.get("fps")

            video_codec = info.get("vcodec", "")
            audio_codec = info.get("acodec", "")
            filesize = info.get("filesize") or info.get("filesize_approx")

            download_date = datetime.utcnow().isoformat()

            subtitles = info.get("subtitles", {})
            auto_subs = info.get("automatic_captions", {})
            all_sub_langs = list(set(list(subtitles.keys()) + list(auto_subs.keys())))
            subtitles_available = json.dumps(sorted(all_sub_langs))

            full_info = dict(info)
            for key in [
                "formats",
                "thumbnails",
                "subtitles",
                "automatic_captions",
                "requested_formats",
            ]:
                full_info.pop(key, None)
            full_metadata = json.dumps(full_info, default=str)

            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO videos (
                        video_id, title, uploader, upload_date, duration,
                        view_count, like_count, comment_count, description,
                        categories, tags, live_status, media_type,
                        width, height, fps, video_codec, audio_codec, filesize,
                        download_date, video_path, thumbnail_path, subtitles_available,
                        full_metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        video_id,
                        title,
                        uploader,
                        upload_date,
                        duration,
                        view_count,
                        like_count,
                        comment_count,
                        description,
                        categories,
                        tags,
                        live_status,
                        media_type,
                        width,
                        height,
                        fps,
                        video_codec,
                        audio_codec,
                        filesize,
                        download_date,
                        str(video_path),
                        str(thumbnail_path) if thumbnail_path else None,
                        subtitles_available,
                        full_metadata,
                    ),
                )
                conn.commit()

            LOG.info("Saved metadata for video %s to database", video_id)

        except Exception as exc:  # noqa: BLE001
            LOG.error(
                "Failed to save video metadata for %s: %s",
                info.get("id", "unknown"),
                exc,
            )

    def get_video_metadata(self, video_id: str) -> Optional[dict]:
        """Retrieve metadata for a single video."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM videos WHERE video_id = ?", (video_id,)
            ).fetchone()

        if not row:
            return None
        return dict(row)

    def list_videos(
        self, limit: Optional[int] = None, offset: int = 0
    ) -> list[dict]:
        """List videos ordered by newest upload date."""
        query = "SELECT * FROM videos ORDER BY upload_date DESC"
        params: list[Any] = []

        if limit:
            query += " LIMIT ? OFFSET ?"
            params = [limit, offset]

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()

        return [dict(row) for row in rows]

    def get_stats(self) -> dict:
        """Aggregate statistics about the archived collection."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 
                    COUNT(*) as total_videos,
                    SUM(view_count) as total_views,
                    SUM(duration) as total_duration,
                    SUM(filesize) as total_size,
                    MIN(upload_date) as earliest_upload,
                    MAX(upload_date) as latest_upload
                FROM videos
                """
            ).fetchone()

        return dict(row) if row else {}