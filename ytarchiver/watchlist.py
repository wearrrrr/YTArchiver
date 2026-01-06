from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator, Sequence


@dataclass(slots=True)
class WatchEntry:
    """
    Represents a poll target for the watch daemon.
    """

    id: int | None = None
    handle: str = ""
    mode: str = "channel"  # channel | shorts
    interval_minutes: int = 60
    last_check_ts: float | None = None
    last_enqueued_ts: float | None = None
    subs: bool = False
    no_cache: bool = False
    out_dir: str = "yt"
    log_level: str = "INFO"
    clear_screen: bool = True
    tags: tuple[str, ...] = field(default_factory=tuple)

    def normalized_handle(self) -> str:
        handle = self.handle.strip()
        if not handle.startswith("@"):
            handle = f"@{handle}"
        return handle

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "handle": self.handle,
            "mode": self.mode,
            "interval_minutes": self.interval_minutes,
            "last_check_ts": self.last_check_ts,
            "last_enqueued_ts": self.last_enqueued_ts,
            "subs": self.subs,
            "no_cache": self.no_cache,
            "out_dir": self.out_dir,
            "log_level": self.log_level,
            "clear_screen": self.clear_screen,
            "tags": list(self.tags),
        }


class WatchlistStore:
    """
    SQLite-backed storage for watch entries and their state.
    """

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS watchlist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    handle TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('channel','shorts')),
                    interval_minutes INTEGER NOT NULL CHECK(interval_minutes > 0),
                    last_check_ts REAL,
                    last_enqueued_ts REAL,
                    subs INTEGER NOT NULL DEFAULT 0,
                    no_cache INTEGER NOT NULL DEFAULT 0,
                    out_dir TEXT NOT NULL DEFAULT 'yt',
                    log_level TEXT NOT NULL DEFAULT 'INFO',
                    clear_screen INTEGER NOT NULL DEFAULT 1,
                    tags TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_watchlist_next ON watchlist(handle)"
            )
            conn.commit()

    # --------------------------------------------------------------------- #
    # row conversion
    # --------------------------------------------------------------------- #
    def _row_to_entry(self, row: sqlite3.Row) -> WatchEntry:
        tags: tuple[str, ...] = tuple(
            filter(None, (row["tags"] or "").split(","))
        )
        return WatchEntry(
            id=row["id"],
            handle=row["handle"],
            mode=row["mode"],
            interval_minutes=row["interval_minutes"],
            last_check_ts=row["last_check_ts"],
            last_enqueued_ts=row["last_enqueued_ts"],
            subs=bool(row["subs"]),
            no_cache=bool(row["no_cache"]),
            out_dir=row["out_dir"],
            log_level=row["log_level"],
            clear_screen=bool(row["clear_screen"]),
            tags=tags,
        )

    # --------------------------------------------------------------------- #
    # CRUD
    # --------------------------------------------------------------------- #
    def list_entries(self) -> list[WatchEntry]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM watchlist ORDER BY LOWER(handle), id"
            ).fetchall()
        return [self._row_to_entry(row) for row in rows]

    def get_entry(self, entry_id: int) -> WatchEntry | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM watchlist WHERE id = ?", (entry_id,)
            ).fetchone()
        return self._row_to_entry(row) if row else None

    def add_entry(
        self,
        *,
        handle: str,
        mode: str = "channel",
        interval_minutes: int = 60,
        subs: bool = False,
        no_cache: bool = False,
        out_dir: str = "yt",
        log_level: str = "INFO",
        clear_screen: bool = True,
        tags: Sequence[str] | None = None,
    ) -> int:
        handle = handle.strip()
        if not handle:
            raise ValueError("Handle is required.")
        if mode not in {"channel", "shorts"}:
            raise ValueError("Mode must be 'channel' or 'shorts'.")
        if interval_minutes <= 0:
            raise ValueError("Interval must be positive.")
        tag_str = ",".join(sorted({tag.strip() for tag in (tags or []) if tag}))
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO watchlist (
                    handle, mode, interval_minutes, subs, no_cache,
                    out_dir, log_level, clear_screen, tags
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    handle,
                    mode,
                    interval_minutes,
                    int(subs),
                    int(no_cache),
                    out_dir.strip() or "yt",
                    log_level.strip().upper() or "INFO",
                    int(clear_screen),
                    tag_str,
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def update_entry(
        self,
        entry_id: int,
        *,
        handle: str | None = None,
        mode: str | None = None,
        interval_minutes: int | None = None,
        subs: bool | None = None,
        no_cache: bool | None = None,
        out_dir: str | None = None,
        log_level: str | None = None,
        clear_screen: bool | None = None,
        tags: Sequence[str] | None = None,
        last_check_ts: float | None = None,
        last_enqueued_ts: float | None = None,
    ):
        fields: list[str] = []
        params: list[object] = []
        if handle is not None:
            name = handle.strip()
            if not name:
                raise ValueError("Handle cannot be empty.")
            fields.append("handle = ?")
            params.append(name)
        if mode is not None:
            if mode not in {"channel", "shorts"}:
                raise ValueError("Mode must be 'channel' or 'shorts'.")
            fields.append("mode = ?")
            params.append(mode)
        if interval_minutes is not None:
            if interval_minutes <= 0:
                raise ValueError("Interval must be positive.")
            fields.append("interval_minutes = ?")
            params.append(interval_minutes)
        if subs is not None:
            fields.append("subs = ?")
            params.append(int(subs))
        if no_cache is not None:
            fields.append("no_cache = ?")
            params.append(int(no_cache))
        if out_dir is not None:
            fields.append("out_dir = ?")
            params.append(out_dir.strip() or "yt")
        if log_level is not None:
            fields.append("log_level = ?")
            params.append(log_level.strip().upper() or "INFO")
        if clear_screen is not None:
            fields.append("clear_screen = ?")
            params.append(int(clear_screen))
        if tags is not None:
            tag_str = ",".join(
                sorted({tag.strip() for tag in tags if tag.strip()})
            )
            fields.append("tags = ?")
            params.append(tag_str)
        if last_check_ts is not None:
            fields.append("last_check_ts = ?")
            params.append(last_check_ts)
        if last_enqueued_ts is not None:
            fields.append("last_enqueued_ts = ?")
            params.append(last_enqueued_ts)
        if not fields:
            return
        params.append(entry_id)
        with self._connect() as conn:
            conn.execute(
                f"UPDATE watchlist SET {', '.join(fields)} WHERE id = ?",
                params,
            )
            conn.commit()

    def delete_entry(self, entry_id: int):
        with self._connect() as conn:
            conn.execute("DELETE FROM watchlist WHERE id = ?", (entry_id,))
            conn.commit()

    def iter_due_entries(self, now_ts: float | None = None) -> Iterator[WatchEntry]:
        """
        Yield entries that should be checked at the provided timestamp.
        """
        if now_ts is None:
            now_ts = time.time()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM watchlist
                WHERE last_check_ts IS NULL
                   OR (? - last_check_ts) >= (interval_minutes * 60)
                ORDER BY COALESCE(last_check_ts, 0) ASC, id ASC
                """,
                (now_ts,),
            ).fetchall()
        for row in rows:
            yield self._row_to_entry(row)

    def bulk_touch(self, updates: Iterable[tuple[int, float]]):
        """
        Update last_check_ts for a batch of entries.
        """
        payload = [(ts, entry_id) for entry_id, ts in updates]
        if not payload:
            return
        with self._connect() as conn:
            conn.executemany(
                "UPDATE watchlist SET last_check_ts = ? WHERE id = ?",
                payload,
            )
            conn.commit()

    def mark_enqueued(self, updates: Iterable[tuple[int, float]]):
        """
        Record the time a watch entry last produced a job.
        """
        payload = [(ts, entry_id) for entry_id, ts in updates]
        if not payload:
            return
        with self._connect() as conn:
            conn.executemany(
                "UPDATE watchlist SET last_enqueued_ts = ? WHERE id = ?",
                payload,
            )
            conn.commit()