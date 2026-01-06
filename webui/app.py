from __future__ import annotations

import json
import re
import threading
from collections import deque
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, redirect, render_template, request, url_for
from flask_sock import Sock
from simple_websocket import ConnectionClosed, Server

from ytarchiver.service import ArchiveConfig
from ytarchiver.watcher import WatchDaemon
from ytarchiver.watchlist import WatchlistStore

from .job_manager import JobManager

app = Flask(__name__)
sock = Sock(app)

job_manager = JobManager(Path("logs/webui-jobs.json"))
watchlist_store = WatchlistStore(Path("logs/watchlist.db"))
watch_daemon = WatchDaemon(job_manager, watchlist_store, poll_interval=300, batch_size=5)
_ws_clients: set[Server] = set()
_ws_clients_lock = threading.Lock()
_log_subscribers: dict[str, set[Server]] = {}
_log_subscribers_lock = threading.Lock()
_ws_log_targets: dict[Server, str] = {}


def _read_log_tail(log_file: str | None, max_lines: int = 200) -> list[str]:
    max_lines = max(1, min(max_lines, 1000))
    if not log_file:
        return ["Log file not specified."]
    path = Path(log_file)
    if not path.exists():
        return ["Log file not created yet."]
    buffer: deque[str] = deque(maxlen=max_lines)
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                buffer.append(line.rstrip("\n"))
    except OSError:
        return ["Unable to read log file."]
    return list(buffer)


def _send_ws_message(ws: Server, payload: dict):
    try:
        ws.send(json.dumps(payload))
    except ConnectionClosed:
        _detach_ws(ws)
        raise


def _broadcast(payload: dict):
    message = json.dumps(payload)
    stale: list[Server] = []
    with _ws_clients_lock:
        targets = list(_ws_clients)
    for ws in targets:
        try:
            ws.send(message)
        except ConnectionClosed:
            stale.append(ws)
    for ws in stale:
        _detach_ws(ws)


def _unsubscribe_log(ws: Server):
    with _log_subscribers_lock:
        job_id = _ws_log_targets.pop(ws, None)
        if not job_id:
            return
        watchers = _log_subscribers.get(job_id)
        if not watchers:
            return
        watchers.discard(ws)
        if not watchers:
            _log_subscribers.pop(job_id, None)


def _clear_log_subscribers(job_id: str):
    with _log_subscribers_lock:
        watchers = _log_subscribers.pop(job_id, set())
        for ws in list(watchers):
            if _ws_log_targets.get(ws) == job_id:
                _ws_log_targets.pop(ws, None)


def _subscribe_log(ws: Server, job_id: str):
    with _log_subscribers_lock:
        previous = _ws_log_targets.get(ws)
        if previous and previous != job_id:
            watchers = _log_subscribers.get(previous)
            if watchers:
                watchers.discard(ws)
                if not watchers:
                    _log_subscribers.pop(previous, None)
        _ws_log_targets[ws] = job_id
        watchers = _log_subscribers.setdefault(job_id, set())
        watchers.add(ws)


def _detach_ws(ws: Server):
    with _ws_clients_lock:
        _ws_clients.discard(ws)
    _unsubscribe_log(ws)


def _send_jobs_snapshot(ws: Server):
    snapshot = job_manager.list_jobs()
    _send_ws_message(ws, {"type": "jobs_snapshot", "jobs": snapshot})


def _broadcast_job_log(job: dict, max_lines: int = 200):
    job_id = job.get("id")
    if not job_id:
        return
    with _log_subscribers_lock:
        targets = list(_log_subscribers.get(job_id, ()))
    if not targets:
        return
    tail = _read_log_tail(job.get("log_file"), max_lines)
    payload = {
        "type": "job_log",
        "job_id": job_id,
        "status": job.get("status"),
        "progress": job.get("progress"),
        "tail": tail,
        "tail_text": "\n".join(tail),
    }
    message = json.dumps(payload)
    stale: list[Server] = []
    for ws in targets:
        try:
            ws.send(message)
        except ConnectionClosed:
            stale.append(ws)
    for ws in stale:
        _detach_ws(ws)


def _job_event_listener(event: dict):
    kind = event.get("event")
    if kind == "job_update":
        job_payload = event.get("job")
        if not job_payload:
            return
        _broadcast({"type": "job_update", "job": job_payload})
        _broadcast_job_log(job_payload)
    elif kind == "job_deleted":
        job_id = event.get("job_id")
        if job_id:
            _clear_log_subscribers(job_id)
            _broadcast({"type": "job_deleted", "job_id": job_id})


job_manager.register_listener(_job_event_listener)

daemon_thread = threading.Thread(target=watch_daemon.run_forever, daemon=True)
daemon_thread.start()


def _parse_video_ids(raw: str) -> list[str]:
    parts = re.split(r"[\s,]+", raw.strip()) if raw else []
    return [part for part in parts if part]


def _coerce_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip()
    return str(value).strip() or default


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "on", "yes"}
    return bool(value)


def _coerce_video_field(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return "\n".join(str(item) for item in value if item)
    return str(value)


def _prepare_job_submission(data: Mapping[str, Any] | dict[str, Any] | None):
    if data is None:
        return None, None, "Missing submission payload."

    command = _coerce_str(data.get("command")).lower()
    if command not in {"channel", "shorts", "video"}:
        return None, None, "Invalid command selected."

    handle = _coerce_str(data.get("handle")) or None
    raw_video_ids = _coerce_video_field(data.get("video_ids"))
    video_ids = _parse_video_ids(raw_video_ids)

    if command in {"channel", "shorts"} and not handle:
        return None, None, "A channel handle is required for channel/shorts modes."
    if command == "video" and not video_ids:
        return None, None, "Provide at least one video ID or URL."

    out_dir = _coerce_str(data.get("out") or "yt") or "yt"
    log_file_input = _coerce_str(data.get("log_file"))
    log_level = _coerce_str(data.get("log_level") or "INFO") or "INFO"

    config = ArchiveConfig(
        command=command,
        handle=handle,
        video_ids=video_ids,
        out=out_dir,
        subs=_coerce_bool(data.get("subs")),
        no_cache=_coerce_bool(data.get("no_cache")),
        log_file=log_file_input or "logs/webui-placeholder.log",
        log_level=log_level.upper(),
        clear_screen=not _coerce_bool(data.get("no_clear")),
    )

    log_override = Path(log_file_input).expanduser() if log_file_input else None
    return config, log_override, None


def _handle_ws_message(ws: Server, data: dict):
    if not isinstance(data, dict):
        return

    message_type = data.get("type")
    if message_type == "request_log":
        job_id = data.get("job_id")
        if not job_id:
            _send_ws_message(ws, {"type": "job_log", "error": "Job ID required."})
            return
        max_lines = data.get("lines")
        try:
            lines = max(1, int(max_lines or 200))
        except (TypeError, ValueError):
            lines = 200
        try:
            log_payload = job_manager.log_payload(job_id, max_lines=lines)
        except ValueError as exc:
            _send_ws_message(ws, {"type": "job_log", "job_id": job_id, "error": str(exc)})
            return
        _subscribe_log(ws, job_id)
        log_payload["type"] = "job_log"
        _send_ws_message(ws, log_payload)
    elif message_type == "create_job":
        payload = data.get("payload")
        if not isinstance(payload, dict):
            payload = {}
        config, log_override, validation_error = _prepare_job_submission(payload)
        if validation_error:
            _send_ws_message(ws, {"type": "job_error", "error": validation_error})
            return
        try:
            job_id = job_manager.create_job(config, log_override)
        except Exception as exc:  # noqa: BLE001
            _send_ws_message(ws, {"type": "job_error", "error": str(exc)})
            return
        _send_ws_message(ws, {"type": "job_created", "job_id": job_id})
    elif message_type == "job_control":
        job_id = data.get("job_id")
        action = (data.get("action") or "").strip().lower()
        if not job_id or action not in {"pause", "stop", "resume", "delete"}:
            _send_ws_message(ws, {"type": "job_error", "job_id": job_id, "action": action, "error": "Invalid job control request."})
            return
        try:
            if action == "pause":
                job_manager.pause_job(job_id)
            elif action == "stop":
                job_manager.stop_job(job_id)
            elif action == "resume":
                job_manager.resume_job(job_id)
            elif action == "delete":
                job_manager.delete_job(job_id)
        except ValueError as exc:
            _send_ws_message(ws, {"type": "job_error", "job_id": job_id, "action": action, "error": str(exc)})
            return
        _send_ws_message(ws, {"type": "job_control_ack", "job_id": job_id, "action": action})


@app.route("/api/watchlist", methods=["GET"])
def get_watchlist():
    try:
        entries = watchlist_store.list_entries()
        return jsonify({"entries": [entry.as_dict() for entry in entries]})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/watchlist", methods=["POST"])
def add_watch_entry():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Missing JSON payload"}), 400
    
    try:
        entry_id = watchlist_store.add_entry(
            handle=data.get("handle", "").strip(),
            mode=data.get("mode", "channel"),
            interval_minutes=int(data.get("interval_minutes", 60)),
            subs=bool(data.get("subs", False)),
            no_cache=bool(data.get("no_cache", False)),
            out_dir=data.get("out_dir", "yt").strip() or "yt",
            log_level=data.get("log_level", "INFO").strip().upper() or "INFO",
            clear_screen=bool(data.get("clear_screen", True)),
            tags=data.get("tags", []),
        )
        return jsonify({"entry_id": entry_id}), 201
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/watchlist/<int:entry_id>", methods=["PUT"])
def update_watch_entry(entry_id: int):
    data = request.get_json()
    if not data:
        return jsonify({"error": "Missing JSON payload"}), 400
    
    try:
        watchlist_store.update_entry(
            entry_id=entry_id,
            handle=data.get("handle"),
            mode=data.get("mode"),
            interval_minutes=data.get("interval_minutes"),
            subs=data.get("subs"),
            no_cache=data.get("no_cache"),
            out_dir=data.get("out_dir"),
            log_level=data.get("log_level"),
            clear_screen=data.get("clear_screen"),
            tags=data.get("tags"),
        )
        return jsonify({"success": True})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/watchlist/<int:entry_id>", methods=["DELETE"])
def delete_watch_entry(entry_id: int):
    try:
        watchlist_store.delete_entry(entry_id)
        return jsonify({"success": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/", methods=["GET", "POST"])
def index():
    error = None
    if request.method == "POST":
        config, log_override, validation_error = _prepare_job_submission(request.form)
        if validation_error:
            error = validation_error
        else:
            try:
                job_manager.create_job(config, log_override)
                return redirect(url_for("index"))
            except Exception as exc:  # noqa: BLE001
                error = str(exc)

    jobs = job_manager.list_jobs()
    return render_template("index.html", jobs=jobs, error=error)


@app.route("/jobs/<job_id>.json")
def job_status(job_id: str):
    max_lines = request.args.get("lines", type=int) or 40
    payload = job_manager.serialize_job(job_id, include_log=True, max_lines=max_lines)
    if not payload:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(payload)


@sock.route("/ws")
def websocket_endpoint(ws: Server):  # pragma: no cover
    with _ws_clients_lock:
        _ws_clients.add(ws)

    try:
        _send_jobs_snapshot(ws)
        while True:
            message = ws.receive()
            if message is None:
                continue
            if isinstance(message, bytes):
                try:
                    message = message.decode("utf-8")
                except UnicodeDecodeError:
                    continue
            try:
                data = json.loads(message)
            except (TypeError, ValueError):
                continue
            _handle_ws_message(ws, data)
    except ConnectionClosed:
        pass
    finally:
        _detach_ws(ws)


if __name__ == "__main__":  # pragma: no cover
    app.run(debug=True, threaded=True)
