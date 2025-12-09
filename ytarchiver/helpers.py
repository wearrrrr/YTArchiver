from pathlib import Path
from urllib.parse import parse_qs, urlparse
import re


def sanitize(name: str) -> str:
    cleaned = re.sub(r"[\\/*:\"<>|]", "_", name or "")
    return re.sub(r"\?", "", cleaned)


def make_watch_url(video_id: str) -> str:
    return video_id if video_id.startswith("http") else f"https://www.youtube.com/watch?v={video_id}"


def normalize_video_id(raw: str) -> str:
    candidate = raw.strip()
    if not candidate:
        return ""

    if candidate.startswith("http"):
        parsed = urlparse(candidate)
        netloc = parsed.netloc.lower()
        if "youtu.be" in netloc:
            return parsed.path.lstrip("/")
        if "youtube.com" in netloc:
            query = parse_qs(parsed.query)
            if "v" in query:
                return query["v"][0]
            segments = [segment for segment in parsed.path.split("/") if segment]
            if segments:
                return segments[-1]
        return candidate

    if "watch?v=" in candidate:
        return candidate.split("watch?v=")[-1].split("&")[0]

    if len(candidate) == 11:
        return candidate

    return candidate


def normalize_video_ids(values):
    seen = set()
    normalized = []
    for value in values:
        vid = normalize_video_id(value)
        if not vid or vid in seen:
            continue
        seen.add(vid)
        normalized.append(vid)
    return normalized

def short_name(path_str: str | None) -> str:
    if not path_str:
        return ""
    return Path(path_str).name
