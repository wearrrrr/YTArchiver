import glob
import shutil
import subprocess
import os
import re
import pprint
import argparse

from datetime import datetime
from yt_dlp import YoutubeDL

ASCII_ART = r"""
__  ___________              __    _
\ \/ /_  __/   |  __________/ /_  (_)   _____  _____
 \  / / / / /| | / ___/ ___/ __ \/ / | / / _ \/ ___/
 / / / / / ___ |/ /  / /__/ / / / /| |/ /  __/ /
/_/ /_/ /_/  |_/_/   \___/_/ /_/_/ |___/\___/_/
"""

class YTArcArgumentParser(argparse.ArgumentParser):
    def print_help(self):
        print(ASCII_ART)
        super().print_help()


def sanitize(name):
    pass1 = re.sub(r'[\\/*:"<>|]', "_", name)
    # replace ? with nothing
    pass2 = re.sub(r'\?', '', pass1)
    return pass2

class ChannelInfo:
    display_name: str = ""
    subscribers: int = 0
    description: str = ""

class CurrentVideoState:
    tmp_file = ""
    tmp_dir = ""
    folder = ""
    vid = ""
    video_dir = ""
    ext = ""

    def clear(self):
        self.tmp_file = ""
        self.tmp_dir = ""
        self.folder = ""
        self.vid = ""
        self.video_dir = ""
        self.ext = ""

def categorize(info):
    live_status = info.get("live_status")
    is_short = "short" in info.get("media_type")

    if is_short:
        return "shorts"
    if live_status == "was_live":
        return "vods"
    return "videos"


vid_state = CurrentVideoState()
def on_postprocess(info):
    """Post-processing hook for handling everything.
    """
    if info.get("status") != "finished":
        return
    if info.get("postprocessor") != "MoveFiles":
        return

    data = info["info_dict"]

    vid_state.tmp_file = data["filepath"]
    vid_state.tmp_dir = os.path.dirname(vid_state.tmp_file)

    folder = categorize(data)

    raw_date = data.get("upload_date")
    date = datetime.strptime(raw_date, "%Y%m%d").strftime("%m-%d-%y") if raw_date else "unknown-date"

    title = sanitize(data.get("title", "unknown-title"))
    vid_state.vid = data.get("id")
    vid_state.ext = data["filepath"].split(".")[-1]

    # Each video has it's own folder.
    video_dir_name = f"{date} - {title} [{vid_state.vid}]"
    channel_root = os.path.join(args.out, sanitize(channel_info.display_name))
    vid_state.video_dir = os.path.join(channel_root, folder, video_dir_name)
    os.makedirs(vid_state.video_dir, exist_ok=True)

    video_filename = f"{title}.{vid_state.ext}"
    new_video_path = os.path.join(vid_state.video_dir, video_filename)
    try:
        shutil.move(vid_state.tmp_file, new_video_path)
        print("Saved video:", new_video_path)
    except Exception as e:
        print(f"Failed to move video {vid_state.tmp_file} -> {new_video_path}: {e}")

    live_chat_filename = f"{vid_state.vid}.live_chat.json"
    live_chat_renamed = "live_chat.json"
    live_chat_path = os.path.join(vid_state.video_dir, live_chat_renamed)
    if os.path.exists(live_chat_filename):
        try:
            shutil.move(live_chat_filename, live_chat_path)
            print("Saved live chat:", live_chat_path)
        except Exception as e:
            print(f"Failed to move live chat {vid_state.tmp_file.replace(f'.{vid_state.ext}', '.live_chat.json')} -> {live_chat_path}: {e}")
            pass

def filter_video_ids(entries):
    for entry in entries:
        if (entry.get('_type') == 'url') and ('id' in entry) and ('youtube' in entry.get('ie_key','').lower()):
            yield entry['id']
        elif entry.get('entries'):
            yield from filter_video_ids(entry['entries'])

def postprocess_subs(info):
    pattern = os.path.join(vid_state.tmp_dir, f"{vid_state.vid}.*.srv3")
    subtitle_files = glob.glob(pattern)

    for sub_path in subtitle_files:
        filename = os.path.basename(sub_path)
        parts = filename.split(".")
        if len(parts) < 3:
            print(f"Skipping malformed subtitle filename: {filename}")
            continue
        lang = parts[-2]

        # Invoke ytsubconverter
        # TODO: This won't work on windows, there needs to be a config file where I can specify the path to ytsubconverter (or try searching %PATH%)
        ass_tmp_path = os.path.join(vid_state.tmp_dir, f"{vid_state.vid}.{lang}.ass")
        try:
            subprocess.run(
                ["ytsubconverter", sub_path, ass_tmp_path],
                check=True
            )
            print(f"Converted {sub_path} -> {ass_tmp_path}")
        except subprocess.CalledProcessError as e:
            print(f"ytsubconverter failed for {sub_path}: {e}")
            ass_tmp_path = None
        except FileNotFoundError:
            print("ytsubconverter not found; skipping conversion.")
            ass_tmp_path = None

        # Handle the subtitle switcheroo
        subs_dir = os.path.join(vid_state.video_dir, "subtitles")
        os.makedirs(subs_dir, exist_ok=True)

        new_srv3_path = os.path.join(subs_dir, f"{lang}.srv3")
        try:
            shutil.move(sub_path, new_srv3_path)
        except Exception as e:
            print(f"Failed to move {sub_path} -> {new_srv3_path}: {e}")

        if ass_tmp_path:
            new_ass_path = os.path.join(subs_dir, f"{lang}.ass")
            try:
                shutil.move(ass_tmp_path, new_ass_path)
            except Exception as e:
                print(f"Failed to move {ass_tmp_path} -> {new_ass_path}: {e}")

parser = YTArcArgumentParser(
    description="Downloads videos or channels.",
    formatter_class=argparse.RawDescriptionHelpFormatter
)
main_group = parser.add_mutually_exclusive_group(required=True)
main_group.add_argument("--channel", help="YouTube channel handle (with @)")
main_group.add_argument("--shorts", help="Download all Shorts from a creator (with @)")
main_group.add_argument("--video", help="YouTube video slug (multiple accepted, comma separated)")

parser.add_argument("--out", help="Output directory (optional)")
parser.add_argument("--subs", help="Download subtitles", default=False, action="store_true")

args = parser.parse_args()

ydl_sub_opts = {
    "writesubtitles": True,
    "subtitleslangs": ["all"],
    "subtitlesformat": "srv3",
    "live_chat": True
}

TARGET = ""
DIR_NAME=""

if args.channel:
    TARGET = args.channel
    DIR_NAME = args.out if args.out else args.channel

if args.video:
    args.video = args.video.split(",")
    TARGET = f"watch?v={args.video[0]}"
    DIR_NAME = args.out if args.out else args.video

if args.shorts:
    TARGET = args.shorts + "/shorts"
    DIR_NAME = args.out if args.out else "shorts"

if args.out is None:
    args.out = f"{DIR_NAME}/"

ydl_opts = {
    "download_archive": f"{DIR_NAME}/downloaded.txt",
    "ignoreerrors": True,
    "outtmpl": "%(id)s.%(ext)s",
    "remux_video": "mkv",
    "merge_output_format": "mkv",
    "postprocessors": [
        {"key": "FFmpegMetadata"},
        {"key": "EmbedThumbnail"},
    ],
    "postprocessor_hooks": [on_postprocess, postprocess_subs],
}

if args.subs:
    ydl_opts.update(ydl_sub_opts)

TARGET_URL = f"https://www.youtube.com/{TARGET}"

channel_info = ChannelInfo()

with YoutubeDL({"extract_flat": True, "quiet": True}) as y:
    print("Downloading Channel Metadata...")
    info = y.extract_info(TARGET_URL, download=False)
    print("Metadata Downloaded!")
    channel_info.display_name = str(info.get("channel")) or ""
    channel_info.subscribers = int(info.get("channel_follower_count")) or 0
    channel_info.description = str(info.get("description")) or "No description"

    video_list = []

    if args.channel or args.shorts:
        video_list = list(filter_video_ids(info.get("entries", [])))

    if args.video:
        video_list = args.video[:]

    total_videos = len(video_list)
    current_index = 0

if args.video:
    for video_id in args.video:
        current_index += 1
        print(f"[{current_index} / {total_videos}] Downloading: {video_id}")

        TARGET_URL = f"https://www.youtube.com/watch?v={video_id}"
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([TARGET_URL])
else:
    for video_id in video_list:
        current_index += 1
        print(f"[{current_index} / {total_videos}] Downloading: {video_id}")

        video_url = f"https://www.youtube.com/watch?v={video_id}"
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
