# YTArchiver

This is a simple script to archive YouTube videos and channels!

# Features

- Download entire channels at a time
- Download videos, multiple at a time!
- Customizable output directory
- Places everything in a smart directory structure for content management.
- Friendly multi-command CLI for channels, Shorts, or specific videos
- yt-dlp logs automatically captured to a file for troubleshooting
- Colorized progress banner with per-video stats while downloading
- Background daemon to automatically monitor channels for new uploads

# Requirements
- Python 3.1x (Older probably works, but untested!)
- Deno (to solve YT challenges)

# Running
1. Initialize the virtual environment:
   `python3 -m venv venv`
2. Activate the virtual environment:
   - If using a POSIX compatible shell:
     `source activate_posix.sh`
   - If using fish:
     `source activate_fish.sh`
3. Install required packages:
   `pip install -r requirements.txt`
4. Run it (pick the subcommand you need):

   ```bash
   # Entire channel archive
   python3 dl.py channel @veritasium

   # Shorts feed + subtitles
   python3 dl.py shorts @veritasium --subs

   # Individual videos with a custom output directory
   python3 dl.py video dQw4w9WgXcQ jNQXAC9IVRw --out /data/archive

   Run the background daemon to monitor channels
   python3 dl.py watch
   ```

# Background Daemon

The watch daemon automatically monitors YouTube channels for new uploads and queues download jobs. Configure watched channels through the web UI, I'll add support for adding through the CLI eventually.

```bash
python3 dl.py watch --poll-interval 600 --batch-size 10
```

# Logging

yt-dlp output is redirected to `logs/ytarchiver.log` by default. Override this path with `--log-file` if you prefer a different destination.

## Web UI (Flask)

Prefer a browser experience? Install Flask (`pip install -r requirements.txt` already covers it) and launch the local dashboard:

```bash
FLASK_APP=webui.app flask run --reload
# or
python -m flask --app webui.app run --reload
```

The page (http://localhost:5000/) lets you start channel/shorts/video jobs with the same options as the CLI. Each submission becomes a queued job, writes its own log file under `logs/`, and exposes a `/jobs/<id>.json` endpoint you can poll for status. Jobs currently run sequentially (matching the CLI behavior), so submit them in the order you want them executed.

The web UI includes a watchlist section where you can add YouTube channels to be automatically monitored for new uploads. The background daemon runs continuously and will queue download jobs when new videos are detected.

# Fancy CLI display

Each download renders a colorized progress banner that shows the queue position, channel name, ID, duration, and canonical URL. The console clears between items for a cleaner feel, you can disable it with `--no-clear` if you prefer a scrollback log.
