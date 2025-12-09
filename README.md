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
   ```

# Logging

yt-dlp output is redirected to `logs/ytarchiver.log` by default. Override this path with `--log-file` if you prefer a different destination.

# Fancy CLI display

Each download renders a colorized progress banner that shows the queue position, channel name, ID, duration, and canonical URL. The console clears between items for a dashboard-like feel; disable that behavior with `--no-clear` if you prefer a scrollback log.
