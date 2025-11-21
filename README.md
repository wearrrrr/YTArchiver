# YTArchiver

This is a simple script to archive YouTube videos and channels!

# Features

- Download entire channels at a time
- Download videos, multiple at a time!
- Customizable output directory
- Places everything in a smart directory structure for content management.

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
4. Run it:
   `python3 dl.py <options>`
