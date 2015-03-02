Telemetry Parser
================

It's a minimal script to transform raw logs into a human-readable CSV file.
Log files are compressed in multiple tarballs, all zipped into one zip file.
Each log file is a JSON-like text file. We mention "-like" because its format does not respect the JSON strict format for keys.

Installation
------------

```shell
git clone git@github.com:TonyEight/telemetry-parser.git
cd telemetry-parser/
virtualenv .
source bin/activate
pip install -r requirements.txt
```

Run
---

First, retrieve the zipped logs and put the zip file in the raw_logs directory.
Then rename it : raw_logs.zip

Then run the following commands :

```shell
cd telemetry-parser/
source bin/activate
python tl-parser.py
```

The resulting CSV is at the script base directory.
