#!/usr/bin/env python3
"""Print {"hash", "time"} of the latest Dolt commit on main (the 00:00 autocommit), read as the `search` user from DATABASE_URL."""
import json
import os
import sys
from urllib.parse import urlparse

import pymysql

u = urlparse(os.environ["DATABASE_URL"])
cn = pymysql.connect(host=u.hostname, port=u.port or 3306, user=u.username, password=u.password or "", database=u.path.lstrip("/").split("/")[0])
cur = cn.cursor()
cur.execute("select dolt_hashof('main')")
h = cur.fetchone()[0]
cur.execute("select date from dolt_log where commit_hash = %s", (h,))
row = cur.fetchone()
if not row:
    sys.exit("commit time of main not found in dolt_log")
print(json.dumps({"hash": h, "time": row[0].strftime("%Y-%m-%dT%H:%M:%SZ")}))  # Dolt stores UTC
