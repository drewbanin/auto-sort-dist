import psycopg2
import sys, os
from collections import defaultdict
import json, yaml

config =  yaml.load(open('config.yml'))
db_config = config['conn']
conn_str = "dbname='{dbname}' user='{user}' host='{host}' password='{password}' port='{port}'".format(**db_config)

handle = psycopg2.connect(conn_str)
cursor = handle.cursor()

def run(sql, get=True):
    cursor.execute(sql)
    if get:
        return cursor.fetchall()
    else:
        return

with open('manifest.json') as fh:
    filenames = json.load(fh)

start_at = sys.argv[1] if len(sys.argv) > 1 else None

keys = sorted(filenames.keys())
for i, fqn in enumerate(keys):
    filename = filenames[fqn]
    if start_at is not None and fqn < start_at:
        continue
    sql = open(filename).read()

    print >> sys.stderr, "Running {} of {} -- {}".format(i + 1, len(keys), fqn)

    run(sql, get=False)

cursor.close()
handle.close()
