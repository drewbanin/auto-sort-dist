import psycopg2
import sys
import os
from collections import defaultdict
import json
import yaml

config = yaml.load(open('config.yml'))

db_config = config['conn']
conn_str = "dbname='{dbname}' user='{user}' host='{host}' password='{password}' port='{port}'".format(**db_config)

schemas = config['schemas']

handle = psycopg2.connect(conn_str)
cursor = handle.cursor()


def run(sql, get=True):
    cursor.execute(sql)
    if get:
        return cursor.fetchall()
    else:
        return

sp_query = "set search_path to {}".format(", ".join(["'{}'".format(schema) for schema in schemas]))
run(sp_query, get=False)


def get_tables(schema):
    sql = "select tablename from pg_tables where schemaname = '{schema}'".format(schema=schema)
    return [(schema, table) for (table,) in run(sql)]


def get_cols():
    schemas_csv = ",".join(["'{}'".format(schema) for schema in schemas])
    sql = """select "schemaname", "tablename", "column" from pg_table_def where "schemaname" in ({})""".format(schemas_csv)
    data = [(schema, table, column) for (schema, table, column) in run(sql)]
    schema_table_cols = defaultdict(list)

    for row in data:
        schema, table, col = row
        schema_table_cols[(schema, table)].append(col)

    return schema_table_cols

schema_tables = []
for schema in schemas:
    schema_tables.extend(get_tables(schema))


def get_create_query(schema_name, table_name, keys):
    sorttype = keys.get('sorttype', 'compound')
    sortkeys = keys.get('sort', None)

    if sortkeys is not None and type(sortkeys) in [str, unicode]:
        sortkeys = [sortkeys]

    diststyle = keys.get('diststyle', 'key')
    distkey = keys.get('dist', None)

    sd_keys = ""
    if distkey is not None:
        sd_keys += 'diststyle {diststyle} distkey({distkey}) '.format(diststyle=diststyle, distkey=distkey)

    if sortkeys is not None:
        sd_keys += '{sorttype} sortkey({sortkeys}) '.format(sorttype=sorttype, sortkeys=", ".join(sortkeys))

    return """\
CREATE TABLE "{schema}"."{table}__tmp_with_keys"\n{sd_keys}\nAS (
    select * from "{schema}"."{table}"
);""".format(schema=schema_name, table=table_name, sd_keys=sd_keys)


def get_rename_query(schema, old_table, new_table):
    return 'alter table "{schema}"."{old_table}" rename to "{new_table}";'.format(schema=schema, old_table=old_table, new_table=new_table)


def get_drop_query(schema, table_name):
    return 'drop table "{schema}"."{table_name}" cascade;'.format(schema=schema, table_name=table_name)


def get_comment(schema, table):
    query = "select description from pg_description where objoid = '{}.{}'::regclass;".format(schema, table)
    res = run(query)
    if len(res) != 1:
        print >> sys.stderr, "    WARNING: No comment found for {}.{}".format(schema, table)
        return ""
    else:
        return res[0][0]


def best_guess_sort_key(table_name, cols):
    lower_cols = [col.lower() for col in cols]

    # hacks hacks hacks
    if table_name == 'spree_orders' and 'completed_at' in lower_cols:
        return 'completed_at'
    elif table_name in ['shipments', 'return_shipments', 'spree_shipments'] and 'shipped_at' in lower_cols:
        return 'shipped_at'

    for pref in config['sort_key_guesses']:
        if pref in lower_cols:
            return cols[lower_cols.index(pref)]
    return None


def best_guess_dist_key(cols):
    lower_cols = [col.lower() for col in cols]
    for pref in config['dist_key_guesses']:
        if pref in lower_cols:
            return cols[lower_cols.index(pref)]
    return None

all_cols = get_cols()


missing_dist = 0
missing_sort = 0
filenames = []

for i, (schema, table) in enumerate(schema_tables):
    cols = all_cols[(schema, table)]

    keys = {
        "dist": best_guess_dist_key(cols),
        "sort": best_guess_sort_key(table, cols)
    }

    missing = ""
    if keys['dist'] is None:
        missing_dist += 1
        missing += " dist"
    if keys['sort'] is None:
        missing_sort += 1
        missing += " sort"

    if len(missing) > 0:
        print >> sys.stderr, "Running {} of {} -- {}.{} -- MISSING: {}".format(i + 1, len(schema_tables), schema, table, missing)
    else:
        print >> sys.stderr, "Running {} of {} -- {}.{}".format(i + 1, len(schema_tables), schema, table)

    comment = get_comment(schema, table)
    create_stmt = get_create_query(schema, table, keys)
    move_existing = get_rename_query(schema, table, table + '__tmp_backup')
    move_new = get_rename_query(schema, table + "__tmp_with_keys", table)
    delete_old = get_drop_query(schema, table + "__tmp_backup")
    new_comment = '''COMMENT ON table "{}"."{}" IS '{}';'''.format(schema, table + "__tmp_with_keys", comment)
    cols_str = ", ".join(cols)
    full_query = "-- cols: {}\n\nBEGIN;\n{}\n{}\n{}\n{}\n{}\nEND;".format(cols_str, create_stmt, new_comment, move_existing, move_new, delete_old)

    filename = "queries/{}/{}.sql".format(schema, table)
    if not os.path.exists("queries/{}".format(schema)):
        os.makedirs("queries/{}".format(schema))

    with open(filename, 'w') as fh:
        fh.write(full_query)
        filenames.append(filename)

manifest = {}
for filename in filenames:
    schema_name = os.path.basename(os.path.dirname(filename))
    table_name = os.path.basename(filename).split(".")[0]
    fqn = "{}.{}".format(schema_name, table_name)
    manifest[fqn] = filename

with open('manifest.json', 'w') as fh:
    json.dump(manifest, fh)

print >> sys.stderr, "Missing %% : dist: %0.2f, sort: %0.2f" % (
    missing_dist / float(len(schema_tables)) * 100, missing_sort / float(len(schema_tables)) * 100)


cursor.close()
handle.close()
