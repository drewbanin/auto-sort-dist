
SORT + DIST KEY ADDER v0.0.0
============================

Overview:
==========

Sort and Distribution keys help tune Redshift to deliver optimal performance. Unfortunately,
it's a little tricky to add sort and dist keys to an existing table. The process looks like:

```sql
BEGIN TRANSACTION;
CREATE TABLE temp_{tablename} SORTKEY({sortkeys}) DISTKEY(distkey) AS (
    SELECT * FROM {tablename}
);
COMMENT ON table "{schema}"."temp_{table}" IS '{comment}';
RENAME {tablename} TO backup_{tablename};
RENAME temp_{tablename} to {tablename};
DROP TABLE backup_{tablename};
END TRANSACTION;
```

Config:
==========

  Copy the `config.sample.yml` file to `config.yml`
    cp config.sample.yml config.yml
  
  Edit the contents of `config.yml` as needed. You'll need to
    - set up your db connection parameters
    - whitelist schemas
    - config "best guess" sort and dist keys


Usage:
==========

There are two enclosed scripts:
  gen.py
  run.py

This distinction makes it possible to recover from failure without starting from the beginning. Run:

    python gen.py

to "compile" SQL scripts. Next,

    python run.py

will run the SQL against your Redshift instance. To start from a specific model, run

    python run.py bedpost.first_model_to_run

All models which are lexicographically >= the first arg are run.
