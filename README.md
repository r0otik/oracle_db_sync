# oracle_db_sync

* Minimal effort required to add a new table to synchronization — in most cases, adding a single line to `config.yml` is enough
* Ability to back up a table before applying any changes
* Backup file rotation
* Ability to synchronize not only by full reload (TRUNCATE -> INSERT), but also by detecting differences between tables and inserting only missing rows
* Ability to define key columns used to detect differences
* During full reload, if INSERT fails (for example, due to data type mismatch between remote and local tables), the local table will be restored to its original state as it was before synchronization started
* Ability to configure synchronization not only via DBLINK
* Automatic exclusion of identity columns before INSERT
* Ability to map columns (local table column names do not need to fully match remote ones)
* Ability to include only selected columns in synchronization
* If a table does not exist in the local database, it will be created automatically by the script

---

## Overall structure of config.yml

The configuration file consists of three logical sections:

```yaml
General:
Connections:
Sync:
```

Each section is described below.

---

## General section

Global script parameters.

```yaml
General:
  backup_path: ./backup
  log_file: ./dbSync.log
```

### Parameters

| Parameter | Required | Description |
|---------|----------|-------------|
| `backup_path` | no | Directory used to store CSV backups of tables before synchronization. If not specified, `./backup` is used. |
| `log_file` | yes | Path to the log file |
| `log_level` | no | Logging level, default is INFO |

---

## Connections section

This section defines **database connections** as well as logical connections via `dblink`.

```yaml
Connections:
  pl_db:
    db_user: USERNAME
    db_password: testing123
    db_host: myhostname.test.local
    db_name: ORCL

  old:
    dblink: True
    scheme_name: newschema
    postfix: old_database
    avail_from: pl_db

  prod:
    dblink: True
    postfix: prod
    avail_from: pl_db
```

---

### Direct database connection

Used for direct Oracle database connections.

```yaml
pl_db:
  db_user: USERNAME
  db_password: PASSWORD
  db_host: hostname
  db_name: SERVICE_NAME
```

#### Parameters

| Parameter | Required | Description |
|---------|----------|-------------|
| `db_user` | yes | Oracle user |
| `db_password` | yes | Password |
| `db_host` | yes | Database host |
| `db_name` | yes | Service name |
| `db_port` | no | Port, default is `1521` |

---

### DBLINK connection

Used when tables are available **only via dblink** from another database.

```yaml
old:
  dblink: True
  scheme_name: newschema
  postfix: old_database
  avail_from: pl_db
```

#### How it works

* The script connects to the database specified in `avail_from`
* Tables are queried as `SCHEMA.TABLE@DBLINK`

#### Parameters

| Parameter | Required | Description |
|---------|----------|-------------|
| `dblink` | yes | Always `True` |
| `avail_from` | yes | Name of the connection through which the dblink is available |
| `scheme_name` | no | Table schema |
| `postfix` | no | DBLINK name |

---

## Sync section

Describes **synchronization jobs**.

For those familiar with Ansible, this is an obvious analogy to tasks inside a playbook.

```yaml
Sync:
  prod_sync_diff:
```

---

## Synchronization job description

```yaml
prod_sync_diff:
  local_db: pl_db
  remote_db: prod
  sync_type: diff
  backup: True
  rotate: 1
  tables:
    - STREETS_TEST: STREETS
      map_columns:
        ST_NAME: STREET_NAME
        ST_ENG: STREET_NAME_ENG
      diff_key:
        - ST_NAME
        - ST_ENG
        - CITY_ID
```

| Parameter | Required | Description |
|---------|----------|-------------|
| `local_db` | yes | Connection used to apply changes. Must be defined in *Connections* |
| `remote_db` | yes | Connection used to fetch reference data. Must be defined in *Connections* |
| `sync_type` | yes | Synchronization type (see below) |
| `backup` | no | Whether to back up the local table before applying changes |
| `rotate` | no | Backup rotation count. Works only if `backup=True` |
| `tables` | yes | List of synchronized tables in the format: `<local_table>: <remote_table>` |

### Synchronization types

**truncate** — full table reload: TRUNCATE -> INSERT

**diff** — incremental synchronization based on key columns

Within a specific table task, the following optional parameters may be specified:

| Parameter | Description |
|---------|-------------|
| `map_columns` | Dictionary of `<local_column>: <remote_column>` pairs |
| `only_mapped` | If True, only columns listed in `map_columns` participate in extraction/comparison |
| `diff_key` | Works only with `sync_type: diff`. List of column names (local names!) used as a composite key to detect differences. By default, all columns are used. |

---

## Recommendations

* Use UPPERCASE for table and column names
* Validate configuration before adding it to cron
* Enable backups

---

## Execution

Several command-line options are available for manual execution:

| Option | Description | Default |
|-------|-------------|---------|
| `-i, --input` | Input table (reference table) | - |
| `-o, --output` | Output table (target table to be modified) | - |
| `-l, --local-conn` | Local DB connection name (must be defined in *Connections*) | pl_db |
| `-r, --remote-conn` | Remote DB connection name (must be defined in *Connections*) | prod |
| `-m, --method-sync` | Synchronization method | truncate |
| `-s, --show-only` | Show comparison results without applying changes. If other parameters are set, shows results for the specified table; otherwise for all tables in config | no |
| `-ll, --log-level` | Logging level. Overrides the level defined in config | INFO |

```bash
cd /data/cdrs/scripts/dbSync/
python3 ./dbSync.py -o STREETS_TEST -i STREETS -m diff
```

```bash
python3 ./dbSync.py -o CMDOFF_VC_APN -i VC_APN -r old -s yes
```

When invoking synchronization for a table via CLI options, column mapping and other advanced features are currently not supported — the tables must be identical.

---

## Planned improvements

* When generating DDL for table creation (used if a table does not exist), a simplified form without constraints is currently used. The functionality exists but is temporarily disabled.
* ~~Use logging with proper log levels~~
* ~~Reduce memory consumption~~
* ~~Move some computations into the database~~

Issues discovered during real-world usage will be fixed as they are found.
