# CSV to ClickHouse: development checkpoint

Date: 2026-06-20 09:42:53 +03:00
Repository: `C:\Users\odar\Downloads\codex\projects\csv_to_click`
Branch at checkpoint: `main`
Remote state before checkpoint file: `main...origin/main`

## Purpose

This checkpoint compresses the current development thread so a new Codex session can continue without rereading the full conversation.

## Product Goal

Build a Streamlit service that loads CSV files into ClickHouse by guiding the user through:

1. CSV path and CSV read settings.
2. CSV column mapping to ClickHouse target names.
3. Data type review and manual type overrides.
4. ClickHouse connection and table settings.
5. DDL preview.
6. Safe create/load into a local `ReplicatedMergeTree` table plus a distributed `Distributed` table.

The UI should not show downstream mapping, DB, or DDL controls before the required earlier steps are complete.

## Implemented Behavior

### UI flow

- First screen includes `CSV path` and CSV read settings before the first read attempt.
- After successful CSV read, the UI shows CSV preview and schema/mapping controls.
- ClickHouse settings are applied after schema/type review.
- `Apply parameters` and `Test connection` are separate buttons.
- `Create tables and load` uses a progress bar and accumulated process log.

### Persistent settings

Persistent settings are stored in the user home settings file through `src/csv_click/settings.py`.

Persisted/static settings:

- `host`
- `port`
- `username`
- `secure`
- `verify`
- `client_cert`
- `client_key`
- `database`
- `cluster`
- `batch_size`
- `strict_preflight`
- CSV `separator`
- CSV `encoding`

Not persisted because they are table-specific:

- CSV path
- distributed table name
- `ORDER BY`
- `PARTITION BY`
- distributed sharding key

Initial certificate defaults came from `pandas_to_click.ipynb`:

- `/home/jovyan/tsh/clickhouse-prod.crt`
- `/home/jovyan/tsh/clickhouse-prod.key`

If the user changes certificate/key paths in UI and applies parameters, those paths are saved for future service restarts.

### ClickHouse defaults and DDL

DDL shape follows the user-provided example:

- local table: `<db>.<table>_local`
- distributed table: `<db>.<table>`
- cluster: `ON CLUSTER clickhouse`
- local engine:
  `ReplicatedMergeTree('/clickhouse/tables/{shard}-{uuid}/<local_table>', '{replica}')`
- local settings:
  `SETTINGS index_granularity = 8192`
- no trailing semicolon, to avoid HTTP driver multi-statement issues.

Important correction:

- `ORDER BY ID` is not a global default.
- `sipHash64(ID)` is not a global default.
- UI now selects `ORDER BY` and distributed sharding column from confirmed `target_name` values.
- Distributed sharding key is generated as `sipHash64(`<selected_column>`)`.
- Identifiers are safely backticked.
- `PARTITION BY` remains manual, with protection against `;` / multi-statement input.

### ClickHouse connection handling

- `Test connection` checks connectivity with `SELECT 1`.
- It does not require table-specific fields.
- SSL error messaging was improved for expired client certificates.
- `SSLV3_ALERT_CERTIFICATE_EXPIRED` is treated as likely expired client certificate/key, not something fixed by `verify=False`.
- Current client cert/key paths are included in the error message.

### Safe create/load flow

Current staged flow:

1. Connect/test connection.
2. Preflight existing table state.
3. Create local table.
4. Verify local table exists on cluster.
5. Create distributed table.
6. Verify distributed table exists.
7. Load CSV.
8. Final success.

Partial state handling:

- If only local or only distributed table exists before start, the app drops both target tables and continues.
- If both target tables already exist before start, the run stops with `ExistingTableError`.
- If the current run fails after create/load started, the app cleans up both target tables and logs cleanup.

This was added after ClickHouse returned:

```text
Code: 60. DB::Exception: Table sandbox.aaa_test_streamlit_local does not exist. (UNKNOWN_TABLE)
```

### CSV preview and encoding

Preview is read with:

- selected/effective separator
- selected/effective encoding
- `dtype=str`
- `keep_default_na=False`

The same effective `ReadOptions` are used for schema inference and load, so preview and ClickHouse insert use the same encoding path.

Mojibake handling:

- Detects common bad strings such as `С‚РµСЃС‚`, `�`, and `пїЅ`.
- Tries configured candidate encodings: selected encoding plus `utf_8`, `utf-8-sig`, `cp1251`, `windows-1251`.
- If a clean encoding is found, it updates `csv_read_options` and uses that for preview/schema/load.
- If all candidates still produce replacement characters/mojibake, it raises `CsvSchemaError` and stops before preview/schema/DDL/load.

Key conclusion from the latest debug:

- The modified `tests/test_csv.csv` contained UTF-8 replacement bytes `EF BF BD` repeated 155 times.
- That means the original Cyrillic was already lost before the service read the file.
- Decoding as UTF-8 shows `\ufffd`; decoding as cp1251/windows-1251 shows `пїЅ`.
- No encoding option can reconstruct the original text from that corrupted file.
- The app now fails fast for that condition instead of previewing or loading corrupted `пїЅ` values.

The user later confirmed:

```text
Да ты прав, оказывается это был битый файл все время
```

## Relevant Files

Core app:

- `src/csv_click/app.py`
- `src/csv_click/pandas_loader.py`
- `src/csv_click/clickhouse.py`
- `src/csv_click/settings.py`
- `src/csv_click/schema.py`

Tests:

- `tests/test_pandas_loader.py`
- `tests/test_app_state.py`
- `tests/test_clickhouse.py`
- `tests/test_settings.py`
- `tests/test_csv.csv`

Docs/examples:

- `README.md`
- `example_pandas_to_click.md`
- `pandas_to_click.ipynb`

## Recent Commits

Latest relevant commits on `main`:

- `59a5c33 Reject corrupted CSV replacement characters`
- `71b2d11 Recover CSV encoding from real fixture`
- `295c38f Use detected CSV encoding for load`
- `4e18be6 Improve CSV preview and safe ClickHouse load flow`
- `0afa3f0 Validate ClickHouse client certificate expiry`

## Verification Evidence

Commands run during the latest debug before this checkpoint:

```bash
uv run --extra dev pytest tests/test_pandas_loader.py::test_choose_read_options_prefers_encoding_with_lower_mojibake_score tests/test_pandas_loader.py::test_choose_read_options_rejects_real_csv_with_replacement_characters -q
```

Result:

```text
2 passed
```

```bash
uv run --extra dev pytest tests/test_pandas_loader.py -q
```

Result:

```text
16 passed
```

```bash
uv run --extra dev pytest
```

Result:

```text
57 passed
```

```bash
git diff --check
```

Result:

- exit code `0`
- only LF/CRLF warnings were observed

Additional manual verification for corrupted CSV:

```text
ef_bf_bd_count 155
STOPPED_BEFORE_DDL
CSV preview still contains replacement characters or mojibake after trying utf_8, utf-8-sig, cp1251, windows-1251. The source file is likely already corrupted or was exported with a wrong encoding.
```

## Current Repository State Before This File

Before creating this checkpoint:

```text
## main...origin/main
```

`git status --porcelain=v1` returned no modified files.

This checkpoint file is the only intended new working tree change from this request.

## Known Gaps / Risks

- `README.md` content displayed as mojibake in the current Windows PowerShell output. This may be a console/codepage display issue or an already misencoded file. It was not changed in this checkpoint.
- The OpenAI/Codex manual helper could not run because `node` was not available in PATH and no bundled runtime dependency was configured in this thread.
- Official web search did not return a useful Codex manual page for a documented checkpoint folder name.
- Because `.codex` already exists in the repository and is the Codex project-level configuration area, this checkpoint was placed under `.codex/checkpoints/`.

## Suggested Next Steps

1. Commit this checkpoint file if it should be kept in the repository.
2. If product docs need cleanup, inspect `README.md` bytes directly before editing because the current rendered output shows mojibake.
3. For any new CSV encoding issue, first inspect raw bytes:
   - if bytes are valid original encoding, improve detection/load path;
   - if bytes already contain `EF BF BD`, fail fast and request a clean source export.
4. For ClickHouse runtime issues, keep testing with `Test connection` first, then `Preview DDL`, then `Create tables and load`.
5. Do not reintroduce table-specific global defaults for `ORDER BY` or sharding key.

