# (pfin-back-etl) Personal Finance Backend Extract/Transform/Load

## Description
This project consists of a set of backend scripts to extract data from external
APIs, and transform them to data formats that align with the overall PFin project
table structures in SupaBase.

## Some External Requirements
For this to work, this project will need a few external things set up:

### UV installation
This project (and all of the connected projects) uses uv as a python and package
manager. It's fast and pretty idiot proof... which is perfect for my skill level.
- macOS: `brew install uv`
- Linux: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Windows: `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`

### Environmental Variables
A `.env` file will need to get added to the project root directory `pfin_back_etl`.
This file contains environmental variables that the scripts use to define API Keys
and database connection variables. An example of this file sits in the root
directory as `sample.env` with non-valid entries.

```
BLS_SERIES_ID=CUUR0000SA0
BLS_API_KEY=<Bureau_of_Labor_Statistics_API_KEY>
FMP_API_KEY=<Financial_Modeling_Prep_API_KEY>
PFIN_DB_USER=<example::postgres.your_project_ref>
PFIN_DB_HOST=<example::pfindash.com>
PFIN_DB_PORT=<example::5432>
PFIN_DB_NAME=<example::postgres>
PFIN_DB_PASSWORD=<password_for_SupaBase_database>
```

### A Valid Financial Modeling Prep API Key
This is what I'm using as a stock financials data source. Other sources could work
perfectly well with some modification... but this is what I'm currently using. The
starter plan supplies most of the data for 2-years of historical data and a pretty
reasonable price. TBD: I should look at what kind of features should be disabled if
the free plan was used instead.
TBD: LINK_TO_FINANCIAL_MODELING_PREP_PLANS

### A BLS API Key
The U.S. Bureau of Labor & Statistics requires registration for an API key (free)
to query data about historical consumer price indexes.
TBD: LINK_TO_BLS_REGISTRATION

### A Running SupaBase Instance
This could be self-hosted or cloud hosted on supabase.com. Or a docker instance on
AWS, etc. The free hosting tier on supabase.com should be sufficient for the database
size and query load for this application. The connection information there
(shared pool) should be added to the `.env` file in the root directory
(see notes above).
TBD: LINK_TO_SUPABASE_SITE

### A Running pfin Schema on Your SupaBase Instance
This requires the pfin-dash project. It has the SQL Data Definition Language (DDL)
commands and migrations under revision control to execute to the postgresql database
on SupaBase.
TBD: Add a link to project and how to setup the database.

## Installation
1. Install uv (see above)
2. Clone the project
3. Navigate to your cloned repository: `cd <Project Directory>/pfin_back_etl`
4. Initialize uv and the environment: `uv sync`

## Testing

### Setup

Install the test dependencies:

```bash
uv sync --group test
```

This pulls in `pytest` and `pytest-cov` on top of the core project dependencies.

### Test Markers

Tests are organized into two tiers using pytest markers (configured in
`pyproject.toml` with `--strict-markers` enforced):

| Marker          | Description                                                    |
| --------------- | -------------------------------------------------------------- |
| `unit`          | Fast tests with no external dependencies (no DB, no API)       |
| `integration`   | Tests that require database and/or API connections             |

### Running Tests

```bash
# Run all tests
uv run pytest

# Run only unit tests (no credentials needed)
uv run pytest -m unit

# Run only integration tests (requires .env with valid credentials)
uv run pytest -m integration

# Run unit tests with coverage report
uv run pytest -m unit --cov=pfin_back_etl --cov-report=term-missing
```

### Test Structure

```
tests/
  conftest.py          # Shared fixtures (sample API responses, DataFrames)
  test_utils.py        # Unit tests for utility functions
  test_core.py         # Unit tests for core ETL classes (SBaseConn, PFinFMP)
  test_dbase_setup.py  # Integration tests for DB init and table reflection
  test_dbase_update.py # Integration tests for ETL update operations
```

**Unit tests** (`test_utils.py`, `test_core.py`) use `unittest.mock` to isolate
logic from external services. They cover:
- camelCase to snake_case column conversion
- Empty string cleaning in DataFrames
- Schema casting between source and target DataFrames
- List-of-dicts to Polars DataFrame conversion
- BLS CPI data parsing (mocked HTTP)
- Environment variable loading and validation
- Row isolation logic (new rows via anti-join, updated rows via semi-join)
- FMP API response conversion and concatenation
- SQLAlchemy automap module name resolution

**Integration tests** (`test_dbase_setup.py`, `test_dbase_update.py`) connect to the
real SupaBase database and external APIs. They verify:
- Backend initialization and schema reflection
- Table structure against an expected list of `pfin` schema tables
- Full ETL update cycle for each table type (CPI, assets, equity profiles,
  income statements, balance sheets, cash flows, earnings, EOD prices)

> **Note:** Integration tests write to the production database. Run them locally
> only, not in CI. They require a valid `.env` file with real credentials.

### Fixtures

Shared fixtures live in `tests/conftest.py`:

- `backend` -- Session-scoped `PFinBackend` instance reused across all integration
  tests. Automatically skips the entire integration suite if the DB connection fails.
- `sample_fmp_income_json`, `sample_fmp_profile_json` -- Sample FMP API response
  payloads for mocking.
- `sample_bls_cpi_json` -- Sample BLS CPI API response structure.
- `sample_camel_case_columns` -- Column name list for snake_case conversion tests.
- `sample_df_old`, `sample_df_new` -- Polars DataFrames for testing row isolation
  (INSERT vs UPDATE) logic.

### Data Validation

Data validation happens at several points in the ETL pipeline:

- **Column normalization** (`utils.col_to_snake`) -- API responses arrive in
  camelCase; columns are converted to snake_case before any DB operations.
- **Empty string cleaning** (`utils.clean_empty_str_df`) -- Replaces `""` with
  `None` so nullable DB columns get proper NULLs.
- **Schema casting** (`utils.apply_schema_df`) -- Ensures DataFrame dtypes match
  the target table schema before insert/update (uses `strict=False` for lenient
  casting).
- **Common column calculation** (`core.SBaseConn._calc_common_cols_df`) -- Only
  columns present in both the API response and the DB table are carried forward,
  preventing mismatched inserts.
- **Row isolation** (`_isolate_new_rows_df`, `_isolate_updated_rows_df`) --
  Anti-join and semi-join logic separates rows into INSERT vs UPDATE sets based on
  key columns.
- **API response validation** -- BLS responses are checked for
  `status == "REQUEST_SUCCEEDED"` before parsing; failures raise an exception.
- **Environment variable validation** (`utils.load_env_variables`) -- Raises
  `ValueError` immediately if required keys (`FMP_API_KEY`, `BLS_API_KEY`, DB
  connection params) are missing.

## CI/CD

GitHub Actions runs on every push and pull request to `main`
(`.github/workflows/ci.yml`):

### Lint Job
- Checks code style with **ruff** (`ruff check` and `ruff format --check`) across
  `src/` and `tests/`.

### Unit Tests Job
- Installs test dependencies with `uv sync --group test`
- Runs `uv run pytest -m unit --cov=pfin_back_etl --cov-report=term-missing`
- No credentials or external services required.

### Integration Tests (local only)
Integration tests are **not** run in CI. They require a `.env` with valid database
and API credentials and write to the production database. Run them locally:

```bash
uv run pytest -m integration
```

## Docker

Build and run the production ETL job:

```bash
docker compose up --build
```

The container installs dependencies from the lockfile (`uv sync --frozen`), installs
the package in editable mode, and runs `main.py` as the entrypoint.

## Usage
- Run Tests: `uv run pytest` (see Testing section above for more options)
- Run ETL: `uv run python main.py`
- Docker: `docker compose up --build`
- Lint: `uv run ruff check src/ tests/`
- Format check: `uv run ruff format --check src/ tests/`

## Contributing
... Just me so far...

## License
MIT License

## Contact
TBD
