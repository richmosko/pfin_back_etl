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

### A Valid Financial Modeling Prep API Key
This is what I'm using as a stock financials data source. Other sources could work
perfectly well with some modification... but this is what I'm currently using. The
starter plan supplies most of the data for 2-years of historical data and a pretty
reasonable price. TBD: I should look at what kind of features should be disables if
the the free plan was used instead.
TBD: LINK_TO_FIANACIAL_MODELING_PREP_PLANS

### A BLS API Key
The U.S. Brureau of Labor & Statistics requires regustration for an API key (free)
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
1. Intall uv
2. Clone the project
3. Navigate to your cloned repository... ie: `cd <Project Directory>/pfin_back_etl`
4. Initialize uv and the environemnt: `uv sync`

## Usage
- Run Tests: `uv run pytest`
- Other Stuff: TBD

## Contributing
... Just me so far...

## License
MIT License

## Contact
TBD
