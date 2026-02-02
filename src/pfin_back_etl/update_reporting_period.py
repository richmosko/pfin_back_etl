"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Update the pfin.reporting_period table

    This code was cloned and modified from fmp-stable-api as a
    starting point. 

    Full disclosure... I am NOT a pofessional sotware developer, so
    this might be a bit janky. Please be kind.
"""

# library imports
import utils
import fmpstab
from datetime import date
import sqlalchemy as sqla
import pandas as pd

# Load environment variables from local .env file
params = utils.load_env_variables()

# FMP:: Initialize client with your API Key
fmp_client = fmpstab.FMPStab(api_key=params['FMP_KEY_VALUE'], log_enabled=False)

# SBASE:: Try to establish a connection to the postgresql database
DB_NAME = params['PFIN_DB_NAME']
DB_HOST = params['PFIN_DB_HOST']
DB_PORT = params['PFIN_DB_PORT']
DB_USER = params['PFIN_DB_USER']
DB_PASSWORD = params['PFIN_DB_PASSWORD']
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DATABASE_URL += f"?sslmode=require"

(engine, metadata, Base) = utils.sbase_setup(DATABASE_URL)

# Access the generated classes
print('\n' + '==== ' * 16)
print(f"USERS SCHEMA:          {Base.by_module.auth.users.__table__.schema}")
print(f"ACCOUNT_TYPE SCHEMA:   {Base.by_module.pfin.account_type.__table__.schema}")
print(f"ACCOUNT SCHEMA:        {Base.by_module.pfin.account.__table__.schema}")
print(f"EQUITY_PROFILE SCHEMA: {Base.by_module.pfin.equity_profile.__table__.schema}\n")

YEARS_TO_FETCH = 5
PERIODS_TO_FETCH = YEARS_TO_FETCH * 4

with sqla.orm.Session(engine) as session:
    # 1. Fetch what's already in pfin.{table} for later comparison
    print('\n' + '==== ' * 16)
    print(f"Figure out what's already in pfin.reporting_period..")
    tab_sbase = Base.by_module.pfin.reporting_period
    df_sbase = utils.fetch_table_df(session, tab_sbase)
    print(df_sbase)

    # 2. Generate list of symbols to work on
    print('\n' + '==== ' * 8)
    print(f"Generating a set of symbols to fetch from FMP...")
    asset_map = utils.fetch_asset_map(session, Base)
    id_list = list(asset_map.values())
    sym_list = list(asset_map.keys())
    print(asset_map)

    # 3. Fetch the stock income_statement data from FMP
    print('\n' + '==== ' * 8)
    print(f"Fetching data from Financial Modeling Prep...")
    df_fmp = utils.fetch_fmp_list_df(fmp_client.income_statement, 'symbol',
                               symbol=sym_list, limit=PERIODS_TO_FETCH, period='quarter')
    df_fmp.rename(columns={'symbol': 'asset_id'}, inplace=True)
    df_fmp.rename(columns={'date': 'end_date'}, inplace=True)
    df_fmp['asset_id'] = df_fmp['asset_id'].map(asset_map)
    df_fmp['filing_date'] = pd.to_datetime(df_fmp['filing_date'])
    df_fmp['filing_date'] = df_fmp['filing_date'].dt.date
    df_fmp['fiscal_year'] = df_fmp['fiscal_year'].astype('int64') # part of unique constraint...
    print(df_fmp)

    # 4. Search for future asset reporting periods and add any missing ones
    print('\n' + '==== ' * 8)
    print(f"Create generic 'future' reporting periods for EPS & Rev estimates...")
    tmp_date_now = pd.to_datetime(date.today(), utc=True)
    tmp_date_fut = pd.to_datetime('4000-12-31', utc=True)
    tmp_year_fut = 4000
    tmp_period_fut = 'NA'

    for asset_id in id_list:
        new_row = {'asset_id': asset_id,
                   'filing_date': tmp_date_fut,
                   'accepted_date': tmp_date_fut,
                   'fiscal_year': tmp_year_fut,
                   'period': tmp_period_fut}
        df_fmp.loc[len(df_fmp)] = new_row
    df_fmp = df_fmp.fillna(None)

    # 5. Find the common columns to populate in the DB table
    print('\n' + '==== ' * 8)
    print(f"Merging columns to (inner join) to limit what gets sent to DB...")
    tab_sbase = Base.by_module.pfin.reporting_period
    (common_cols, df_old, df_new) = utils.df_calc_common_cols(tab_sbase, df_sbase, df_fmp)
    print(common_cols)

    # 6. Isolate new row(s) to insert
    print('\n' + '==== ' * 8)
    print(f"Determining entries to insert...")
    key_list = ['asset_id', 'fiscal_year', 'period']
    df_insert = utils.df_isolate_new_rows(key_list, df_old, df_new)
    print(df_insert)

    # 7. Isolate existing row(s) to update
    print('\n' + '==== ' * 8)
    print(f"Determining entries to update...")
    df_update = df_old
    df_update['id'] = df_sbase['id'] # [richmosko]: ensure primary key present
    print(df_update)

with sqla.orm.Session(engine) as session:
    utils.df_insert_table(session, tab_sbase, df_insert)

with sqla.orm.Session(engine) as session:
    utils.df_update_table(session, metadata, tab_sbase, 'id', df_update)

