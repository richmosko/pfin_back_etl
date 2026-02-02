"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Update the pfin.eod_price table

    This code was cloned and modified from fmp-stable-api as a
    starting point. 

    Full disclosure... I am NOT a pofessional sotware developer, so
    this might be a bit janky. Please be kind.
"""

# library imports
import utils
import fmpstab
from datetime import date, datetime, timedelta
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
DAYS_TO_FETCH = YEARS_TO_FETCH * 365

with sqla.orm.Session(engine) as session:
    # 1. Fetch what's already in pfin.eod_price for later comparison
    print('\n' + '==== ' * 16)
    print(f"Figure out what's already in pfin.eod_price...")
    tab_sbase = Base.by_module.pfin.eod_price
    df_sbase = utils.fetch_table_df(session, tab_sbase)
    print(df_sbase)

    # 2. Generate list of symbols to work on
    print('\n' + '==== ' * 8)
    print(f"Generating a set of symbols to fetch from FMP...")
    asset_map = utils.fetch_asset_map(session, Base)
    id_list = list(asset_map.values())
    sym_list = list(asset_map.keys())
    print(asset_map)

    # 3. Fetch the eod_price(historical-eod/full) data from FMP
    print('\n' + '==== ' * 8)
    print(f"Fetching EOD historical data from Financial Modeling Prep...")
    date_5y_ago = datetime.now() - timedelta(days=DAYS_TO_FETCH)
    date_5y_ago = date_5y_ago.strftime('%Y-%m-%d')
    df_fmp = utils.fetch_fmp_list_df(fmp_client.historical_full, 'symbol',
                                     symbol=sym_list, start_date=date_5y_ago)
    df_fmp.rename(columns={'symbol': 'asset_id'}, inplace=True)
    df_fmp.rename(columns={'date': 'end_date'}, inplace=True)
    df_fmp['asset_id'] = df_fmp['asset_id'].map(asset_map)
    df_fmp['end_date'] = pd.to_datetime(df_fmp['end_date'])
    df_fmp['end_date'] = df_fmp['end_date'].dt.date
    print(df_fmp)

    # 4. Find the common columns to populate in the eod_price DB table
    print('\n' + '==== ' * 8)
    print(f"Merging columns to (inner join) to limit what gets sent to DB...")
    tab_sbase = Base.by_module.pfin.eod_price
    (common_cols, df_old, df_new) = utils.df_calc_common_cols(tab_sbase, df_sbase, df_fmp)
    print(common_cols)

    # 5. Isolate new eod_price(s) to insert
    print('\n' + '==== ' * 8)
    print(f"Determining entries to insert...")
    key_list = ['asset_id', 'end_date']
    df_insert = utils.df_isolate_new_rows(key_list, df_old, df_new)
    print(df_insert)

    # 6. Isolate existing eod_price(s) to update
    print('\n' + '==== ' * 8)
    print(f"Determining entries to update...")
    df_update = df_old
    df_update['id'] = df_sbase['id'] # [richmosko]: ensure primary key present
    print(df_update)

with sqla.orm.Session(engine) as session:
    utils.df_insert_table(session, tab_sbase, df_insert)

with sqla.orm.Session(engine) as session:
    utils.df_update_table(session, metadata, tab_sbase, key_list, df_update)

