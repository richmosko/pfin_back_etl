"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Update the CPI table in supabase

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

print('\n' + '==== ' * 16)
print(f"Fetch current CPI data from the BLS...")
current_year = date.today().year

df_fmp = utils.fetch_cpi_df('2017', current_year, ['CUUR0000SA0'])
#df_fmp = fetch_cpi('2022', '2026', ['CUUR0000SA0','SUUR0000SA0'])
df_fmp['series_name'] = 'cpi-u'

with sqla.orm.Session(engine) as session:
    # 1. Fetch what's already in pfin.cpi for later comparison
    print('\n' + '==== ' * 16)
    print(f"Figure out what's already in pfin.cpi...")
    df_sbase = utils.fetch_table_df(session, Base.by_module.pfin.cpi)
    print(df_sbase)

    # 2. Find the common columns to populate in the cpi DB table
    print('\n' + '==== ' * 8)
    print(f"Merging columns to (inner join) to limit what gets sent to DB...")
    tab_sbase = Base.by_module.pfin.cpi
    (common_cols, df_old, df_new) = utils.df_calc_common_cols(tab_sbase, df_sbase, df_fmp)
    print(common_cols)

    # 3. Isolate new cpi(s) to insert
    print('\n' + '==== ' * 8)
    print(f"Determining entries to insert...")
    key_list = ['year', 'month']
    df_insert = utils.df_isolate_new_rows(key_list, df_old, df_new)
    print(df_insert)

    # 4. Isolate existing cpi(s) to update
    print('\n' + '==== ' * 8)
    print(f"Determining entries to update...")
    df_update = df_old
    df_update['id'] = df_sbase['id'] # [richmosko]: ensure primary key present
    print(df_update)

with sqla.orm.Session(engine) as session:
    utils.df_insert_table(session, tab_sbase, df_insert)

with sqla.orm.Session(engine) as session:
    utils.df_update_table(session, metadata, tab_sbase, 'id', df_update)
