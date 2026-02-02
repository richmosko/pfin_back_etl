"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Update the pfin.asset table
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

with sqla.orm.Session(engine) as session:
    # 1. Fetch what's already in pfin.asset for later comparison
    print('\n' + '==== ' * 16)
    print(f"Figure out what's already in pfin.asset...")
    df_sbase = utils.fetch_table_df(session, Base.by_module.pfin.asset)
    print(df_sbase['symbol'].tolist())

    # 2. Figure out the pfin.asset_cat.id to use as default
    print('\n' + '==== ' * 8)
    print(f"Querying for asset category...")
    tab = Base.by_module.pfin.asset_cat
    stmt = sqla.select(tab.id).where(tab.cat == 'Equity').where(tab.sub_cat == 'UNKNOWN')
    ldict = utils.fetch_sbase_ldict(session, stmt)
    asset_cat_id = ldict[0]['id']
    print(f"pfin.asset_cat.id = {asset_cat_id}\n")

    # 3. Generate a symbol list
    #    This should probably be a collected from FMP's company-screener.
    print('\n' + '==== ' * 8)
    print(f"TBD:: Generating a symbol list to process...")
    #sym_list = ['NVDA', 'AAPL', 'IREN']
    #sym_list = ['AAPL', 'IREN', 'V', 'ALAB']
    sym_list = ['NVDA', 'AAPL', 'IREN', 'V', 'ALAB', 'APP', 'GOOGL', 'META']
    df_slist = utils.fetch_fmp_df(fmp_client.company_screener,
                            marketCapMoreThan=1000000000,
                            country='US',
                            isEtf=False,
                            isFund=False,
                            isActivelyTrading=True,
                            limit=5000)
    print(df_slist)

    # 4. Convert the symbol list to a table with data from FMP->search-symbol
    #    Ensure that the column names are consistant
    print('\n' + '==== ' * 8)
    print(f"Fetching data from Financial Modeling Prep...")
    df_fmp = utils.fetch_fmp_list_df(fmp_client.search_symbol, 'query', query=sym_list, limit=1)
    df_fmp.rename(columns={'name': 'description'}, inplace=True)
    df_fmp['asset_cat_id'] = asset_cat_id
    df_fmp['has_financials'] = True
    df_fmp['has_chart'] = True

    # 5. Find the common columns to populate in the DB table so that we don't
    #    try to insert into undefined columns.
    print('\n' + '==== ' * 8)
    print(f"Merging columns to (inner join) to limit what gets sent to DB...")
    tab_sbase = Base.by_module.pfin.asset
    (common_cols, df_old, df_new) = utils.df_calc_common_cols(tab_sbase, df_sbase, df_fmp)
    print(common_cols)

    # 6. Isolate only new assets to insert
    print('\n' + '==== ' * 8)
    print(f"Determining entries to insert...")
    key_list = ['symbol']
    df_insert = utils.df_isolate_new_rows(key_list, df_old, df_new)
    print(df_insert['symbol'].tolist())

    # 7. Isolate existing assets to update
    print('\n' + '==== ' * 8)
    print(f"Determining entries to update...")
    df_update = df_old
    df_update['id'] = df_sbase['id'] # [richmosko]: ensure primary key present
    print(df_update)

with sqla.orm.Session(engine) as session:
    utils.df_insert_table(session, tab_sbase, df_insert)


with sqla.orm.Session(engine) as session:
    # 1. Fetch what's already in pfin.equity_profile for later comparison
    print('\n' + '==== ' * 16)
    print(f"Figure out what's already in pfin.equity_profile...")
    tab_sbase = Base.by_module.pfin.equity_profile
    df_sbase = utils.fetch_table_df(session, tab_sbase)
    print(df_sbase)

    # 2. Generate list of symbols to work on
    print('\n' + '==== ' * 8)
    print(f"Generating set of symbol profiles to fetch from FMP...")
    asset_map = utils.fetch_asset_map(session, Base)
    id_list = list(asset_map.values())
    sym_list = list(asset_map.keys())
    print(sym_list)

    # 3. Fetch the stock profile data from FMP
    print('\n' + '==== ' * 8)
    print(f"Fetching data from Financial Modeling Prep...")
    key_list = ['symbol']
    df_fmp = utils.fetch_fmp_list_df(fmp_client.profile, 'symbol', symbol=sym_list, limit=1)
    df_fmp.rename(columns={'symbol': 'asset_id'}, inplace=True)
    df_fmp['asset_id'] = id_list
    print(df_fmp['asset_id'].tolist())

    # 4. Find the common columns to populate in the equity_profile DB table
    print('\n' + '==== ' * 8)
    print(f"Merging columns to (inner join) to limit what gets sent to DB...")
    tab_sbase = Base.by_module.pfin.equity_profile
    (common_cols, df_old, df_new) = utils.df_calc_common_cols(tab_sbase, df_sbase, df_fmp)
    print(common_cols)

    # 5. Isolate new equity_profile(s) to insert
    print('\n' + '==== ' * 8)
    print(f"Determining entries to insert...")
    key_list = 'asset_id'
    df_insert = utils.df_isolate_new_rows(key_list, df_old, df_new)
    print(df_insert['asset_id'].tolist())

    # 6. Isolate existing equity_profile(s) to update
    print('\n' + '==== ' * 8)
    print(f"Determining entries to update...")
    df_update = df_old
    # [richmosko]: primary key already present in FK asset_id
    print(df_update['asset_id'].tolist())

with sqla.orm.Session(engine) as session:
    utils.df_insert_table(session, tab_sbase, df_insert)

with sqla.orm.Session(engine) as session:
    utils.df_update_table(session, metadata, tab_sbase, key_list, df_update)

