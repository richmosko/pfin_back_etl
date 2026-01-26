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
import os
import dotenv
import datetime as dt
from datetime import date
import re
import requests
import psycopg2
import sqlalchemy as sqla
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.automap import automap_base
import pandas as pd
import fmpstab


# This should get moved to the common library/Package
def col_to_snake(col_list):
    col_dict = {}
    for col in col_list:
        col_dict[col] = re.sub(r"([a-z])([A-Z])", r"\1_\2", col).lower()
    return col_dict

def fetch_sbase_ldict(session, stmt):
    result = session.execute(stmt)
    ldict = []
    for row in result:
        row_as_dict = row._asdict()
        ldict.append(row_as_dict)
    return ldict

def ldict_to_df(ldict, tab):
    cols = tab.columns.keys()
    df = pd.DataFrame(columns=cols) if not ldict else pd.DataFrame(ldict)
    return df

def fetch_fmp_df(fmp_func, **kwargs):
    fmp_api_name = fmp_func.__name__
    print(f"    FMP ({fmp_api_name}): Fetching {kwargs} ...")
    rsp = fmp_func(**kwargs)
    df = pd.DataFrame(rsp.json())
    df.rename(columns=col_to_snake(df.columns.to_list()), inplace=True)
    return df

def fetch_fmp_list_df(fmp_func, key, **kwargs):
    fmp_api_name = fmp_func.__name__
    key_list = kwargs.pop(key)
    if not isinstance(key_list, list):
        key_list = [key_list]

    df_list = []
    for item in key_list:
        kwargs[key] = item
        df_list.append(fetch_fmp_df(fmp_func, **kwargs))
    df_fmp = pd.concat(df_list, axis=0)
    return df_fmp

def df_merge_common_cols(on_key, df_old, df_new):
    df_mrg = pd.merge(df_new, df_old,
                      on=on_key, how='outer',
                      suffixes=('', '_y'), indicator=True)
    df_mrg = df_mrg[df_mrg['_merge'] == 'left_only']
    df_mrg = df_mrg[df_new.columns]
    return df_mrg

def module_name_for_table(tablename, declarativetable, reflecttable):
    if reflecttable.schema:
        # e.g., returns "mymodules.schema_a"
        #print(reflecttable.schema)
        return reflecttable.schema
    else:
        # Default module name if no schema is present
        return "default"

def resolve_referred_schema(table, to_metadata, constraint, referred_schema):
    """
    Dynamically determines the target schema for a foreign key reference.
    """
    if referred_schema == 'source_schema':
        return 'target_schema' # Map 'source_schema' to 'target_schema'
    return referred_schema

def staging_update(session, metadata, tab_sbase, key_list, ldict_update):
    if not isinstance(key_list, list):
        key_list = [key_list]

    result = session.execute(sqla.text("DISCARD TEMPORARY"))
    session.commit()

    tab_stag = tab_sbase.__table__.to_metadata(metadata,
                                               name='table_staging',
                                               schema=None,
                                               referred_schema_fn=resolve_referred_schema)
    tab_stag._prefixes.append("TEMP")
    tab_stag.constraints = set()
    tab_stag.foreign_keys = set()

    tg_name = tab_sbase.__table__.name
    st_name = tab_stag.name
    stmt = sqla.text(f"""CREATE TEMP TABLE {st_name} AS
                         SELECT * FROM pfin.{tg_name};""")
    result = session.execute(stmt)

    stmt = sqla.insert(tab_stag)
    session.execute(stmt, ldict_update)

    # SQL statement to update from staging table
    t_tab_name = tab_sbase.__table__.name
    s_tab_name = tab_stag.name

    ud_stmt = f"""UPDATE pfin.{tg_name} as TG"""
    ud_stmt += f"""\nSET"""
    set_list = []
    for column in tab_sbase.__table__.columns:
        if column.name not in key_list:
            set_list.append(f"""\n{column.name} = ST.{column.name}""")
    ud_stmt += ", ".join(set_list)
    ud_stmt += f"""\nFROM {st_name} as ST"""
    ud_stmt += f"""\nWHERE """
    cond_list = []
    for key_col in key_list:
        cond_list.append(f"""TG.{key_col}=ST.{key_col}""")
    ud_stmt += " AND ".join(cond_list)
    ud_stmt += ';'
    stmt = sqla.text(ud_stmt)
    #print(stmt)
    result = session.execute(stmt)
    session.commit()


# Load environment variables from local .env file
dotenv.load_dotenv()

# Check for API Key in FMP_API_KEY env variable and stores it if not found
key_name = 'FMP_API_KEY'
key_value = os.getenv(key_name)
if key_value is not None:
    print(f"{key_name} value found...")
else:
    raise ValueError(f"Environment variable {key_name} does not exist. The .env file should be updated.")

# Fetch other env variables
DB_USER = os.getenv('PFIN_DB_USER')
DB_PASSWORD = os.getenv('PFIN_DB_PASSWORD')
DB_HOST = os.getenv('PFIN_DB_HOST')
DB_PORT = os.getenv('PFIN_DB_PORT')
DB_NAME = os.getenv('PFIN_DB_NAME')

# FMP:: Initialize client with your API Key
fmp_client = fmpstab.FMPStab(api_key=key_value, log_enabled=False)

# SBASE:: Try to establish a connection to the postgresql database
# 1. Construct the SQLAlchemy connection string and setup the engine
print(f'\n====    ====    ====    ====    ====    ====    ====')
print(f"Setting up sqlalchemy engine...")
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
engine = sqla.create_engine(DATABASE_URL, poolclass=sqla.pool.NullPool)

# 2. Create the Automap Base, linking to your engine's metadata
print(f"Initializing sqlalchemy MetaData object...")
metadata = sqla.MetaData()
Base = sqla.ext.automap.automap_base(metadata=metadata)

# 3. Reflect tables from each schema into the *same* metadata object
print(f"Reflect database tables to sqlalchemy MetaData object...")
metadata.reflect(bind=engine, schema='auth')
metadata.reflect(bind=engine, schema='pfin')

# 4. Prepare the Automap base
print(f"Automapping DB tables to sqlalchemy base object...")
Base.prepare(autoload_with=engine, modulename_for_table=module_name_for_table)

# Access the generated classes
print(f'\n====    ====    ====    ====    ====    ====    ====')
print(f"USERS SCHEMA:          {Base.by_module.auth.users.__table__.schema}")
print(f"ACCOUNT_TYPE SCHEMA:   {Base.by_module.pfin.account_type.__table__.schema}")
print(f"ACCOUNT SCHEMA:        {Base.by_module.pfin.account.__table__.schema}")
print(f"EQUITY_PROFILE SCHEMA: {Base.by_module.pfin.equity_profile.__table__.schema}\n")

# Now let's try to insert into the database from a pandas dataframe...
print(f'\n====    ====    ====    ====    ====    ====    ====')
with sqla.orm.Session(engine) as session:
    # 0. Get what's already in pfin.asset for later comparison
    print(f"Fetching what's already in pfin.asset...")
    tab = Base.by_module.pfin.asset.__table__
    stmt = sqla.select(tab)
    ldict = fetch_sbase_ldict(session, stmt)
    df_sbase = ldict_to_df(ldict, tab)
    print(df_sbase['symbol'].tolist())

    # 1. Figure out the pfin.asset_cat.id to use as default
    print(f"Querying for asset category...")
    tab = Base.by_module.pfin.asset_cat
    stmt = sqla.select(tab.id).where(tab.cat == 'Equity').where(tab.sub_cat == 'UNKNOWN')
    ldict = fetch_sbase_ldict(session, stmt)
    asset_cat_id = ldict[0]['id']
    print(f"pfin.asset_cat.id = {asset_cat_id}\n")

    # 2. Generate a symbol list
    #    This should probably be a collected from FMP's company-screener.
    print(f"TBD:: Generating a symbol list to process...")
    #sym_list = ['NVDA', 'AAPL', 'IREN']
    #sym_list = ['AAPL', 'IREN', 'V', 'ALAB']
    sym_list = ['NVDA', 'AAPL', 'IREN', 'V', 'ALAB', 'APP', 'GOOGL']
    df_slist = fetch_fmp_df(fmp_client.company_screener,
                            marketCapMoreThan=1000000000,
                            country='US',
                            isEtf=False,
                            isFund=False,
                            isActivelyTrading=True,
                            limit=5000)
    print(df_slist)

    # 3. Convert the symbol list to a table with data from FMP->search-symbol
    #    Ensure that the column names are consistant
    print(f"Fetching data from Financial Modeling Prep...")
    df_fmp = fetch_fmp_list_df(fmp_client.search_symbol, 'query', query=sym_list, limit=1)
    df_fmp.rename(columns={'name': 'description'}, inplace=True)
    df_fmp['asset_cat_id'] = asset_cat_id
    df_fmp['has_financials'] = True
    df_fmp['has_chart'] = True

    # 4. Find the common columns to populate in the DB table so that we don't
    #    try to insert into undefined columns.
    print(f"Find the common columns to insert into the database...")
    asset_cols = Base.by_module.pfin.asset.__table__.columns.keys()
    fmp_cols = set(list(df_fmp.columns))
    common_cols = [item for item in asset_cols if item in fmp_cols]
    print(common_cols)
    df_asset = pd.DataFrame(columns=common_cols)
    for col in common_cols:
        df_asset[col] = df_fmp[col]
    #print(df_asset)

    # 5. Isolate only new assets to insert
    print(f"Isolating only new assets to insert...")
    df_insert = df_merge_common_cols('symbol', df_sbase, df_asset)
    print(df_insert['symbol'].tolist())


    # 6. Insert new asset rows into DB table pfin.asset
    print(f"Inserting new assets to pfin.asset...")
    ldict_insert = df_insert.to_dict('records')
    if ldict_insert:
        tab_sbase = Base.by_module.pfin.asset
        stmt = sqla.insert(tab_sbase)
        session.execute(stmt, df_insert.to_dict('records'))
        session.commit()


print(f'\n====    ====    ====    ====    ====    ====    ====')
with sqla.orm.Session(engine) as session:
    # 7. Fetch what's already in pfin.equity_profile for later comparison
    print(f"Figure out what's already in pfin.equity_profile...")
    tab = Base.by_module.pfin.equity_profile.__table__
    stmt = sqla.select(tab)
    ldict = fetch_sbase_ldict(session, stmt)
    df_sbase = ldict_to_df(ldict, tab)
    print(df_sbase)

    # 8. Generate list of symbols to work on
    print(f"Generating set of symbol profiles to fetch from FMP...")
    t_asset = Base.by_module.pfin.asset
    t_asset_cat = Base.by_module.pfin.asset_cat
    stmt = sqla.select(t_asset.symbol, t_asset.id
              ).join(t_asset_cat
              ).where(t_asset_cat.cat == 'Equity'
              ).where(t_asset.has_financials==True)
    ldict = fetch_sbase_ldict(session, stmt)
    id_list = [d['id'] for d in ldict]
    sym_list = [d['symbol'] for d in ldict]
    print(sym_list)

    # 9. Fetch the stock profile data from FMP
    print(f"Fetching data from Financial Modeling Prep...")
    df_fmp = fetch_fmp_list_df(fmp_client.profile, 'symbol', symbol=sym_list, limit=1)
    df_fmp.rename(columns={'symbol': 'asset_id'}, inplace=True)
    df_fmp['asset_id'] = id_list

    print(f"Set of entries to insert/update in pfin.equity_profile:")
    print(df_fmp['asset_id'].tolist())

    # 10. Find the common columns to populate in the equity_profile DB table
    print(f"Merging columns to (inner join) to limit what gets sent to DB...")
    tab_sbase = Base.by_module.pfin.equity_profile
    sb_cols = tab_sbase.__table__.columns.keys()
    fmp_cols = set(list(df_fmp.columns))
    common_cols = [item for item in sb_cols if item in fmp_cols]
    df_new = pd.DataFrame(columns=common_cols) # initialze empty DF
    df_old = pd.DataFrame(columns=common_cols) # initialze empty DF
    for col in common_cols:
        df_new[col] = df_fmp[col]
        df_old[col] = df_sbase[col]
    #print(common_cols)

    # 11. Isolate new equity_profile(s) to insert
    print(f"Determining entries to insert...")
    key_list = 'asset_id'
    df_insert = df_merge_common_cols(key_list, df_old, df_new)
    print(df_insert['asset_id'].tolist())

    # 12. Isolate existing equity_profile(s) to update
    print(f"Determining entries to update...")
    df_update = df_old
    # [richmosko]: primary key already present in FK asset_id
    print(df_update['asset_id'].tolist())


with sqla.orm.Session(engine) as session:
    # 13. Insert new entries
    print(f"Inserting new entries...")
    ldict_insert = df_insert.to_dict('records')
    if ldict_insert:
        stmt = sqla.insert(tab_sbase)
        session.execute(stmt, ldict_insert)
        session.commit()

with sqla.orm.Session(engine) as session:
    # 14. Update existing entries
    print(f"Updating existing entries...")
    ldict_update = df_update.to_dict('records')
    if ldict_update:
        staging_update(session, metadata, tab_sbase,
                       key_list, ldict_update)

