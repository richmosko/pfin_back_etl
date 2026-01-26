"""
Project:       pfin_etlback
Author:        Rich Mosko

Description:
    Update the pfin.balance_sheet_statement table

    This code was cloned and modified from fmp-stable-api as a
    starting point. 

    Full disclosure... I am NOT a pofessional sotware developer, so
    this might be a bit janky. Please be kind.
"""

# library imports
import os
import dotenv
import datetime as dt
import re
import requests
from datetime import date
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
#engine = sqla.create_engine(DATABASE_URL, poolclass=sqla.pool.NullPool, echo=True)

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


print(f'\n====    ====    ====    ====    ====    ====    ====')
with sqla.orm.Session(engine) as session:
    # A. Fetch what's already in pfin.reporting_period for later comparison
    print(f"Figure out what's already in pfin.reporting_period...")
    tab = Base.by_module.pfin.reporting_period.__table__
    stmt = sqla.select(tab)
    ldict = fetch_sbase_ldict(session, stmt)
    df_rp = ldict_to_df(ldict, tab)
    df_rp_map = df_rp[['id', 'asset_id', 'filing_date']]
    print(df_rp_map)

    # B. Generate list of symbols to work on
    print(f"Generating a set of balance_sheet_statements to fetch from FMP...")
    t_asset = Base.by_module.pfin.asset
    t_asset_cat = Base.by_module.pfin.asset_cat
    stmt = sqla.select(t_asset.symbol, t_asset.id
              ).join(t_asset_cat
              ).where(t_asset_cat.cat == 'Equity'
              ).where(t_asset.has_financials==True)
    ldict = fetch_sbase_ldict(session, stmt)
    id_list = [d['id'] for d in ldict]
    sym_list = [d['symbol'] for d in ldict]
    asset_map = {}
    for item in ldict:
        sym = item['symbol']
        xid = item['id']
        asset_map[sym] = xid
    print(asset_map)

    # 1. Fetch what's already in pfin.balance_sheet_statement for later comparison
    print(f"Figure out what's already in pfin.balance_sheet_statement...")
    tab_sbase = Base.by_module.pfin.balance_sheet_statement
    tab = tab_sbase.__table__
    stmt = sqla.select(tab)
    ldict = fetch_sbase_ldict(session, stmt)
    df_sbase = ldict_to_df(ldict, tab)
    print(df_sbase)

    # 2. Fetch the stock balance_sheet_statement data from FMP
    print(f"Fetching balance_sheet_statement data from Financial Modeling Prep...")
    df_fmp = fetch_fmp_list_df(fmp_client.balance_sheet_statement, 'symbol',
                               symbol=sym_list, limit=20, period='quarter')
    df_fmp.rename(columns={'symbol': 'asset_id'}, inplace=True)
    df_fmp.rename(columns={'date': 'end_date'}, inplace=True)
    df_fmp['asset_id'] = df_fmp['asset_id'].map(asset_map)
    df_fmp['filing_date'] = pd.to_datetime(df_fmp['filing_date'])
    df_fmp['filing_date'] = df_fmp['filing_date'].dt.date
    uq_cols = ['asset_id', 'filing_date']
    df_fmp = pd.merge(df_fmp, df_rp_map, on=uq_cols, how='inner')
    df_fmp = df_fmp.drop(columns=uq_cols)
    df_fmp.rename(columns={'id': 'reporting_period_id'}, inplace=True)
    print(f"Set of entries to insert/update in pfin.reporting_period:")
    print(df_fmp['reporting_period_id'].to_list())

    # 3. Find the common columns to populate in the balance_sheet_statement DB table
    print(f"Merging columns (inner join) to limit what gets sent to DB...")
    t_sbase = Base.by_module.pfin.balance_sheet_statement
    sb_cols = t_sbase.__table__.columns.keys()
    fmp_cols = set(list(df_fmp.columns))
    common_cols = [item for item in sb_cols if item in fmp_cols]
    print(common_cols)
    df_new = pd.DataFrame(columns=common_cols) # initialze empty DF
    df_old = pd.DataFrame(columns=common_cols) # initialze empty DF
    for col in common_cols:
        df_new[col] = df_fmp[col]
        df_old[col] = df_sbase[col]
    print(common_cols)

    # 4. Isolate new balance_sheet_statement(s) to insert
    print(f"Determining entries to insert...")
    key_list = 'reporting_period_id'
    df_insert = df_merge_common_cols(key_list, df_old, df_new)
    print(df_insert)

    # 5. Isolate existing balance_sheet_statement(s) to update
    print(f"Determining entries to update...")
    df_update = df_old
    # [richmosko]: primary key already present in FK reporting_period_id
    #df_update['id'] = df_sbase['id'] # [richmosko]: ensure primary key present
    print(df_update)

with sqla.orm.Session(engine) as session:
    # 7. Insert new entries
    print(f"Inserting new entries...")
    ldict_insert = df_insert.to_dict('records')
    if ldict_insert:
        stmt = sqla.insert(tab_sbase)
        session.execute(stmt, ldict_insert)
        session.commit()


with sqla.orm.Session(engine) as session:
    result = session.execute(sqla.text("DISCARD TEMPORARY"))
    session.commit()

    # 8. Update existing entries
    print(f"Updating existing entries...")
    ldict_update = df_update.to_dict('records')
    if ldict_update:
        staging_update(session, metadata, tab_sbase,
                       key_list, ldict_update)

