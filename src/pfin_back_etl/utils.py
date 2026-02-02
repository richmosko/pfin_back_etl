"""
Project:       pfin_back_etl
Author:        Rich Mosko

Description:
    Common utility functions

"""

# library imports
import os
import dotenv
import datetime as dt
from datetime import date
import re
import requests
import json
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
    print(f"    FMP ({fmp_api_name}): Fetching {kwargs} ...", end='')
    rsp = fmp_func(**kwargs)
    df = pd.DataFrame(rsp.json())
    df.rename(columns=col_to_snake(df.columns.to_list()), inplace=True)
    print(f" Got {len(df)} rows")
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

def load_env_variables():
    params = {}

    # Load environment variables from local .env file
    dotenv.load_dotenv()

    # Check for API Key in FMP_API_KEY env variable and stores it if not found
    key_name = 'FMP_API_KEY'
    key_value = os.getenv(key_name)
    params['FMP_KEY_VALUE'] = key_value
    if key_value is not None:
        print(f"{key_name} value found...")
    else:
        raise ValueError(f"Environment variable {key_name} does not exist in .env file.")

    # Fetch other env variables
    params['PFIN_DB_USER'] = os.getenv('PFIN_DB_USER')
    params['PFIN_DB_PASSWORD'] = os.getenv('PFIN_DB_PASSWORD')
    params['PFIN_DB_HOST'] = os.getenv('PFIN_DB_HOST')
    params['PFIN_DB_PORT'] = os.getenv('PFIN_DB_PORT')
    params['PFIN_DB_NAME'] = os.getenv('PFIN_DB_NAME')
    return params


def sbase_setup(DATABASE_URL):
    # SBASE:: Try to establish a connection to the postgresql database
    # 1. Construct the SQLAlchemy connection string and setup the engine
    print(f"Setting up sqlalchemy engine...")
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
    return (engine, metadata, Base)


def fetch_table_df(session, table):
    # Fetch what's already in {table} for later comparison
    tab = table.__table__
    stmt = sqla.select(tab)
    ldict = fetch_sbase_ldict(session, stmt)
    df_tab = ldict_to_df(ldict, tab)
    return df_tab

def fetch_asset_map(session, Base):
    # B. Generate list of symbols to work on
    print(f"Generating a set of income_statements to fetch from FMP...")
    tab_asset = Base.by_module.pfin.asset
    tab_asset_cat = Base.by_module.pfin.asset_cat
    stmt = sqla.select(tab_asset.symbol, tab_asset.id
              ).join(tab_asset_cat
              ).where(tab_asset_cat.cat == 'Equity'
              ).where(tab_asset.has_financials==True)
    ldict = fetch_sbase_ldict(session, stmt)
    id_list = [d['id'] for d in ldict]
    sym_list = [d['symbol'] for d in ldict]
    asset_map = {}
    for item in ldict:
        sym = item['symbol']
        xid = item['id']
        asset_map[sym] = xid
    return asset_map

def df_calc_common_cols(tab_sbase, df_sbase, df_fmp):
    # Find the common columns to populate in the DB table
    sb_cols = tab_sbase.__table__.columns.keys()
    fmp_cols = set(list(df_fmp.columns))
    common_cols = [item for item in sb_cols if item in fmp_cols]
    df_new = pd.DataFrame(columns=common_cols) # initialze empty DF
    df_old = pd.DataFrame(columns=common_cols) # initialze empty DF
    for col in common_cols:
        df_new[col] = df_fmp[col]
        df_old[col] = df_sbase[col]
    return (common_cols, df_old, df_new)

def df_isolate_new_rows(on_key, df_old, df_new):
    df_mrg = pd.merge(df_new, df_old,
                      on=on_key, how='outer',
                      suffixes=('', '_y'), indicator=True)
    df_mrg = df_mrg[df_mrg['_merge'] == 'left_only']
    df_mrg = df_mrg[df_new.columns]
    return df_mrg

def df_insert_table(session, tab_sbase, df_insert):
    print(f"Inserting new entries...")
    ldict_insert = df_insert.to_dict('records')
    if ldict_insert:
        stmt = sqla.insert(tab_sbase)
        session.execute(stmt, ldict_insert)
        session.commit()

def df_update_table(session, metadata, tab_sbase, key_list, df_update):
    print(f"Updating existing entries...")
    ldict_update = df_update.to_dict('records')
    if ldict_update:
        staging_update(session, metadata, tab_sbase,
                       key_list, ldict_update)
        session.commit()

def fetch_cpi_df (startyear, endyear, series_id_lst):
    headers = {'Content-type': 'application/json'}
    data = json.dumps({"seriesid": series_id_lst,
                       "startyear":startyear,
                       "endyear":endyear})
    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/',
                      data=data, headers=headers)
    json_data = json.loads(p.text)

    if json_data['status'] != 'REQUEST_SUCCEEDED':
        raise Exception('BLS CPI fetch request unsuccessful')

    df_list = []
    for series in json_data['Results']['series']:
        df = pd.DataFrame(series['data'])
        df.rename(columns=col_to_snake(df.columns.to_list()), inplace=True)
        df['series_id'] = series['seriesID']
        df.rename(columns={'period': 'month'}, inplace=True)
        df['month'] = pd.to_numeric(df['month'].str.lstrip('M'))
        df['year'] = pd.to_numeric(df['year'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df = df.dropna(subset=['value'])
        df.rename(columns={'value': 'series_value'}, inplace=True)
        df.drop('footnotes', axis=1, inplace=True)
        df['ref_date'] = df['year'].astype(str) + '-' + df['month'].astype(str) + '-14'
        df_list.append(df)
    df_cpi = pd.concat(df_list, ignore_index=True)
    print(df_cpi)
    return df_cpi

