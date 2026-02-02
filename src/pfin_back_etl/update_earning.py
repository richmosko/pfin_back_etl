"""
Project:       pfin_back_etl
Author:        Rich Mosko

Description:
    Update the pfin.earning table which tracks reported revenue, earnings, and estimates

    This code was cloned and modified from fmp-stable-api as a
    starting point. 

    Full disclosure... I am NOT a pofessional sotware developer, so
    this might be a bit janky. Please be kind.
"""

# library imports
import utils
import fmpstab
from datetime import date, timedelta
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
    # A. Fetch what's already in pfin.repprting_period for later comparison
    print('\n' + '==== ' * 16)
    print(f"Figure out what's already in pfin.reporting_period...")
    tab_sbase = Base.by_module.pfin.reporting_period
    df_rp = utils.fetch_table_df(session, tab_sbase)
    df_rp['filing_date'] = pd.to_datetime(df_rp['filing_date'], utc=True).astype('datetime64[us, UTC]')
    df_rp_map = df_rp[['id', 'asset_id', 'filing_date']]
    #df_rp_map.rename(columns={'id': 'reporting_period_id'}, inplace=True)
    print(df_rp_map)

    # B. Find the latest (non-future) date for each asset_id
    print(f"Find the latest (current) report date for each asset_id in pfin.reporting_period...")
    asset_id_list = df_rp_map['asset_id'].unique().tolist()
    latest_rpt = {}
    for asset_id in asset_id_list:
        df_tmp = df_rp_map[df_rp_map['asset_id']==asset_id]
        df_tmp = df_tmp.sort_values(by='filing_date', ascending=False)
        # [richmosko]: skip the 1st date which is reserved for future estimates...
        latest_rpt[asset_id] = df_tmp.iat[1, df_tmp.columns.get_loc('filing_date')]
    print(latest_rpt)

    # 1. Fetch what's already in pfin.{table} for later comparison
    print('\n' + '==== ' * 8)
    print(f"Figure out what's already in pfin.earning...")
    tab_sbase = Base.by_module.pfin.earning
    df_sbase = utils.fetch_table_df(session, tab_sbase)
    print(df_sbase)

    # 2. Generate list of symbols to work on
    print('\n' + '==== ' * 8)
    print(f"Generating a set of symbols to fetch from FMP...")
    asset_map = utils.fetch_asset_map(session, Base)
    id_list = list(asset_map.values())
    sym_list = list(asset_map.keys())
    print(asset_map)

    test_id = 4
    #print(df_rp_map[df_rp_map['asset_id']==test_id])

    # 3. Fetch the stock earning(s) data from FMP
    #    First entry typically is just a date and should be filtered
    print('\n' + '==== ' * 8)
    print(f"Fetching earning data from Financial Modeling Prep...")
    df_fmp = utils.fetch_fmp_list_df(fmp_client.earnings, 'symbol',
                                     symbol=sym_list, limit=(PERIODS_TO_FETCH+2))
    df_fmp = df_fmp.dropna(subset=['revenue_actual', 'revenue_estimated',], how='all')
    df_fmp.rename(columns={'symbol': 'asset_id'}, inplace=True)
    df_fmp.rename(columns={'date': 'filing_date'}, inplace=True)
    df_fmp['asset_id'] = df_fmp['asset_id'].map(asset_map)
    df_fmp['filing_date'] = pd.to_datetime(df_fmp['filing_date'])
    df_fmp['filing_date'] = df_fmp['filing_date'].dt.tz_localize('UTC')
    df_fmp['ref_date'] = df_fmp['filing_date'].dt.date
    df_fmp['reporting_period_id'] = None
    print(df_fmp)
    #print(df_fmp[df_fmp['asset_id']==test_id])

    # 4. Match earnings reports to posted reporting_period ids
    print('\n' + '==== ' * 8)
    print(f"Match earnings reports to posted reporting_periods...")
    df_rp_map = df_rp_map.sort_values(by='filing_date', ascending=False).reset_index(drop=True)
    df_fmp = df_fmp.sort_values(by='filing_date', ascending=False).reset_index(drop=True)

    fmp_drop_list = []
    for asset_id in id_list:
        cond_fmp_asset_id = (df_fmp['asset_id']==asset_id)
        cond_rpm_asset_id = (df_rp_map['asset_id']==asset_id)
        cond_fmp_filing_date = (df_fmp['filing_date']<=latest_rpt[asset_id]+timedelta(weeks=2))
        cond_rpm_filing_date = (df_rp_map['filing_date']<=latest_rpt[asset_id]+timedelta(weeks=2))
        fmp_idx = df_fmp.index[cond_fmp_asset_id & cond_fmp_filing_date].tolist()
        rpm_idx = df_rp_map.index[cond_rpm_asset_id & cond_rpm_filing_date].tolist()
        joint_len = min([len(fmp_idx), len(rpm_idx)])
        fmp_drop_list.extend(fmp_idx[joint_len:])
        fmp_idx = fmp_idx[:joint_len]
        rpm_idx = rpm_idx[:joint_len]
        df_fmp.loc[fmp_idx, 'reporting_period_id'] = df_rp_map.loc[rpm_idx, 'id'].to_list()
    print(f"  Dropping long dated reporting_periods: {fmp_drop_list}...")
    df_fmp.drop(fmp_drop_list, inplace=True)

    # 5a. Standardize future earnings estimate filing_date for mapping
    print('\n' + '==== ' * 8)
    print(f"Match future earnings reports to posted reporting_periods...")
    tmp_date_now = pd.to_datetime(date.today(), utc=True)
    tmp_date_fut = pd.to_datetime('4000-12-31', utc=True)
    fmp_idx = df_fmp.index[df_fmp['filing_date'] > tmp_date_now].tolist()
    df_fmp.loc[fmp_idx, 'filing_date'] = tmp_date_fut
    print(fmp_idx)

    # 5b. Map the unique columns to a reporting_period_id
    uq_cols = ['asset_id', 'filing_date']
    df_fmp = pd.merge(df_fmp, df_rp_map, on=uq_cols, how='left')
    df_fmp = df_fmp.drop(columns=uq_cols)
    df_fmp.loc[fmp_idx, 'reporting_period_id'] = df_fmp.loc[fmp_idx, 'id'].astype('int64')
    print(df_fmp)

    # 6. Find the common columns to populate in the estimate DB table
    print('\n' + '==== ' * 8)
    print(f"Merging columns to (inner join) to limit what gets sent to DB...")
    (common_cols, df_old, df_new) = utils.df_calc_common_cols(tab_sbase, df_sbase, df_fmp)
    print(common_cols)

    # 7. Isolate new earning(s) to insert
    print(f"Determining entries to insert...")
    key_list = 'reporting_period_id'
    df_insert = utils.df_isolate_new_rows(key_list, df_old, df_new)
    print(df_insert)

    # 8. Isolate existing earning(s) to update
    print(f"Determining entries to update...")
    df_update = df_old
    # [richmosko]: primary key already present in FK reporting_period_id
    #df_update['id'] = df_sbase['id'] # [richmosko]: ensure primary key present
    print(df_update)

with sqla.orm.Session(engine) as session:
    utils.df_insert_table(session, tab_sbase, df_insert)

with sqla.orm.Session(engine) as session:
    utils.df_update_table(session, metadata, tab_sbase, key_list, df_update)

