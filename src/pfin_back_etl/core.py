"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Define the Classes used for creating the database connections
    and accessing APIs to populate the tables...

    Full disclosure... I am NOT a pofessional sotware developer, so
    this might be a bit janky. Please be kind.
"""

# library imports
import json
import psycopg2
import time
from datetime import date, datetime, timezone, timedelta
import sqlalchemy as sqla
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.automap import automap_base
import polars as pl
import fmpstab
from pfin_back_etl import utils


class PFinFMP(fmpstab.FMPStab):
    """
    Personal Finance Fiancial Modeling Prep Connection
    Child class of FMPStab which adds some fetching functions specific to the
    PFin project.
    """

    def __init__(self, api_key: str) -> None:
        max_calls_per_minute = 280
        config_file = None
        base_url = None
        logger = None
        log_enabled = False
        super().__init__(
            api_key, max_calls_per_minute, config_file, base_url, logger, log_enabled
        )

    def get_screened_stocks(self, min_mkt_cap, result_limit):
        """
        Run FMP company-screener API to get list of stocks to add to assets...

        returns:
            df_slist:      polars dataframe of screened stocks
        """
        df_slist = self.fetch_fmp_df(
            self.company_screener,
            marketCapMoreThan=min_mkt_cap,
            country="US",
            isEtf=False,
            isFund=False,
            isActivelyTrading=True,
            limit=result_limit,
        )
        return df_slist

    def fetch_fmp_list_df(self, fmp_func, key, **kwargs):
        """
        Calls self.fetch_fmp_df multiple times for each item in key(list).
        Concatenates each result into a single polars dataframe

        returns: df_fmp (polars dataframe of query results)
        """
        fmp_api_name = fmp_func.__name__
        key_list = kwargs.pop(key)
        if not isinstance(key_list, list):
            key_list = [key_list]

        df_list = []
        for item in key_list:
            kwargs[key] = item
            df_list.append(self.fetch_fmp_df(fmp_func, **kwargs))
        df_fmp = pl.concat(df_list, how="vertical_relaxed")
        return df_fmp

    def fetch_fmp_df(self, fmp_func, **kwargs):
        """
        fetch data from the Financial Modeling Prep API using the access function
        fmp_func(). specific arguments to that function are passed through kwargs.

        returns: df (polars dataframe of query results)
        """
        fmp_api_name = fmp_func.__name__
        print(f"  FMP ({fmp_api_name}): Fetching {kwargs} ...", end="")
        rsp = fmp_func(**kwargs)
        df = pl.DataFrame(rsp.json())
        df = df.rename(utils.col_to_snake(df.columns))
        print(f" Got {len(df)} row(s)")
        return df


class SBaseConn:
    """
    SupaBase Connection
    Setup and maintain a connection to the SupaBase postgreSQL database.
    Query the connection to discover the relavant tables, and create
    methods to query, insert, and update data.
    """

    def __init__(self, env_prefix, schema_list):
        """
        Class initializer...
        """
        self._env_prefix = env_prefix
        self._schema_list = schema_list
        self._params = utils.load_env_variables(env_prefix)
        (self.engine, self.metadata, self.base) = self._sbase_setup()

    def fetch_table_df(self, table):
        """
        Fetch what's already in {table}
        Args:    table (sqlalchemy ORM table object)
        Returns: df_tab (pandas dataframe of table entries)
        """
        tab = table.__table__
        stmt = sqla.select(tab)
        with sqla.orm.Session(self.engine) as session:
            df_tab = pl.read_database(stmt, session)
        # print(f"self.fetch_table_df():\n {df_tab}")
        return df_tab

    def insert_table_df(self, tab_sbase, df_insert):
        """
        Insert new row entries into table tab_sbase from
        polars dataframe df_insert
        """
        with sqla.orm.Session(self.engine) as session:
            s_name = tab_sbase.__table__.schema
            t_name = tab_sbase.__table__.name
            print(f"Inserting {len(df_insert)} new entries in {s_name}.{t_name}...")
            ldict_insert = df_insert.to_dicts()
            if ldict_insert:
                stmt = sqla.insert(tab_sbase)
                session.execute(stmt, ldict_insert)
                session.commit()

    def update_table_df(self, tab_sbase, key_list, df_update):
        """
        Update existing row entries in table tab_sbase from
        polars dataframe df_update. This will create a temp
        table matching tab_sbase, and insert the rows into the
        temp table. It then updates the data locally in the database
        which executes much faster than a sqlalchemy update command.
        """
        with sqla.orm.Session(self.engine) as session:
            s_name = tab_sbase.__table__.schema
            t_name = tab_sbase.__table__.name
            print(f"Updating {len(df_update)} existing entries in {s_name}.{t_name}...")
            ldict_update = df_update.to_dicts()
            if ldict_update:
                self._staging_update(session, tab_sbase, key_list, ldict_update)
                session.commit()

    def print_schema_info(self):
        """
        Print the schema and table names reflected from supabase
        TBD:: Need to fill this out by polling all the table names per schema
        """
        print(f"Iterating through automapped Classes:")
        schema_list = self.base.by_module.keys()
        for schema in schema_list:
            print("\n" + "==== " * 8)
            print(f"SCHEMA: {schema}")
            tab_list = self.base.by_module[schema].keys()
            for tab in tab_list:
                print(f"    TABLE: {tab}")

    def get_reflected_table(self, schema_name, table_name):
        """
        return the reflected table object based on a schema name
        and table name...

        returns:
            tab:           sqlalchemy ORM Table object
        """
        tab_collection = self.base.by_module[schema_name]
        tab = tab_collection[table_name]
        return tab

    def get_column_dict(self, tab_obj):
        """
        Get a list of the column names in a table

        args:
            tab_obj:       sqlalchemy ORM Table object

        returns:
            keys:          dictionary of column names -> data types
        """
        c_dict = {}
        columns = tab_obj.__table__.columns
        print(f"Table Name: {tab_obj.__table__.schema}.{tab_obj.__table__.name}")
        for column in columns:
            c_dict[column.name] = column.type
            print(
                f"  Column Name: {column.name}, Type: {column.type}[{type(column.type)}]"
            )
        return c_dict

    def _sbase_setup(self):
        """
        Sets up the sqlalchemy engine connection and reflects the database
        structure to self.base object for referencing the database table data

        returns:
            engine:        The connection engine
            metadata:      The database table metadata to define the fields
            base:          The base instance containing the reflected tables
        """
        # SBASE:: Try to establish a connection to the postgresql database
        DB_NAME = self._params["DB_NAME"]
        DB_HOST = self._params["DB_HOST"]
        DB_PORT = self._params["DB_PORT"]
        DB_USER = self._params["DB_USER"]
        DB_PASSWORD = self._params["DB_PASSWORD"]
        DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@"
        DATABASE_URL += f"{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

        # 1. Construct the SQLAlchemy connection string and setup the engine
        print(f"Setting up sqlalchemy engine...")
        engine = sqla.create_engine(DATABASE_URL, poolclass=sqla.pool.NullPool)
        # engine = sqla.create_engine(DATABASE_URL, poolclass=sqla.pool.NullPool, echo=True)

        # 2. Create the Automap Base, linking to your engine's metadata
        print(f"Initializing sqlalchemy MetaData object...")
        metadata = sqla.MetaData()
        base = sqla.ext.automap.automap_base(metadata=metadata)

        # 3. Reflect tables from each schema into the *same* metadata object
        print(f"Reflect database tables to sqlalchemy MetaData object...")
        for sch in self._schema_list:
            metadata.reflect(bind=engine, schema=sch)
            metadata.reflect(bind=engine, schema=sch)

        # 4. Prepare the Automap base
        print(f"Automapping DB tables to sqlalchemy base object...")
        base.prepare(
            autoload_with=engine, modulename_for_table=utils.sqla_modulename_for_table
        )
        return (engine, metadata, base)

    def _staging_update(self, session, tab_sbase, key_list, ldict_update):
        """
        Create a temp staging table, and insert data into table. Updates from temp
        table to the actual target table internally in the database...

        args:
            session:       The active sqlalchemy session
            tab_sbase:     The sqlalchemy table instance to target
            key_list:      list of columns that are unique to key off of
            ldict_update:  list of dictionaries (rows) to update

        returns:
            None
        """
        if not isinstance(key_list, list):
            key_list = [key_list]

        result = session.execute(sqla.text("DISCARD TEMPORARY"))
        session.commit()

        tab_stag = tab_sbase.__table__.to_metadata(
            self.metadata,
            name="table_staging",
            schema=None,
            referred_schema_fn=utils.sqla_resolve_referred_schema,
        )
        tab_stag._prefixes.append("TEMP")
        tab_stag.constraints = set()
        tab_stag.foreign_keys = set()

        tg_name = tab_sbase.__table__.name
        st_name = tab_stag.name
        sch_name = "pfin"
        stmt = sqla.text(f"""CREATE TEMP TABLE {st_name} AS
                             SELECT * FROM {sch_name}.{tg_name};""")
        result = session.execute(stmt)

        stmt = sqla.insert(tab_stag)
        session.execute(stmt, ldict_update)

        # SQL statement to update from staging table
        t_tab_name = tab_sbase.__table__.name
        s_tab_name = tab_stag.name

        ud_stmt = f"""UPDATE {sch_name}.{tg_name} as TG"""
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
        ud_stmt += ";"
        stmt = sqla.text(ud_stmt)
        # print(stmt)
        result = session.execute(stmt)
        session.commit()
        self.metadata.remove(tab_stag)

    def _calc_common_cols_df(self, tab_sbase, df_sbase, df_api):
        """
        Find the common columns to populate in the DB table.
        args:
            tab_sbase:     sqlachemy ORM table object
            df_sbase:      existing table entries as polars dataframe
            df_api:        data from API source as polars dataframe
        returns:
            common_cols:   common columns detected
            df_old:        df_sbase, reformated with common_cols
            df_new:        df_api, reformated with common_cols
        """
        sb_cols = tab_sbase.__table__.columns.keys()
        api_cols = set(list(df_api.columns))
        common_cols = [item for item in sb_cols if item in api_cols]
        # df_new = pd.DataFrame(columns=common_cols) # initialze empty DF
        # df_old = pd.DataFrame(columns=common_cols) # initialze empty DF
        df_new = df_api.select(common_cols)
        df_old = df_sbase.select(common_cols)
        return (common_cols, df_old, df_new)

    def _isolate_new_rows_df(self, on_key, df_old, df_new):
        """
        Compare the existing and new pandas dataframs, and isolate which
        rows are new and should be inserted instead of updated
        args:
            on_key:        list of column names to use for key matching
            df_old:        existing dataframe
            df_new:        dataframe with new and updated entries
        returns:
            df_mrg:        polars dataframe with only the new entries to insert
                           (can be empty dataframe)
        """
        if len(df_old) == 0:
            # special handling of empty table... as data types were not inferred
            return df_new

        df_mrg = df_new.join(df_old, on=on_key, how="anti")
        df_mrg = utils.apply_schema_df(df_old, df_mrg)
        return df_mrg

    def _fetch_sbase_ldict(self, stmt):
        """
        Run a select query (stmt) on the database.

        args:
            stmt:          sqlalchemy (select) statement to execute

        returns:
            ldict:         List of dictionaries, one dict per row
        """
        with sqla.orm.Session(self.engine) as session:
            result = session.execute(stmt)
            ldict = []
            for row in result:
                row_as_dict = row._asdict()
                ldict.append(row_as_dict)
        return ldict


class PFinBackend(SBaseConn):
    """
    Personal Finance Backend
    Setup all database and API connections for the Personal Finance
    Backend ETL (Extract, Transfer, and Load) functionality. Define
    methods to update the tables in the (postgres) database,
    pulling from the various API data sources.
    """

    def __init__(self):
        env_prefix = "PFIN_"
        schema_list = ["auth", "pfin"]
        super().__init__(env_prefix, schema_list)
        self.fmp_client = PFinFMP(api_key=self._params["FMP_API_KEY"])
        self._stock_screener_min_mkt_cap = 1000000000
        self._stock_screener_result_limit = 5000
        self._tmp_date_fut = "4000-12-31"
        self._tmp_year_fut = 4000
        self._tmp_period_fut = "NA"

    def update_table_all(self):
        """
        Update all tables that get data from external API services... Meant to
        be run as a scheduled job nightly.
        """
        self.update_table_cpi()
        self.update_table_asset()
        self.update_table_equity_profile()
        self.update_table_reporting_period()
        self.update_table_income_statement()
        self.update_table_balance_sheet_statement()
        self.update_table_cash_flow_statement()
        self.update_table_earning()
        self.update_table_eod_price()
        return

    def update_table_cpi(self):
        """
        Fetch CPI data from the BLS. Insert new data into SupaBase... otherwise
        update the existing data in the cpi table in case the data was revised.
        """
        print("\n" + "==== " * 16)
        print(f"==== Updating pfin.cpi Table")
        api_key = self._params["BLS_API_KEY"]

        print(f"Fetch current CPI data from the BLS...")
        current_year = date.today().year

        # [richmosko]: FIXME... Get Series Name(s) from .env
        df_api = utils.fetch_cpi_df(api_key, "2017", current_year, ["CUUR0000SA0"])
        # df_api = fetch_cpi(api_key, '2022', '2026', ['CUUR0000SA0','SUUR0000SA0'])
        df_api = df_api.with_columns(pl.lit("cpi-u").alias("series_name"))
        df_api = utils.clean_empty_str_df(df_api)
        # print(df_api)

        print(f"Figure out what's already in pfin.cpi...")
        tab_sbase = self.base.by_module.pfin.cpi
        df_sbase = self.fetch_table_df(tab_sbase)
        # print(df_sbase)

        print(f"Merging columns to (inner join) to limit what gets sent to DB...")
        (common_cols, df_old, df_new) = self._calc_common_cols_df(
            tab_sbase, df_sbase, df_api
        )
        # print(common_cols)

        print(f"Determining entries to insert...")
        # [richmosko]: FIXME... key_list should inclue Series Name
        key_list = ["year", "month"]
        df_insert = self._isolate_new_rows_df(key_list, df_old, df_new)
        print(df_insert)

        print(f"Determining entries to update...")
        df_update = df_old
        # [richmosko]: ensure primary key present
        df_update = df_update.with_columns(df_sbase["id"].alias("id"))
        print(df_update)

        self.insert_table_df(tab_sbase, df_insert)
        self.update_table_df(tab_sbase, "id", df_update)
        return

    def update_table_asset(self):
        """
        Fetch asset date from the FMP API. Insert new data into SupaBase...
        """
        print("\n" + "==== " * 16)
        print(f"==== Updating pfin.asset Table")

        print(f"Figure out what's already in pfin.asset...")
        tab_sbase = self.base.by_module.pfin.asset
        df_sbase = self.fetch_table_df(tab_sbase)
        # print(f"  Existing Symbols: {df_sbase['symbol'].to_list()}")

        print(f"Querying for asset category...")
        tab_acat = self.base.by_module.pfin.asset_cat
        stmt = (
            sqla.select(tab_acat.id)
            .where(tab_acat.cat == "Equity")
            .where(tab_acat.sub_cat == "UNKNOWN")
        )
        ldict = self._fetch_sbase_ldict(stmt)
        asset_cat_id = ldict[0]["id"]
        # print(f"pfin.asset_cat.id = {asset_cat_id}\n")

        print(f"Generating a symbol list to process...")
        # df_slist = self.fmp_client.get_screened_stocks(self._stock_screener_min_mkt_cap,
        #                                               self._stock_screener_result_limit)
        # sym_list = df_slist['symbol'].to_list()
        sym_list = [
            "NVDA",
            "AAPL",
            "IREN",
            "V",
            "ALAB",
            "APP",
            "GOOGL",
            "META",
            "ABXL",
            "MSFT",
        ]

        print(f"Fetching data from Financial Modeling Prep...")
        df_fmp = self.fmp_client.fetch_fmp_list_df(
            self.fmp_client.search_symbol, "query", query=sym_list, limit=1
        )
        df_fmp = df_fmp.rename({"name": "description"})
        df_fmp = df_fmp.with_columns(
            [
                pl.lit(asset_cat_id).alias("asset_cat_id"),
                pl.lit(True).alias("has_financials"),
                pl.lit(True).alias("has_chart"),
            ]
        )
        df_fmp = utils.clean_empty_str_df(df_fmp)

        print(f"Merging columns to (inner join) to limit what gets sent to DB...")
        (common_cols, df_old, df_new) = self._calc_common_cols_df(
            tab_sbase, df_sbase, df_fmp
        )
        # print(common_cols)

        print(f"Determining entries to insert...")
        key_list = ["symbol"]
        df_insert = self._isolate_new_rows_df(key_list, df_old, df_new)
        print(df_insert)

        self.insert_table_df(tab_sbase, df_insert)
        return

    def update_table_equity_profile(self):
        """
        Fetch extended Equity Profile data from FMP using the equity-profile API.
        Insert new entries into SupaBase, otherwise update existing entries with
        fresh data.
        """
        print("\n" + "==== " * 16)
        print(f"==== Updating pfin.equity_profile Table")

        print(f"Figure out what's already in pfin.equity_profile...")
        tab_sbase = self.base.by_module.pfin.equity_profile
        df_sbase = self.fetch_table_df(tab_sbase)
        # print(df_sbase)

        print(f"Compiling set of symbol profiles to fetch from FMP...")
        asset_map = self._fetch_asset_map()
        id_list = list(asset_map.values())
        sym_list = list(asset_map.keys())
        # print(sym_list)

        print(f"Fetching data from Financial Modeling Prep...")
        key_list = ["symbol"]
        df_fmp = self.fmp_client.fetch_fmp_list_df(
            self.fmp_client.profile, "symbol", symbol=sym_list, limit=1
        )
        df_fmp = df_fmp.rename({"symbol": "asset_id"})
        df_fmp = df_fmp.with_columns(pl.Series("asset_id", id_list))
        df_fmp = utils.clean_empty_str_df(df_fmp)
        # print(df_fmp['asset_id'].to_list())

        print(f"Merging columns to (inner join) to limit what gets sent to DB...")
        (common_cols, df_old, df_new) = self._calc_common_cols_df(
            tab_sbase, df_sbase, df_fmp
        )
        # print(common_cols)

        print(f"Determining entries to insert...")
        key_list = "asset_id"
        df_insert = self._isolate_new_rows_df(key_list, df_old, df_new)
        print(df_insert)

        print(f"Determining entries to update...")
        df_update = df_old
        # [richmosko]: primary key already present in FK asset_id
        print(df_update)

        self.insert_table_df(tab_sbase, df_insert)
        self.update_table_df(tab_sbase, key_list, df_update)
        return

    def update_table_reporting_period(self):
        """
        Fetch reporting-period data from FMP using the income-statement API.
        Insert new entries into SupaBase, otherwise update existing entries with
        fresh data.
        """
        YEARS_TO_FETCH = 5
        PERIODS_TO_FETCH = YEARS_TO_FETCH * 4

        print("\n" + "==== " * 16)
        print(f"==== Updating pfin.reporting_period Table")

        print(f"Figure out what's already in pfin.reporting_period..")
        tab_sbase = self.base.by_module.pfin.reporting_period
        df_sbase = self.fetch_table_df(tab_sbase)
        # print(df_sbase)

        print(f"Generating a set of symbols to fetch from FMP...")
        asset_map = self._fetch_asset_map()
        id_list = list(asset_map.values())
        sym_list = list(asset_map.keys())
        # print(asset_map)

        print(f"Fetching data from Financial Modeling Prep...")
        df_fmp = self.fmp_client.fetch_fmp_list_df(
            self.fmp_client.income_statement,
            "symbol",
            symbol=sym_list,
            limit=PERIODS_TO_FETCH,
            period="quarter",
        )
        df_fmp = utils.clean_empty_str_df(df_fmp)
        df_fmp = df_fmp.rename({"symbol": "asset_id"})
        df_fmp = df_fmp.with_columns(
            pl.col("asset_id")
            .replace(asset_map)
            .str.to_integer(strict=False)
            .alias("asset_id")
        )
        df_fmp = df_fmp.with_columns(
            pl.col("fiscal_year").str.to_integer(strict=False).alias("fiscal_year")
        )
        df_fmp = df_fmp.rename({"date": "end_date"})
        df_fmp = df_fmp.with_columns(
            pl.col("end_date").str.to_date(strict=False).alias("end_date")
        )
        df_fmp = df_fmp.with_columns(
            pl.col("filing_date").str.to_date(strict=False).alias("filing_date")
        )
        df_fmp = df_fmp.with_columns(
            pl.col("accepted_date")
            .str.to_datetime(strict=False, time_zone="UTC")
            .alias("accepted_date")
        )

        print(f"Create generic 'future' reporting periods for EPS & Rev estimates...")
        tmp_date_now = datetime.now(timezone.utc)
        tmp_date_fut = datetime.fromisoformat(self._tmp_date_fut).replace(
            tzinfo=timezone.utc
        )
        tmp_year_fut = self._tmp_year_fut
        tmp_period_fut = self._tmp_period_fut
        for asset_id in id_list:
            new_row = {
                "asset_id": asset_id,
                "filing_date": tmp_date_fut.date(),
                "accepted_date": tmp_date_fut,
                "fiscal_year": tmp_year_fut,
                "period": tmp_period_fut,
            }
            df_row = pl.DataFrame(new_row)
            df_fmp = pl.concat([df_fmp, df_row], how="diagonal")
        # print(df_fmp)

        print(f"Merging columns to (inner join) to limit what gets sent to DB...")
        (common_cols, df_old, df_new) = self._calc_common_cols_df(
            tab_sbase, df_sbase, df_fmp
        )
        # print(common_cols)

        print(f"Determining entries to insert...")
        key_list = ["asset_id", "fiscal_year", "period"]
        df_insert = self._isolate_new_rows_df(key_list, df_old, df_new)
        print(df_insert)

        print(f"Determining entries to update...")
        df_update = df_old
        # [richmosko]: ensure primary key present
        df_update = df_update.with_columns(df_sbase["id"].alias("id"))
        print(df_update)

        self.insert_table_df(tab_sbase, df_insert)
        self.update_table_df(tab_sbase, "id", df_update)
        return

    def update_table_income_statement(self):
        """
        Fetch income-statement data from the FMP API.
        Insert new entries into SupaBase, otherwise update existing entries with
        fresh data.
        """
        YEARS_TO_FETCH = 5
        PERIODS_TO_FETCH = YEARS_TO_FETCH * 4

        print("\n" + "==== " * 16)
        print(f"==== Updating pfin.income_statement Table")

        print(f"Figure out what's already in pfin.reporting_period..")
        tab_rp = self.base.by_module.pfin.reporting_period
        df_rp = self.fetch_table_df(tab_rp)
        df_rp_map = df_rp[["id", "asset_id", "filing_date"]]
        # print(df_rp_map)

        tab_sbase = self.base.by_module.pfin.income_statement
        df_sbase = self.fetch_table_df(tab_sbase)
        # print(df_sbase)

        print(f"Generating a set of symbols to fetch from FMP...")
        asset_map = self._fetch_asset_map()
        id_list = list(asset_map.values())
        sym_list = list(asset_map.keys())
        # print(asset_map)

        print(f"Fetching income_statement data from Financial Modeling Prep...")
        df_fmp = self.fmp_client.fetch_fmp_list_df(
            self.fmp_client.income_statement,
            "symbol",
            symbol=sym_list,
            limit=PERIODS_TO_FETCH,
            period="quarter",
        )
        df_fmp = utils.clean_empty_str_df(df_fmp)
        df_fmp = df_fmp.rename({"symbol": "asset_id"})
        df_fmp = df_fmp.with_columns(
            pl.col("asset_id")
            .replace(asset_map)
            .str.to_integer(strict=False)
            .alias("asset_id")
        )
        df_fmp = df_fmp.with_columns(
            pl.col("fiscal_year").str.to_integer(strict=False).alias("fiscal_year")
        )
        df_fmp = df_fmp.rename({"date": "end_date"})
        df_fmp = df_fmp.with_columns(
            pl.col("end_date").str.to_date(strict=False).alias("end_date")
        )
        df_fmp = df_fmp.with_columns(
            pl.col("filing_date").str.to_date(strict=False).alias("filing_date")
        )
        df_fmp = df_fmp.with_columns(
            pl.col("accepted_date")
            .str.to_datetime(strict=False, time_zone="UTC")
            .alias("accepted_date")
        )
        uq_cols = ["asset_id", "filing_date"]
        df_fmp = df_rp_map.join(df_fmp, on=uq_cols, how="inner")
        df_fmp = df_fmp.drop(uq_cols)
        df_fmp = df_fmp.rename({"id": "reporting_period_id"})
        # print(df_fmp['reporting_period_id'].to_list())

        print(f"Merging columns to (inner join) to limit what gets sent to DB...")
        (common_cols, df_old, df_new) = self._calc_common_cols_df(
            tab_sbase, df_sbase, df_fmp
        )
        # print(common_cols)

        print(f"Determining entries to insert...")
        key_list = "reporting_period_id"
        df_insert = self._isolate_new_rows_df(key_list, df_old, df_new)
        print(df_insert)

        print(f"Determining entries to update...")
        df_update = df_old
        # [richmosko]: primary key already present in FK reporting_period_id
        print(df_update)

        self.insert_table_df(tab_sbase, df_insert)
        self.update_table_df(tab_sbase, key_list, df_update)
        return

    def update_table_balance_sheet_statement(self):
        """
        Fetch balance-sheet-statement data from the FMP API.
        Insert new entries into SupaBase, otherwise update existing entries with
        fresh data.
        """
        YEARS_TO_FETCH = 5
        PERIODS_TO_FETCH = YEARS_TO_FETCH * 4

        print("\n" + "==== " * 16)
        print(f"==== Updating pfin.balance_sheet_statement Table")

        print(f"Figure out what's already in pfin.reporting_period..")
        tab_rp = self.base.by_module.pfin.reporting_period
        df_rp = self.fetch_table_df(tab_rp)
        df_rp_map = df_rp[["id", "asset_id", "filing_date"]]
        # print(df_rp_map)

        print(f"Figure out what's already in pfin.balance_sheet_statement..")
        tab_sbase = self.base.by_module.pfin.balance_sheet_statement
        df_sbase = self.fetch_table_df(tab_sbase)
        # print(df_sbase)

        print(f"Generating a set of symbols to fetch from FMP...")
        asset_map = self._fetch_asset_map()
        id_list = list(asset_map.values())
        sym_list = list(asset_map.keys())
        # print(asset_map)

        print(f"Fetching balance_sheet_statement data from Financial Modeling Prep...")
        df_fmp = self.fmp_client.fetch_fmp_list_df(
            self.fmp_client.balance_sheet_statement,
            "symbol",
            symbol=sym_list,
            limit=PERIODS_TO_FETCH,
            period="quarter",
        )
        df_fmp = utils.clean_empty_str_df(df_fmp)
        df_fmp = df_fmp.rename({"symbol": "asset_id"})
        df_fmp = df_fmp.with_columns(
            pl.col("asset_id")
            .replace(asset_map)
            .str.to_integer(strict=False)
            .alias("asset_id")
        )
        df_fmp = df_fmp.with_columns(
            pl.col("fiscal_year").str.to_integer(strict=False).alias("fiscal_year")
        )
        df_fmp = df_fmp.rename({"date": "end_date"})
        df_fmp = df_fmp.with_columns(
            pl.col("end_date").str.to_date(strict=False).alias("end_date")
        )
        df_fmp = df_fmp.with_columns(
            pl.col("filing_date").str.to_date(strict=False).alias("filing_date")
        )
        df_fmp = df_fmp.with_columns(
            pl.col("accepted_date")
            .str.to_datetime(strict=False, time_zone="UTC")
            .alias("accepted_date")
        )
        uq_cols = ["asset_id", "filing_date"]
        df_fmp = df_rp_map.join(df_fmp, on=uq_cols, how="inner")
        df_fmp = df_fmp.drop(uq_cols)
        df_fmp = df_fmp.rename({"id": "reporting_period_id"})
        # print(df_fmp['reporting_period_id'].to_list())

        print(f"Merging columns to (inner join) to limit what gets sent to DB...")
        (common_cols, df_old, df_new) = self._calc_common_cols_df(
            tab_sbase, df_sbase, df_fmp
        )
        # print(common_cols)

        print(f"Determining entries to insert...")
        key_list = "reporting_period_id"
        df_insert = self._isolate_new_rows_df(key_list, df_old, df_new)
        print(df_insert)

        print(f"Determining entries to update...")
        df_update = df_old
        # [richmosko]: primary key already present in FK reporting_period_id
        print(df_update)

        self.insert_table_df(tab_sbase, df_insert)
        self.update_table_df(tab_sbase, key_list, df_update)
        return

    def update_table_cash_flow_statement(self):
        """
        Fetch cash-flow-statement data from the FMP API.
        Insert new entries into SupaBase, otherwise update existing entries with
        fresh data.
        """
        YEARS_TO_FETCH = 5
        PERIODS_TO_FETCH = YEARS_TO_FETCH * 4

        print("\n" + "==== " * 16)
        print(f"==== Updating pfin.cash_flow_statement Table")

        print(f"Figure out what's already in pfin.reporting_period..")
        tab_rp = self.base.by_module.pfin.reporting_period
        df_rp = self.fetch_table_df(tab_rp)
        df_rp_map = df_rp[["id", "asset_id", "filing_date"]]
        # print(df_rp_map)

        print(f"Figure out what's already in pfin.cash_flow_statement..")
        tab_sbase = self.base.by_module.pfin.cash_flow_statement
        df_sbase = self.fetch_table_df(tab_sbase)
        # print(df_sbase)

        print(f"Generating a set of symbols to fetch from FMP...")
        asset_map = self._fetch_asset_map()
        id_list = list(asset_map.values())
        sym_list = list(asset_map.keys())
        # print(asset_map)

        print(f"Fetching cash_flow_statement data from Financial Modeling Prep...")
        df_fmp = self.fmp_client.fetch_fmp_list_df(
            self.fmp_client.cash_flow_statement,
            "symbol",
            symbol=sym_list,
            limit=PERIODS_TO_FETCH,
            period="quarter",
        )
        df_fmp = utils.clean_empty_str_df(df_fmp)
        df_fmp = df_fmp.rename({"symbol": "asset_id"})
        df_fmp = df_fmp.with_columns(
            pl.col("asset_id")
            .replace(asset_map)
            .str.to_integer(strict=False)
            .alias("asset_id")
        )
        df_fmp = df_fmp.with_columns(
            pl.col("fiscal_year").str.to_integer(strict=False).alias("fiscal_year")
        )
        df_fmp = df_fmp.rename({"date": "end_date"})
        df_fmp = df_fmp.with_columns(
            pl.col("end_date").str.to_date(strict=False).alias("end_date")
        )
        df_fmp = df_fmp.with_columns(
            pl.col("filing_date").str.to_date(strict=False).alias("filing_date")
        )
        df_fmp = df_fmp.with_columns(
            pl.col("accepted_date")
            .str.to_datetime(strict=False, time_zone="UTC")
            .alias("accepted_date")
        )
        uq_cols = ["asset_id", "filing_date"]
        df_fmp = df_rp_map.join(df_fmp, on=uq_cols, how="inner")
        df_fmp = df_fmp.drop(uq_cols)
        df_fmp = df_fmp.rename({"id": "reporting_period_id"})
        # print(df_fmp['reporting_period_id'].to_list())

        print(f"Merging columns to (inner join) to limit what gets sent to DB...")
        (common_cols, df_old, df_new) = self._calc_common_cols_df(
            tab_sbase, df_sbase, df_fmp
        )
        # print(common_cols)

        print(f"Determining entries to insert...")
        key_list = "reporting_period_id"
        df_insert = self._isolate_new_rows_df(key_list, df_old, df_new)
        print(df_insert)

        print(f"Determining entries to update...")
        df_update = df_old
        # [richmosko]: primary key already present in FK reporting_period_id
        print(df_update)

        self.insert_table_df(tab_sbase, df_insert)
        self.update_table_df(tab_sbase, key_list, df_update)
        return

    def update_table_earning(self):
        """
        Fetch earnings data from the FMP API.
        Insert new entries into SupaBase, otherwise update existing entries with
        fresh data.

        The alignment with reporting_period(s) needs to be handled uniquely, as the
        reference dates in earnings do not match the filing or accepted dates in
        the actual 10Q or income_statement data. The latest one is close though... so
        the older quarters are backfilled based on that match. Future earnings estimates
        are stored in an arbitrary future date to denote that the data is incomplete and
        so that the actual quarterly updates don't create duplicate quarters with different
        refernece dates.
        """

        YEARS_TO_FETCH = 5
        PERIODS_TO_FETCH = YEARS_TO_FETCH * 4

        print("\n" + "==== " * 16)
        print(f"==== Updating pfin.earning Table")

        print(f"Figure out what's already in pfin.reporting_period...")
        tab_rp = self.base.by_module.pfin.reporting_period
        df_rp = self.fetch_table_df(tab_rp)
        df_rp_map = df_rp[["id", "asset_id", "filing_date"]]
        # print(df_rp_map)

        print(
            f"Find the latest (current) report date for each asset_id in pfin.reporting_period..."
        )
        asset_id_list = df_rp_map["asset_id"].unique().to_list()
        latest_rpt = {}
        for asset_id in asset_id_list:
            df_tmp = df_rp_map.filter(pl.col("asset_id") == asset_id)
            df_tmp = df_tmp.sort("filing_date", descending=True)
            # [richmosko]: skip the 1st date which is reserved for future estimates...
            latest_rpt[asset_id] = df_tmp.item(1, "filing_date")
        # print(f"  Latest Reports: {latest_rpt}")

        print(f"Figure out what's already in pfin.earning...")
        tab_sbase = self.base.by_module.pfin.earning
        df_sbase = self.fetch_table_df(tab_sbase)
        # print(df_sbase)

        print(f"Generating a set of symbols to fetch from FMP...")
        asset_map = self._fetch_asset_map()
        id_list = list(asset_map.values())
        sym_list = list(asset_map.keys())
        # print(asset_map)

        print(f"Fetching earning data from Financial Modeling Prep...")
        df_fmp = self.fmp_client.fetch_fmp_list_df(
            self.fmp_client.earnings,
            "symbol",
            symbol=sym_list,
            limit=(PERIODS_TO_FETCH + 2),
        )
        df_fmp = utils.clean_empty_str_df(df_fmp)
        df_fmp = df_fmp.filter(
            ~(
                pl.col("revenue_actual").is_null()
                & pl.col("revenue_estimated").is_null()
            )
        )
        df_fmp = df_fmp.rename({"symbol": "asset_id"})
        df_fmp = df_fmp.rename({"date": "filing_date"})
        df_fmp = df_fmp.with_columns(
            pl.col("asset_id")
            .replace(asset_map)
            .str.to_integer(strict=False)
            .alias("asset_id")
        )
        df_fmp = df_fmp.with_columns(
            pl.col("filing_date").str.to_date(strict=False).alias("filing_date")
        )
        df_fmp = df_fmp.with_columns(pl.col("filing_date").alias("ref_date"))
        df_fmp = df_fmp.with_columns(pl.lit(None).alias("reporting_period_id"))
        # print(df_fmp)

        print(f"Match earnings reports to posted reporting_periods...")
        # sort and add a temporary row_idx as primary keys
        df_rp_map = df_rp_map.sort("filing_date", descending=True).with_row_index(
            name="row_idx"
        )
        df_fmp = df_fmp.sort("filing_date", descending=True).with_row_index(
            name="row_idx"
        )

        fmp_drop_list = []
        for asset_id in id_list:
            cond_fmp_asset_id = pl.col("asset_id") == asset_id
            cond_fmp_filing_date = pl.col("filing_date") <= latest_rpt[
                asset_id
            ] + timedelta(weeks=2)
            # filter for the conditions above, and get the row_idx values as lists
            fmp_idx = df_fmp.filter(cond_fmp_asset_id & cond_fmp_filing_date)[
                "row_idx"
            ].to_list()
            rpm_idx = df_rp_map.filter(cond_fmp_asset_id & cond_fmp_filing_date)[
                "row_idx"
            ].to_list()
            # since the lengths have to match, truncate the the shorter length and remember dropped idxs
            joint_len = min([len(fmp_idx), len(rpm_idx)])
            fmp_drop_list.extend(fmp_idx[joint_len:])
            fmp_idx = fmp_idx[:joint_len]
            rpm_idx = rpm_idx[:joint_len]
            # get the list of reporting_period.id(s) from the row indexes
            rpm_list = df_rp_map.filter(pl.col("row_idx").is_in(rpm_idx))[
                "id"
            ].to_list()
            df_map = pl.DataFrame({"row_idx": fmp_idx, "reporting_period_id": rpm_list})
            # now find the index rows in df_fmp and replace them with rpm_list
            if len(df_map):
                df_fmp = df_fmp.update(df_map, on="row_idx")

        print(
            f"Dropping long dated earnings with no reporting_periods: {fmp_drop_list}..."
        )
        df_fmp = df_fmp.filter(~pl.col("row_idx").is_in(fmp_drop_list))

        print(
            f"Set remaining unmatched earnings reports to future reporting_periods..."
        )
        tmp_date_now = datetime.now(timezone.utc)
        tmp_date_fut = (
            datetime.fromisoformat(self._tmp_date_fut)
            .replace(tzinfo=timezone.utc)
            .date()
        )
        df_fmp = df_fmp.with_columns(
            pl.when(pl.col("reporting_period_id").is_null())
            .then(pl.lit(tmp_date_fut))
            .otherwise(pl.col("filing_date"))
            .alias("filing_date")
        )
        uq_cols = ["asset_id", "filing_date"]
        df_rp_map = (
            df_rp_map.filter(pl.col("filing_date") == tmp_date_fut)
            .rename({"id": "reporting_period_id"})
            .drop("row_idx")
        )
        df_fmp = df_fmp.update(df_rp_map, on=uq_cols).drop(
            ["asset_id", "filing_date", "row_idx"]
        )
        print(
            f"  Null reporting_period_id(s) found: {
                len(df_fmp.filter(pl.col('reporting_period_id').is_null()))
            }"
        )
        # print(df_fmp)

        print(f"Merging columns to (inner join) to limit what gets sent to DB...")
        (common_cols, df_old, df_new) = self._calc_common_cols_df(
            tab_sbase, df_sbase, df_fmp
        )
        # print(common_cols)

        print(f"Determining entries to insert...")
        key_list = "reporting_period_id"
        df_insert = self._isolate_new_rows_df(key_list, df_old, df_new)
        print(df_insert)

        print(f"Determining entries to update...")
        df_update = df_old
        # [richmosko]: primary key already present in FK reporting_period_id
        print(df_update)

        self.insert_table_df(tab_sbase, df_insert)
        self.update_table_df(tab_sbase, key_list, df_update)
        return

    def update_table_eod_price(self):
        """
        Fetch end of day price data from the FMP API.
        Insert new entries into SupaBase, otherwise update existing entries with
        fresh data in case the historical data was revised.
        """

        YEARS_TO_FETCH = 5
        DAYS_TO_FETCH = YEARS_TO_FETCH * 365

        print("\n" + "==== " * 16)
        print(f"==== Updating pfin.eod_price Table")

        print(f"Figure out what's already in pfin.eod_price...")
        tab_sbase = self.base.by_module.pfin.eod_price
        df_sbase = self.fetch_table_df(tab_sbase)
        # print(df_sbase)

        print(f"Generating a set of symbols to fetch from FMP...")
        asset_map = self._fetch_asset_map()
        id_list = list(asset_map.values())
        sym_list = list(asset_map.keys())
        # print(asset_map)

        print(f"Fetching EOD historical data from Financial Modeling Prep...")
        date_5y_ago = datetime.now() - timedelta(days=DAYS_TO_FETCH)
        date_5y_ago = date_5y_ago.strftime("%Y-%m-%d")
        df_fmp = self.fmp_client.fetch_fmp_list_df(
            self.fmp_client.historical_full,
            "symbol",
            symbol=sym_list,
            start_date=date_5y_ago,
        )
        df_fmp = utils.clean_empty_str_df(df_fmp)
        df_fmp = df_fmp.rename({"symbol": "asset_id"})
        df_fmp = df_fmp.with_columns(
            pl.col("asset_id")
            .replace(asset_map)
            .str.to_integer(strict=False)
            .alias("asset_id")
        )
        df_fmp = df_fmp.rename({"date": "end_date"})
        df_fmp = df_fmp.with_columns(
            pl.col("end_date").str.to_date(strict=False).alias("end_date")
        )
        # print(df_fmp)

        print(f"Merging columns to (inner join) to limit what gets sent to DB...")
        (common_cols, df_old, df_new) = self._calc_common_cols_df(
            tab_sbase, df_sbase, df_fmp
        )
        # print(common_cols)

        print(f"Determining entries to insert...")
        key_list = ["asset_id", "end_date"]
        df_insert = self._isolate_new_rows_df(key_list, df_old, df_new)
        print(df_insert)

        print(f"Determining entries to update...")
        df_update = df_old
        # [richmosko]: ensure primary key present
        df_update = df_update.with_columns(df_sbase["id"].alias("id"))
        print(df_update)

        self.insert_table_df(tab_sbase, df_insert)
        self.update_table_df(tab_sbase, "id", df_update)
        return

    def _fetch_asset_map(self):
        """
        Generate list of symbols to work on by querying the asset table and
        filtering by asset_cat as Equity and matching appropriate boolean flags.
        args:
            session:       the current active sqlalchemy session

        returns:
            asset_map:     dictionary of symbol(s) and mapped asset_id(s)
        """
        tab_asset = self.base.by_module.pfin.asset
        tab_asset_cat = self.base.by_module.pfin.asset_cat
        stmt = (
            sqla.select(tab_asset.symbol, tab_asset.id)
            .join(tab_asset_cat)
            .where(tab_asset_cat.cat == "Equity")
            .where(tab_asset.has_financials == True)
        )
        ldict = self._fetch_sbase_ldict(stmt)
        id_list = [d["id"] for d in ldict]
        sym_list = [d["symbol"] for d in ldict]
        asset_map = {}
        for item in ldict:
            sym = item["symbol"]
            xid = item["id"]
            asset_map[sym] = xid
        return asset_map

    def _set_dtype_df(self, tab_sbase, df_sbase):
        """
        Set the initial datatypes for columns in a polars dataframe from the
        sqlalchemy reflected table.

        args:
            tab_sbase:     reflected sqlalchemy table
            df_sbase:      polars dataframe of  ^^^^^ table

        returns:
            df_dtype:      schema corrected polars dataframe
        """
        c_dict = pfb.get_column_dict(tab_sbase)
        df_schema = df_sbase.schema
        # TODO: Creating a big case statement for all the data types is a big
        #       undertaking... Not urgent to impleent this right away.
        return None
