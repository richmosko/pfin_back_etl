"""
Project:       pfin_back_etl
Author:        Rich Mosko

Description:
    Common utility functions

"""

# library imports
import os
import dotenv
import re
import requests
import json
import polars as pl


def col_to_snake(col_list):
    col_dict = {}
    for col in col_list:
        col_dict[col] = re.sub(r"([a-z])([A-Z])", r"\1_\2", col).lower()
    return col_dict


def ldict_to_df(ldict, tab):
    cols = tab.columns.keys()
    df = pl.DataFrame(schema=cols) if not ldict else pl.DataFrame(ldict)
    return df


def clean_empty_str_df(df):
    df_clean = df.with_columns(pl.col(pl.String).replace("", None))
    return df_clean


def apply_schema_df(df_src, df_tgt):
    """
    Cast datatypes from one polars dataframe(df_from) to another DF(df_to)

    args:
        df_src:            polars dataframe source to extract schema(datatypes)
        df_tgt:            polars dataframe target to apply schema(datatypes)

    returns:
        df_cast:           df_tgt with casted datatypes (polars DF)
    """
    schema_src = df_src.schema
    schema_tgt = df_tgt.schema
    for key in schema_tgt.keys():
        if key in schema_src:
            schema_tgt[key] = schema_src[key]
    df_cast = df_tgt.cast(schema_tgt)
    return df_cast


def load_env_variables(env_prefix):
    """
    Load the environmental variables from a '.env' file. The variables read
    should countain the specific setup constraints and passwords for use in
    the database access and API calls.

    returns: params (dictionary of the desired environmental variables)
    """
    params = {}

    # Load environment variables from local .env file
    dotenv.load_dotenv()

    # Check for API Key in FMP_API_KEY env variable
    key_name = "FMP_API_KEY"
    key_value = os.getenv(key_name)
    params["FMP_API_KEY"] = key_value
    if key_value is not None:
        print(f"{key_name} value found...")
    else:
        raise ValueError(
            f"Environment variable {key_name} does not exist in .env file."
        )

    # Check for BLS API Key in env variable
    key_name = "BLS_API_KEY"
    key_value = os.getenv(key_name)
    params["BLS_API_KEY"] = key_value
    if key_value is not None:
        print(f"{key_name} value found...")
    else:
        raise ValueError(
            f"Environment variable {key_name} does not exist in .env file."
        )

    # Fetch other env variables
    params["DB_USER"] = os.getenv(env_prefix + "DB_USER")
    params["DB_HOST"] = os.getenv(env_prefix + "DB_HOST")
    params["DB_PORT"] = os.getenv(env_prefix + "DB_PORT")
    params["DB_NAME"] = os.getenv(env_prefix + "DB_NAME")
    params["DB_PASSWORD"] = os.getenv(env_prefix + "DB_PASSWORD")
    return params


def sqla_modulename_for_table(tablename, declarativetable, reflecttable):
    """
    This function needs to be defined with the above input arguments
    for sqlalchemy to automap the table names including the schemas
    when referencing through the 'by_module' class:
        ie: base.by_module.pfin.reporting_period

    used in the _sbase_setup() member function of PfinSBaseConn

    returns: schema (string of schema name to use)
    """
    if reflecttable.schema:
        return reflecttable.schema
    else:
        # Default module name if no schema is present
        return "public"


def sqla_resolve_referred_schema(table, to_metadata, constraint, referred_schema):
    """
    Dynamically determines the target schema for a foreign key reference
    in sqlalchemy. Used when creating a temp table for a staging update.
    """
    if referred_schema == "source_schema":
        return "target_schema"  # Map 'source_schema' to 'target_schema'
    return referred_schema


def fetch_cpi_df(api_key, startyear, endyear, series_id_lst):
    """
    Fetch Consumer Price Index data from the Brureau of Labor Statistics.

    args:
        startyear:         starting year to fetch in 'yyyy' format
        endyear:           ending year to fetch in 'yyyy' format
        series_id_lst:     list of series IDs to fetch. id: ['CUUR0000SA0']

    returns:
        df_cpi:            polars dataframe of CPI index data
    """
    headers = {"Content-type": "application/json"}
    data = json.dumps(
        {
            "registrationkey": api_key,
            "seriesid": series_id_lst,
            "startyear": startyear,
            "endyear": endyear,
        }
    )
    p = requests.post(
        "https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers
    )
    json_data = json.loads(p.text)

    print(f"  JSON STATUS: {json_data['status']}")
    if json_data["status"] != "REQUEST_SUCCEEDED":
        raise Exception("BLS CPI fetch request unsuccessful")

    df_list = []
    for series in json_data["Results"]["series"]:
        df = pl.DataFrame(series["data"])
        df = df.rename(col_to_snake(df.columns))
        df = df.with_columns(pl.lit(series["seriesID"]).alias("series_id"))
        df = df.rename({"period": "month"})
        df = df.with_columns(
            [
                pl.col("month")
                .str.strip_chars("M")
                .cast(pl.Int64, strict=False)
                .alias("month"),
                pl.col("year").cast(pl.Int64, strict=False).alias("year"),
                pl.col("value").cast(pl.Float64, strict=False).alias("value"),
            ]
        )
        df = df.drop_nulls(subset=["value"])
        df = df.rename({"value": "series_value"})
        df = df.drop("footnotes")
        df = df.with_columns(pl.format("{}-{}-14", "year", "month").alias("ref_date"))
        df_list.append(df)
    df_cpi = pl.concat(df_list, how="vertical_relaxed")
    return df_cpi
