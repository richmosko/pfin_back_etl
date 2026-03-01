"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Integration tests for database initialization and table reflection.
    Requires valid .env credentials to connect to the SupaBase instance.
"""

import pytest
import sqlalchemy as sqla


@pytest.mark.integration
def test_backend_init(backend):
    backend.print_schema_info()
    assert backend


@pytest.mark.integration
def test_table_reflection(backend):
    insp = sqla.inspect(backend.engine)
    print(insp.get_table_names(schema="pfin"))
    pfin_tab_check_list = [
        "account",
        "account_users",
        "account_trans",
        "account_type",
        "asset",
        "asset_cat",
        "balance_sheet_statement",
        "cash_flow_statement",
        "cpi",
        "earning",
        "eod_price",
        "equity_profile",
        "income_statement",
        "nav",
        "reporting_period",
        "schema_version",
        "tax_cat",
        "trans_cat",
        "user_profile",
        "watchlist",
    ]
    print(f"Table Check List: {pfin_tab_check_list}")
    for tab_name in pfin_tab_check_list:
        print(f"CHECKING TABLE pfin.{tab_name}")
        tab = backend.get_reflected_table("pfin", tab_name)
        assert tab.__table__.schema == "pfin"
        assert tab.__table__.name == tab_name
        c_dict = backend.get_column_dict(tab)
        assert c_dict
