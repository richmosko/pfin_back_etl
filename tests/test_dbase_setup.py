"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Test function to initialize the backend and database

"""

# library imports
import pfin_back_etl as pfbe
import sqlalchemy as sqla


def test_backend_init():
    pfb = pfbe.PFinBackend()
    pfb.print_schema_info()
    assert pfb


def test_table_reflection():
    pfb = pfbe.PFinBackend()
    insp = sqla.inspect(pfb.engine)
    print(insp.get_table_names(schema="pfin"))
    pfin_tab_check_list = [
        "account",
        "account_access",
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
        "member",
        "nav",
        "reporting_period",
        "schema_version",
        "tax_cat",
        "trans_cat",
        "watchlist",
    ]
    print(f"Table Check List: {pfin_tab_check_list}")
    for tab_name in pfin_tab_check_list:
        print(f"CHECKING TABLE pfin.{tab_name}")
        tab = pfb.get_reflected_table("pfin", tab_name)
        assert tab.__table__.schema == "pfin"
        assert tab.__table__.name == tab_name
        c_dict = pfb.get_column_dict(tab)
        assert c_dict
