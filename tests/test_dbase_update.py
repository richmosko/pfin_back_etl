"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Test function to initialize the backend and database

"""

# library imports
import pfin_back_etl as pfbe


def test_update_table_cpi():
    pfb = pfbe.PFinBackend()
    pfb.update_table_cpi()


def test_update_table_asset():
    pfb = pfbe.PFinBackend()
    pfb.update_table_asset()


def test_update_table_equity_profile():
    pfb = pfbe.PFinBackend()
    pfb.update_table_equity_profile()


def test_update_table_reporting_period():
    pfb = pfbe.PFinBackend()
    pfb.update_table_reporting_period()


def test_update_table_income_statement():
    pfb = pfbe.PFinBackend()
    pfb.update_table_income_statement()


def test_update_table_balance_sheet_statement():
    pfb = pfbe.PFinBackend()
    pfb.update_table_balance_sheet_statement()


def test_update_table_cash_flow_statement():
    pfb = pfbe.PFinBackend()
    pfb.update_table_cash_flow_statement()


def test_update_table_earning():
    pfb = pfbe.PFinBackend()
    pfb.update_table_earning()


def test_update_table_eod_price():
    pfb = pfbe.PFinBackend()
    pfb.update_table_eod_price()


def test_update_table_all():
    pfb = pfbe.PFinBackend()
    pfb.update_table_all()
