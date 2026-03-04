"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Integration tests for ETL update operations.
    Requires valid .env credentials to connect to SupaBase and external APIs.
"""

import pytest
import pfin_back_etl as pfbe

SYMBOL_LIST = [
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
    "UAMY",
    "VIR",
    "VRTX",
    "MPT",
    "ADSK",
    "PANW",
]


@pytest.mark.integration
def test_update_table_cpi(backend):
    backend.update_table_cpi()
    backend.update_table_cpi(num_years=2)


@pytest.mark.integration
def test_update_table_asset(backend):
    backend.update_table_asset(sym_list=SYMBOL_LIST)


@pytest.mark.integration
def test_update_table_equity_profile(backend):
    backend.update_table_equity_profile(sym_list=SYMBOL_LIST)


@pytest.mark.integration
def test_update_table_reporting_period(backend):
    backend.update_table_reporting_period(sym_list=SYMBOL_LIST)


@pytest.mark.integration
def test_update_table_income_statement(backend):
    backend.update_table_income_statement(sym_list=SYMBOL_LIST)


@pytest.mark.integration
def test_update_table_balance_sheet_statement(backend):
    backend.update_table_balance_sheet_statement(sym_list=SYMBOL_LIST)


@pytest.mark.integration
def test_update_table_cash_flow_statement(backend):
    backend.update_table_cash_flow_statement(sym_list=SYMBOL_LIST)


@pytest.mark.integration
def test_update_table_earning(backend):
    backend.update_table_earning(sym_list=SYMBOL_LIST)


@pytest.mark.integration
def test_update_table_eod_price(backend):
    backend.update_table_eod_price(sym_list=SYMBOL_LIST)


@pytest.mark.integration
def test_update_table_all(backend):
    backend.update_table_all(sym_list=SYMBOL_LIST)


@pytest.mark.deployment
def test_update_table_all_search():
    print("Perform Full Symbol Search. Update All records...")
    pfb = pfbe.PFinBackend()
    pfb.update_table_all()
