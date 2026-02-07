"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
"""

# library imports
import core
import polars as pl

pfb = core.PFinBackend()
pfb.print_schema_info()
tab = pfb.get_reflected_table('pfin', 'cpi')
c_dict = pfb.get_column_dict(tab)
#print(c_dict)

pfb.update_table_all()
#pfb.update_table_cpi()
#pfb.update_table_asset()
#pfb.update_table_equity_profile()
#pfb.update_table_reporting_period()
#pfb.update_table_income_statement()
#pfb.update_table_balance_sheet_statement()
#pfb.update_table_cash_flow_statement()
#pfb.update_table_earning()
#pfb.update_table_eod_price()
