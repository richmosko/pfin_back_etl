"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Unit tests for core ETL logic in pfin_back_etl.core.
    Tests the DataFrame manipulation methods without requiring
    database or API connections.
"""

import pytest
import polars as pl
from unittest.mock import MagicMock, patch, PropertyMock
from pfin_back_etl.core import SBaseConn, PFinFMP


# ===================================================================
# SBaseConn row isolation logic (tested via a mock subclass)
# ===================================================================
class TestIsolateNewRows:
    """Tests for _isolate_new_rows_df — identifies rows to INSERT."""

    @pytest.mark.unit
    def test_new_rows_detected(self, sample_df_old, sample_df_new):
        """META and MSFT are new (not in old), should be returned."""
        # We need to test the method directly, so we create a minimal mock
        conn = object.__new__(SBaseConn)

        key_list = ["symbol"]
        result = conn._isolate_new_rows_df(key_list, sample_df_old, sample_df_new)

        symbols = sorted(result["symbol"].to_list())
        assert symbols == ["META", "MSFT"]

    @pytest.mark.unit
    def test_no_new_rows(self, sample_df_old):
        """When all new rows already exist, result should be empty."""
        conn = object.__new__(SBaseConn)

        df_new = pl.DataFrame(
            {
                "symbol": ["AAPL", "NVDA"],
                "description": ["Apple", "NVIDIA"],
            }
        )
        key_list = ["symbol"]
        result = conn._isolate_new_rows_df(key_list, sample_df_old, df_new)
        assert len(result) == 0

    @pytest.mark.unit
    def test_all_new_rows_when_old_empty(self, sample_df_new):
        """When old table is empty, all rows should be returned as new."""
        conn = object.__new__(SBaseConn)

        df_old = pl.DataFrame(schema={"symbol": pl.String, "description": pl.String})
        key_list = ["symbol"]
        result = conn._isolate_new_rows_df(key_list, df_old, sample_df_new)
        assert len(result) == 4  # All rows are new


class TestIsolateUpdatedRows:
    """Tests for _isolate_updated_rows_df — identifies rows to UPDATE."""

    @pytest.mark.unit
    def test_overlapping_rows_detected(self, sample_df_old, sample_df_new):
        """AAPL and NVDA exist in both, should be returned for update."""
        conn = object.__new__(SBaseConn)

        key_list = ["symbol"]
        result = conn._isolate_updated_rows_df(key_list, sample_df_old, sample_df_new)

        symbols = sorted(result["symbol"].to_list())
        assert symbols == ["AAPL", "NVDA"]

    @pytest.mark.unit
    def test_no_overlapping_rows(self, sample_df_old):
        """When there's no overlap, result should be empty."""
        conn = object.__new__(SBaseConn)

        df_new = pl.DataFrame(
            {
                "symbol": ["META", "MSFT"],
                "description": ["Meta", "Microsoft"],
            }
        )
        key_list = ["symbol"]
        result = conn._isolate_updated_rows_df(key_list, sample_df_old, df_new)
        assert len(result) == 0

    @pytest.mark.unit
    def test_empty_old_returns_empty(self, sample_df_new):
        """When old table is empty, no rows to update."""
        conn = object.__new__(SBaseConn)

        df_old = pl.DataFrame(schema={"symbol": pl.String, "description": pl.String})
        key_list = ["symbol"]
        result = conn._isolate_updated_rows_df(key_list, df_old, sample_df_new)
        assert len(result) == 0


class TestCalcCommonCols:
    """Tests for _calc_common_cols_df — finds common columns between DB and API."""

    @pytest.mark.unit
    def test_common_cols_found(self):
        conn = object.__new__(SBaseConn)

        # Mock the tab_sbase with column keys
        mock_table = MagicMock()
        mock_table.__table__ = MagicMock()
        mock_table.__table__.columns.keys.return_value = [
            "id", "symbol", "description", "created_at"
        ]

        df_sbase = pl.DataFrame({
            "id": [1, 2],
            "symbol": ["AAPL", "NVDA"],
            "description": ["Apple", "NVIDIA"],
            "created_at": ["2024-01-01", "2024-01-02"],
        })

        df_api = pl.DataFrame({
            "symbol": ["AAPL", "NVDA", "META"],
            "description": ["Apple Inc.", "NVIDIA Corp.", "Meta"],
            "extra_field": [100, 200, 300],  # not in DB
        })

        common_cols, df_old, df_new = conn._calc_common_cols_df(
            mock_table, df_sbase, df_api
        )

        assert "symbol" in common_cols
        assert "description" in common_cols
        assert "extra_field" not in common_cols
        assert "created_at" not in common_cols  # not in API data
        assert len(df_old) == 2
        assert len(df_new) == 3


# ===================================================================
# PFinFMP (tested with mocked API calls)
# ===================================================================
class TestPFinFMP:
    """Tests for FMP client wrapper methods."""

    @pytest.mark.unit
    def test_fetch_fmp_df_converts_to_snake_case(self):
        """Verify that FMP responses get their columns converted to snake_case."""
        fmp = object.__new__(PFinFMP)

        mock_func = MagicMock()
        mock_func.__name__ = "test_api"
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"reportedCurrency": "USD", "netIncome": 1000000}
        ]
        mock_func.return_value = mock_response

        result = fmp.fetch_fmp_df(mock_func, symbol="AAPL")

        assert "reported_currency" in result.columns
        assert "net_income" in result.columns
        assert "reportedCurrency" not in result.columns

    @pytest.mark.unit
    def test_fetch_fmp_df_empty_response(self):
        """An empty API response should return an empty DataFrame."""
        fmp = object.__new__(PFinFMP)

        mock_func = MagicMock()
        mock_func.__name__ = "test_api"
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_func.return_value = mock_response

        result = fmp.fetch_fmp_df(mock_func, symbol="FAKE")
        assert len(result) == 0

    @pytest.mark.unit
    def test_fetch_fmp_list_df_concatenates(self):
        """Verify multiple API calls are concatenated into one DataFrame."""
        fmp = object.__new__(PFinFMP)

        call_count = 0

        def mock_fetch_fmp_df(func, **kwargs):
            nonlocal call_count
            call_count += 1
            return pl.DataFrame({"symbol": [kwargs["symbol"]], "value": [call_count]})

        fmp.fetch_fmp_df = mock_fetch_fmp_df

        mock_func = MagicMock()
        result = fmp.fetch_fmp_list_df(
            mock_func, "symbol", symbol=["AAPL", "NVDA", "META"]
        )

        assert len(result) == 3
        assert sorted(result["symbol"].to_list()) == ["AAPL", "META", "NVDA"]
