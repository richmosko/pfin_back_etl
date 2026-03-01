"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Unit tests for utility functions in pfin_back_etl.utils.
    These tests run without any external dependencies (no DB, no API).
"""

import pytest
import polars as pl
from unittest.mock import patch, MagicMock
from pfin_back_etl import utils


# ===================================================================
# col_to_snake
# ===================================================================
class TestColToSnake:
    """Tests for camelCase -> snake_case column name conversion."""

    @pytest.mark.unit
    def test_basic_conversion(self):
        result = utils.col_to_snake(["reportedCurrency", "fillingDate"])
        assert result == {
            "reportedCurrency": "reported_currency",
            "fillingDate": "filling_date",
        }

    @pytest.mark.unit
    def test_already_snake_case(self):
        result = utils.col_to_snake(["net_income", "gross_profit"])
        assert result == {
            "net_income": "net_income",
            "gross_profit": "gross_profit",
        }

    @pytest.mark.unit
    def test_single_word(self):
        result = utils.col_to_snake(["revenue", "eps", "price"])
        assert result == {
            "revenue": "revenue",
            "eps": "eps",
            "price": "price",
        }

    @pytest.mark.unit
    def test_multiple_capitals(self):
        result = utils.col_to_snake(["weightedAverageShsOut", "costOfRevenue"])
        assert result == {
            "weightedAverageShsOut": "weighted_average_shs_out",
            "costOfRevenue": "cost_of_revenue",
        }

    @pytest.mark.unit
    def test_empty_list(self):
        result = utils.col_to_snake([])
        assert result == {}

    @pytest.mark.unit
    def test_full_column_set(self, sample_camel_case_columns):
        result = utils.col_to_snake(sample_camel_case_columns)
        assert len(result) == len(sample_camel_case_columns)
        for original, converted in result.items():
            # No uppercase letters in the result
            assert converted == converted.lower()
            # Original key is preserved
            assert original in sample_camel_case_columns


# ===================================================================
# clean_empty_str_df
# ===================================================================
class TestCleanEmptyStrDf:
    """Tests for cleaning empty strings to None in DataFrames."""

    @pytest.mark.unit
    def test_replaces_empty_strings(self):
        df = pl.DataFrame({"a": ["hello", "", "world"], "b": ["", "foo", ""]})
        result = utils.clean_empty_str_df(df)
        assert result["a"].to_list() == ["hello", None, "world"]
        assert result["b"].to_list() == [None, "foo", None]

    @pytest.mark.unit
    def test_non_string_columns_unchanged(self):
        df = pl.DataFrame({"name": ["a", "", "c"], "value": [1, 2, 3]})
        result = utils.clean_empty_str_df(df)
        assert result["name"].to_list() == ["a", None, "c"]
        assert result["value"].to_list() == [1, 2, 3]

    @pytest.mark.unit
    def test_no_empty_strings(self):
        df = pl.DataFrame({"a": ["hello", "world"]})
        result = utils.clean_empty_str_df(df)
        assert result["a"].to_list() == ["hello", "world"]

    @pytest.mark.unit
    def test_all_empty_strings(self):
        df = pl.DataFrame({"a": ["", "", ""]})
        result = utils.clean_empty_str_df(df)
        assert result["a"].to_list() == [None, None, None]


# ===================================================================
# apply_schema_df
# ===================================================================
class TestApplySchemaDf:
    """Tests for casting DataFrame schemas from source to target."""

    @pytest.mark.unit
    def test_cast_int_to_float(self):
        df_src = pl.DataFrame({"val": [1.0, 2.0, 3.0]})
        df_tgt = pl.DataFrame({"val": [10, 20, 30]})
        result = utils.apply_schema_df(df_src, df_tgt)
        assert result["val"].dtype == pl.Float64

    @pytest.mark.unit
    def test_mismatched_columns_preserved(self):
        """Columns in target but not in source keep their original type."""
        df_src = pl.DataFrame({"a": [1.0]})
        df_tgt = pl.DataFrame({"a": [1], "b": ["hello"]})
        result = utils.apply_schema_df(df_src, df_tgt)
        assert result["a"].dtype == pl.Float64
        assert result["b"].dtype == pl.String

    @pytest.mark.unit
    def test_same_schema_noop(self):
        df_src = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        df_tgt = pl.DataFrame({"a": [3, 4], "b": ["z", "w"]})
        result = utils.apply_schema_df(df_src, df_tgt)
        assert result.schema == df_src.schema


# ===================================================================
# ldict_to_df
# ===================================================================
class TestLdictToDf:
    """Tests for converting list-of-dicts to Polars DataFrame."""

    @pytest.mark.unit
    def test_with_data(self):
        mock_table = MagicMock()
        mock_table.columns.keys.return_value = ["id", "name"]
        ldict = [{"id": 1, "name": "AAPL"}, {"id": 2, "name": "NVDA"}]
        result = utils.ldict_to_df(ldict, mock_table)
        assert len(result) == 2
        assert result["id"].to_list() == [1, 2]

    @pytest.mark.unit
    def test_empty_list(self):
        mock_table = MagicMock()
        mock_table.columns.keys.return_value = ["id", "name"]
        result = utils.ldict_to_df([], mock_table)
        assert len(result) == 0
        assert list(result.columns) == ["id", "name"]


# ===================================================================
# fetch_cpi_df
# ===================================================================
class TestFetchCpiDf:
    """Tests for BLS CPI data fetching (mocked HTTP)."""

    @pytest.mark.unit
    def test_successful_fetch(self, sample_bls_cpi_json):
        import json

        mock_response = MagicMock()
        mock_response.text = json.dumps(sample_bls_cpi_json)

        with patch("pfin_back_etl.utils.requests.post", return_value=mock_response):
            df = utils.fetch_cpi_df("fake_key", 2024, 2024, ["CUUR0000SA0"])

        assert len(df) == 2
        assert "year" in df.columns
        assert "month" in df.columns
        assert "series_value" in df.columns
        assert "series_id" in df.columns
        assert "ref_date" in df.columns
        # Check types
        assert df["year"].dtype == pl.Int64
        assert df["month"].dtype == pl.Int64
        assert df["series_value"].dtype == pl.Float64

    @pytest.mark.unit
    def test_failed_request_raises(self):
        import json

        failed_json = {"status": "REQUEST_FAILED", "Results": {}}
        mock_response = MagicMock()
        mock_response.text = json.dumps(failed_json)

        with patch("pfin_back_etl.utils.requests.post", return_value=mock_response):
            with pytest.raises(Exception, match="unsuccessful"):
                utils.fetch_cpi_df("fake_key", 2024, 2024, ["CUUR0000SA0"])

    @pytest.mark.unit
    def test_month_parsing(self, sample_bls_cpi_json):
        """Verify M12 -> 12 and M11 -> 11 conversion."""
        import json

        mock_response = MagicMock()
        mock_response.text = json.dumps(sample_bls_cpi_json)

        with patch("pfin_back_etl.utils.requests.post", return_value=mock_response):
            df = utils.fetch_cpi_df("fake_key", 2024, 2024, ["CUUR0000SA0"])

        months = sorted(df["month"].to_list())
        assert months == [11, 12]


# ===================================================================
# load_env_variables
# ===================================================================
class TestLoadEnvVariables:
    """Tests for environment variable loading."""

    @pytest.mark.unit
    def test_missing_fmp_key_raises(self):
        with patch("pfin_back_etl.utils.dotenv.load_dotenv"):
            with patch("pfin_back_etl.utils.os.getenv", return_value=None):
                with pytest.raises(ValueError, match="FMP_API_KEY"):
                    utils.load_env_variables("PFIN_")

    @pytest.mark.unit
    def test_missing_bls_key_raises(self):
        def mock_getenv(key):
            if key == "FMP_API_KEY":
                return "fake_fmp_key"
            return None

        with patch("pfin_back_etl.utils.dotenv.load_dotenv"):
            with patch("pfin_back_etl.utils.os.getenv", side_effect=mock_getenv):
                with pytest.raises(ValueError, match="BLS_API_KEY"):
                    utils.load_env_variables("PFIN_")

    @pytest.mark.unit
    def test_successful_load(self):
        env_vars = {
            "FMP_API_KEY": "fmp_test",
            "BLS_API_KEY": "bls_test",
            "PFIN_DB_USER": "user",
            "PFIN_DB_HOST": "localhost",
            "PFIN_DB_PORT": "5432",
            "PFIN_DB_NAME": "postgres",
            "PFIN_DB_PASSWORD": "secret",
        }

        with patch("pfin_back_etl.utils.dotenv.load_dotenv"):
            with patch(
                "pfin_back_etl.utils.os.getenv", side_effect=lambda k: env_vars.get(k)
            ):
                params = utils.load_env_variables("PFIN_")

        assert params["FMP_API_KEY"] == "fmp_test"
        assert params["BLS_API_KEY"] == "bls_test"
        assert params["DB_USER"] == "user"
        assert params["DB_HOST"] == "localhost"
        assert params["DB_PORT"] == "5432"


# ===================================================================
# sqla_modulename_for_table
# ===================================================================
class TestSqlaModuleNameForTable:
    """Tests for SQLAlchemy automap module name resolution."""

    @pytest.mark.unit
    def test_with_schema(self):
        mock_reflect = MagicMock()
        mock_reflect.schema = "pfin"
        result = utils.sqla_modulename_for_table("some_table", None, mock_reflect)
        assert result == "pfin"

    @pytest.mark.unit
    def test_without_schema(self):
        mock_reflect = MagicMock()
        mock_reflect.schema = None
        result = utils.sqla_modulename_for_table("some_table", None, mock_reflect)
        assert result == "public"
