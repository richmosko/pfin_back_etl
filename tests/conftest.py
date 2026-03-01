"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Shared pytest fixtures and configuration for the test suite.

    Test tiers:
        - unit:        Fast tests with no external dependencies (mocked DB/API)
        - integration: Tests that hit the real database and/or APIs

    Usage:
        uv run pytest -m unit          # fast, no credentials needed
        uv run pytest -m integration   # requires .env with valid credentials
        uv run pytest                  # runs everything
"""

import pytest
import polars as pl


# ---------------------------------------------------------------------------
# Session-scoped backend fixture (shared across all integration tests)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def backend():
    """
    Create a single PFinBackend instance for all integration tests.
    This avoids re-connecting to the database for every test function.
    Skips all integration tests if the connection fails.
    """
    try:
        import pfin_back_etl as pfbe

        pfb = pfbe.PFinBackend()
        return pfb
    except Exception as e:
        pytest.skip(f"Could not connect to database: {e}")


# ---------------------------------------------------------------------------
# Sample data fixtures for unit tests
# ---------------------------------------------------------------------------
@pytest.fixture
def sample_fmp_income_json():
    """Sample FMP income statement API response (single record)."""
    return [
        {
            "date": "2024-09-30",
            "symbol": "AAPL",
            "reportedCurrency": "USD",
            "cik": "0000320193",
            "fillingDate": "2024-11-01",
            "acceptedDate": "2024-11-01 06:01:36",
            "calendarYear": "2024",
            "period": "Q4",
            "revenue": 94930000000,
            "costOfRevenue": 52553000000,
            "grossProfit": 42377000000,
            "operatingIncome": 29592000000,
            "netIncome": 14736000000,
            "eps": 0.97,
            "epsdiluted": 0.97,
            "weightedAverageShsOut": 15204137000,
            "weightedAverageShsOutDil": 15204137000,
        }
    ]


@pytest.fixture
def sample_fmp_profile_json():
    """Sample FMP company profile API response."""
    return [
        {
            "symbol": "AAPL",
            "companyName": "Apple Inc.",
            "currency": "USD",
            "exchange": "NASDAQ",
            "exchangeShortName": "NASDAQ",
            "industry": "Consumer Electronics",
            "sector": "Technology",
            "country": "US",
            "mktCap": 3500000000000,
            "price": 230.50,
            "beta": 1.24,
            "volAvg": 55000000,
            "lastDiv": 1.00,
            "ipoDate": "1980-12-12",
            "description": "Apple Inc. designs, manufactures...",
        }
    ]


@pytest.fixture
def sample_bls_cpi_json():
    """Sample BLS CPI API response structure."""
    return {
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [
                {
                    "seriesID": "CUUR0000SA0",
                    "data": [
                        {
                            "year": "2024",
                            "period": "M12",
                            "periodName": "December",
                            "value": "315.605",
                            "footnotes": [{}],
                        },
                        {
                            "year": "2024",
                            "period": "M11",
                            "periodName": "November",
                            "value": "315.493",
                            "footnotes": [{}],
                        },
                    ],
                }
            ]
        },
    }


@pytest.fixture
def sample_camel_case_columns():
    """Sample camelCase column names for testing snake_case conversion."""
    return [
        "reportedCurrency",
        "fillingDate",
        "acceptedDate",
        "calendarYear",
        "costOfRevenue",
        "grossProfit",
        "operatingIncome",
        "netIncome",
        "weightedAverageShsOut",
    ]


@pytest.fixture
def sample_df_old():
    """Sample 'existing' dataframe for testing row isolation logic."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "symbol": ["AAPL", "NVDA", "GOOGL"],
            "description": ["Apple Inc.", "NVIDIA Corp.", "Alphabet Inc."],
        }
    )


@pytest.fixture
def sample_df_new():
    """Sample 'new' dataframe with some overlapping and some new entries."""
    return pl.DataFrame(
        {
            "symbol": ["AAPL", "NVDA", "META", "MSFT"],
            "description": [
                "Apple Inc.",
                "NVIDIA Corporation",  # updated description
                "Meta Platforms",  # new entry
                "Microsoft Corp.",  # new entry
            ],
        }
    )
