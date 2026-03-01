"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Production entry point for the Personal Finance Backend ETL.
    Creates a PFinBackend instance, runs a full stock screener,
    and updates all tables in the SupaBase database.
"""

from datetime import datetime, timezone
from pfin_back_etl import PFinBackend


def main():
    t_start = datetime.now(timezone.utc)
    print(f"pfin-back-etl: Starting ETL run at {t_start.isoformat()}")

    pfb = PFinBackend()
    pfb.update_table_all()

    t_end = datetime.now(timezone.utc)
    elapsed = t_end - t_start
    print(f"pfin-back-etl: Finished at {t_end.isoformat()} (elapsed: {elapsed})")


if __name__ == "__main__":
    main()
