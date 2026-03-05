"""
Project:       pfin-back-etl
Author:        Rich Mosko

Description:
    Production entry point for the Personal Finance Backend ETL.
    Creates a PFinBackend instance, runs a full stock screener,
    and updates all tables in the SupaBase database.
"""

import logging
import os
import sys
from datetime import datetime, timezone
from pfin_back_etl import PFinBackend

LOG_FILE = os.path.join(os.getcwd(), "pfin_back_etl.log")


def setup_logging():
    """Configure logging to write to both stdout and a log file."""
    logger = logging.getLogger("pfin_etl")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # Console handler (stdout)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler (append mode)
    file_handler = logging.FileHandler(LOG_FILE, mode="a")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Silence noisy third-party loggers
    logging.getLogger("FMPStab").setLevel(logging.WARNING)

    return logger


def main():
    logger = setup_logging()

    t_start = datetime.now(timezone.utc)
    logger.info(f"Starting ETL run at {t_start.isoformat()}")

    pfb = PFinBackend()
    pfb.update_table_all(sym_list=["NVDA","AAP","IREN","V","GOOGL","META"])

    t_end = datetime.now(timezone.utc)
    elapsed = t_end - t_start
    logger.info(f"Finished at {t_end.isoformat()} (elapsed: {elapsed})")


if __name__ == "__main__":
    main()
