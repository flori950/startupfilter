import time

from bigquery.client import BigQueryClient
from crunchbase.client import CrunchbaseClient

from tasks import (
    crunchbase
)
from config import Config

from logger import Logger as logger


if __name__ == "__main__":

    _s_time = time.time()
    # Step 1: Get Arguments and Environment Variables to set Config

    CONFIG = Config()

    # Step 2: Create Clients depending on Config

    logger.info("Create Clients")

    # create Bigquery Client if needed
    if CONFIG.BIGQUERY_NEEDED:
        logger.log("Creating BigQuery Client")
        BQClient = BigQueryClient(
            project_id=CONFIG.PROJECT_ID,
            dataset_name=CONFIG.DATASET_ID
        )
    else:
        logger.log("BigQuery is not needed")
        BQClient = None

    # create Crunchbase Client if needed
    if CONFIG.CRUNBASE_NEEDED:
        logger.log("Creating Crunchbase Client")
        CRUNCHBASE = CrunchbaseClient(
            CONFIG.CRUNCHBASE_API_KEY,
            CONFIG.CRUNCHBASE_BASE_URL
            # CONFIG.CRUNCHBASE_BASE_API
        )
    else:
        logger.log("Crunchbase is not needed")
        CRUNCHBASE = None

    # Step 3: BigQuery check Dataset (if not exists, create it)

    if CONFIG.BIGQUERY_NEEDED and not BQClient.dataset_exists():
        logger.warning("Dataset does not exist. Creating it now...")
        BQClient.create_dataset()

    # Step 4: do the single jobs to get data and upload

    if CONFIG.DO_DOWNLOAD:
        logger.info("Start Download Job")
        # run job
        crunchbase.run_job(CRUNCHBASE, BQClient, CONFIG.DO_UPLOAD)
        logger.success("Finished Download Job")

    # Programm finished

    _e_time = time.time()
    logger.log(f"Programm finished in {_e_time - _s_time} seconds")
