import argparse
import os
from pathlib import Path
from dotenv import load_dotenv

from logger import (
    Logger as logger
)

class Config:
    """
    Holds all Config variables and set them by environment variables or arguments
    """
    DEV_MODE = False
    STAGE = "unknown"
    CLIENT = None
    PROJECT_ID = None
    DATASET_ID = None
    ENGINE = None
    CRUNCHBASE_BASE_API = None
    CRUNCHBASE_BASE_URL = None
    CRUNCHBASE_API_KEY = None

    # task config
    DO_UPLOAD = False
    DO_DOWNLOAD = False


    # client config
    CRUNBASE_NEEDED = False
    BIGQUERY_NEEDED = False

    def __init__(self):
        """ Init Config """
        ARGS = self.parse_arguments()
        self.load_environment()
        try:
            self.set_general_settings(ARGS)
            if Config.DEV_MODE:
                # set debug settings
                self.set_development_settings()
            else:
                # set production settings
                self.set_production_settings(ARGS)
            # set preprocessing settings
            self.set_preprocessing_settings()
            logger.success("Config set")
        except Exception as e:
            logger.error(f"Config creation error: {e}")
            exit(1)

    def parse_arguments(self):
        """ Parse arguments """
        parser = argparse.ArgumentParser(description='Program for downloading data from Crunchbase.')
        parser.add_argument('--download_flag', action='store_true', help='Flag to enable crunchbase data processing.')
        parser.add_argument('--upload_flag', action='store_true', help='Flag to enable category data processing.')
        parser.add_argument('--project_id', help='BigQuery project ID to ignore the environment variable')
        parser.add_argument('--dataset_id', help='BigQuery dataset ID to ignore the environment variable')
        return parser.parse_args()

    def load_environment(self):
        """
        Load environment variables from .env file
        """
        # check if local env_base exists
        DOTENV_PATH = Path("env_base.env")
        # load it
        if DOTENV_PATH.exists():
            load_dotenv(dotenv_path=DOTENV_PATH)
            logger.info("Environment loaded from env_base.env")
            # debug mode could only set on local machine
            Config.DEV_MODE = os.getenv("MODE") == "DEV"
        else:
            load_dotenv()
            logger.info("Environment loaded from container")

    def set_general_settings(self, args):
        """
        Set general settings

        Args:
            args (dict): Arguments
        """

        if args.project_id:
            Config.PROJECT_ID = args.project_idS
        else:
            Config.PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
        if args.dataset_id:
            Config.DATASET_ID = args.dataset_id
        else:
            Config.DATASET_ID = os.getenv("GOOGLE_DATASET_ID")
       
        Config.STAGE = os.getenv("STAGE")

        # set Crunchbase Config
        Config.CRUNCHBASE_API_KEY = os.getenv("CRUNCHBASE_API_KEY")
        Config.CRUNCHBASE_BASE_API = os.getenv("CRUNCHBASE_BASE_API")
        Config.CRUNCHBASE_BASE_URL = os.getenv("CRUNCHBASE_BASE_URL") 

        Config.DO_DOWNLOAD = args.download_flag


    def set_development_settings(self):
        """
        Set development settings
        Theese settings are only for development to prevent
        upload to BigQuery 
        """
        logger.verbose = True
        logger.debug("Development Mode is enabled")
        # changes for development mode
        Config.DO_UPLOAD = False

    def set_production_settings(self, args):
        """
        Set production settings
        Theese settings are only for production
        and never runs in development mode

        Args:
            args (dict): Arguments
        """
        Config.DO_UPLOAD = args.upload_flag
        Config.DO_DOWNLOAD = args.download_flag

    def set_preprocessing_settings(self):
        """
        Set preprocessing settings
        Theese settings are set after all other settings
        """

        # Crunchbase is needed if this tasks are enabled
        Config.CRUNBASE_NEEDED = any([
            Config.DO_DOWNLOAD
        ])
        # BigQuery is needed if this tasks are enabled
        Config.BIGQUERY_NEEDED = any([
            Config.DO_DOWNLOAD,
            Config.DO_UPLOAD
        ])
