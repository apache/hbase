import logging
import sys

from dotenv import load_dotenv
from .environment_loader import get_env

LOG_FORMAT = '%(asctime)s %(levelname)-5s %(module)s.%(funcName)s(%(lineno)d): %(message)s'

# Load settings from .env file
load_dotenv()


def configure_logging(level=get_env('LOG_LEVEL')):
    """
    Centralized logging configuration for HBase testing scripts.
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-5s %(module)s.%(funcName)s(%(lineno)d): %(message)s',
        level=level,
        handlers=[
            logging.StreamHandler(sys.stdout)  # Ensures logs show up in GH Actions console
        ]
    )


def get_logger(name):
    """
    Helper to get a logger. This can be used to ensure the config
    is applied whenever a logger is requested.
    """
    # If the root logger has no handlers, configure it now
    if not logging.getLogger().hasHandlers():
        configure_logging()
    return logging.getLogger(name)
