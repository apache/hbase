# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
    # Convert string level ('DEBUG', 'INFO') to integer level (10, 20)
    if isinstance(level, str):
        numeric_level = logging.getLevelName(level.upper())
    else:
        numeric_level = level

    logging.basicConfig(
        format=LOG_FORMAT,
        level=numeric_level,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Suppress verbose HTTP connection logs from Python Docker SDK and urllib3
    # when the root logger is set to DEBUG or more verbose.
    if numeric_level <= logging.DEBUG:
        logging.getLogger("urllib3").setLevel(logging.INFO)
        logging.getLogger("docker").setLevel(logging.INFO)


def get_logger(name):
    """
    Helper to get a logger. This can be used to ensure the config
    is applied whenever a logger is requested.
    """
    # If the root logger has no handlers, configure it now
    if not logging.getLogger().hasHandlers():
        configure_logging()
    return logging.getLogger(name)
