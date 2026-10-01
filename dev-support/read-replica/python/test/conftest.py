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

# conftest.py is a special pytest file. Fixtures and hooks defined here are
# automatically available to all tests in this directory and its subdirectories.

import logging
import os
import shutil

import pytest

from python.src.logger_config import LOG_FORMAT

DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'output')


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    setattr(item, f"rep_{report.when}", report)


def _clear_directory(path):
    if not os.path.isdir(path):
        return
    for entry in os.listdir(path):
        entry_path = os.path.join(path, entry)
        if os.path.isdir(entry_path):
            shutil.rmtree(entry_path)
        else:
            os.remove(entry_path)


@pytest.fixture(autouse=True)
def per_test_log_file(request):
    output_dir = os.environ.get('OUTPUT_DIR', DEFAULT_OUTPUT_DIR)
    test_name = request.node.name
    execution_count = getattr(request.node, 'execution_count', 1)

    run_dir = os.path.join(output_dir, test_name, f"run{execution_count}")
    os.makedirs(run_dir, exist_ok=True)

    log_path = os.path.join(run_dir, f"{test_name}.run{execution_count}.log")

    active_logs_dir = os.environ.get('ACTIVE_CLUSTER_LOGS_DIR')
    replica_logs_dir = os.environ.get('REPLICA_CLUSTER_LOGS_DIR')

    _clear_directory(active_logs_dir)
    _clear_directory(replica_logs_dir)

    handler = logging.FileHandler(log_path, mode='w')
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    handler.setLevel(logging.DEBUG)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    yield

    rep_call = getattr(request.node, 'rep_call', None)
    if rep_call is not None and rep_call.failed and rep_call.longreprtext:
        logging.getLogger().error(
            "TEST FAILED — pytest traceback:\n%s", rep_call.longreprtext
        )

    root_logger.removeHandler(handler)
    handler.close()

    if active_logs_dir and os.path.isdir(active_logs_dir):
        shutil.copytree(active_logs_dir,
                        os.path.join(run_dir, f"hbase-cluster1-run{execution_count}-logs"),
                        dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns('*.out'))
    if replica_logs_dir and os.path.isdir(replica_logs_dir):
        shutil.copytree(replica_logs_dir,
                        os.path.join(run_dir, f"hbase-cluster2-run{execution_count}-logs"),
                        dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns('*.out'))
