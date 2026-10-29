#!/usr/bin/env python3
#
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

"""
Verifies the hbase-docker containers have started properly. For each cluster, the script first
curls the HBase UI until it receives a 200 response and then gets the server status to verify
there are no dead clusters
"""
import argparse

from python.src import get_env
from python.src.hbase_docker_client import HBaseDockerClient
from python.src.logger_config import get_logger
from python.src.utils import (load_env_and_set_up_clients, log_script_start, log_script_end,
                              add_common_skip_container_stop_or_restart_arg)

logger = get_logger(__name__)


def main():
    start_time = log_script_start(__file__, logger)

    parser = argparse.ArgumentParser()
    parser = add_common_skip_container_stop_or_restart_arg(parser)
    args = parser.parse_args()

    active_cluster, replica_cluster = load_env_and_set_up_clients(cluster1_name="Active Cluster",
                                                                  cluster2_name="Read-Replica Cluster")
    data_store_root = get_env("HBASE_DATA_STORE_ROOT")
    docker_compose_file = get_env("DOCKER_COMPOSE_FILE")

    if not args.skip_container_start_or_restart:
        HBaseDockerClient.start_or_restart_containers(docker_compose_file=docker_compose_file,
                                                      data_store_root=f'{data_store_root}')

    HBaseDockerClient.wait_for_clusters_to_start([active_cluster, replica_cluster])

    log_script_end(__file__, logger, start_time)


if __name__ == "__main__":
    main()
