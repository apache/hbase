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
Tests table creation behavior for read-replica clusters. It creates a table on the active
cluster, and then runs refresh_meta on the replica cluster and verifies the table's existence.
It does a similar process when dropping the table on the active cluster. It also verifies
tables cannot be created/dropped on the replica cluster.
"""
import argparse

from python.src.logger_config import get_logger
from python.src.utils import (add_common_drop_existing_tables_arg, add_common_new_containers_arg, clean_up_tables,
                              log_script_start, log_script_end, reset_docker_container_environment)

logger = get_logger(__name__)


def test_table_creation_behavior(active_cluster, replica_cluster, table_name, column_family):
    # We should not be able to create a new table on the read-replica cluster
    replica_cluster.assert_read_only_error_occurs('create', table_name, column_family)

    active_cluster.create_table(table_name, column_family)

    # Read-Replica cluster should not see the newly created table yet
    logger.info(f"Verifying {active_cluster.name} now has table '{table_name}', "
                f"while {replica_cluster.name} cluster does not")
    active_cluster.assert_table_exists(table_name)
    replica_cluster.assert_table_does_not_exist(table_name)

    # Read-Replica cluster should now see the newly created table
    replica_cluster.refresh_meta()
    logger.info(f"Verifying {replica_cluster.name} has table '{table_name}' after refreshing meta")
    replica_cluster.assert_table_exists(table_name)
    active_cluster.assert_table_exists(table_name)

    # Cannot drop the table on the Read-Replica cluster. A WriteAttemptedOnReadOnlyClusterException should occur
    replica_cluster.disable_table(table_name)
    replica_cluster.assert_read_only_error_occurs('drop', table_name, column_family)
    # The table should still exist on the read-replica cluster since drops are not allowed
    replica_cluster.assert_table_exists(table_name)

    # Drop the table on the active cluster
    active_cluster.disable_table(table_name)
    active_cluster.drop_table(table_name)

    # The read-replica cluster should still have the table that was dropped on the active
    # cluster since 'refresh_meta' has not been run yet.
    logger.info(f"Verifying {replica_cluster.name} still has table '{table_name}'")
    active_cluster.assert_table_does_not_exist(table_name)
    replica_cluster.assert_table_exists(table_name)

    # The read-replica cluster no longer has the dropped table after running 'refresh_meta'.
    logger.info(f"Verifying {replica_cluster.name} no longer has table '{table_name}' after "
                f"refreshing meta")
    replica_cluster.refresh_meta()
    replica_cluster.assert_table_does_not_exist(table_name)


def run_test(drop_existing_tables: bool = False, new_containers: bool = False):
    start_time = log_script_start(__file__, logger)

    active_cluster, replica_cluster = reset_docker_container_environment(
        new_containers=new_containers,
        cluster1_name="Active Cluster",
        cluster2_name="Replica Cluster"
    )

    table_name = "t1"
    column_family = "cf"
    if drop_existing_tables:
        clean_up_tables(active_cluster, replica_cluster)

    test_table_creation_behavior(active_cluster, replica_cluster, table_name, column_family)

    log_script_end(__file__, logger, start_time)


def main(args=None):
    parser = argparse.ArgumentParser()
    parser = add_common_drop_existing_tables_arg(parser)
    parser = add_common_new_containers_arg(parser)
    parsed_args = parser.parse_args(args)

    run_test(
        drop_existing_tables=parsed_args.drop_existing_tables,
        new_containers=parsed_args.new_containers
    )


if __name__ == "__main__":
    main()