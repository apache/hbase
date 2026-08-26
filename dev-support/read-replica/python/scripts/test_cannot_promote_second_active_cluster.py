#!/usr/bin/env python3
"""
Verifies a cluster cannot be promoted to an active cluster when another active cluster already exists.

The test starts with two Read-Replica HBase clusters, where one cluster is the active cluster and the other cluster is
the replica cluster. The test tries to promote the replica cluster to a second active cluster and expects an error to
occur. It then verifies this "second active cluster" is still in read-only mode and that data can still be added to the
actual active cluster.

This test script verifies the fix for:

HBASE-30220: A replica cluster can have read-only mode disabled even when another active cluster already exists
https://issues.apache.org/jira/browse/HBASE-30220

Before implementing the fix for HBASE-30220, a cluster could be promoted to from a replica cluster to an active cluster
even when another active cluster already existed.
"""
import argparse

from python.src.environment_loader import get_env
from python.src.hbase_docker_client import HBaseDockerClient, DockerExecCommandError
from python.src.logger_config import get_logger
from python.src.utils import (assert_crud_operations_work_on_active_cluster, assert_correct_active_cluster_suffix,
                              add_common_skip_container_stop_or_restart_arg, clean_up_tables, reset_cluster_setup,
                              load_env_and_set_up_clients, create_table_and_test_active_and_replica_clusters,
                              log_script_start, log_script_end)
from time import sleep

logger = get_logger(__name__)

COLUMN_FAMILY = "cf"
EXPECTED_ERROR_MSG = ("ReadOnlyTransitionException: Cannot disable read-only mode because another active cluster "
                      "already exists on this storage location. The read-only coprocessors have not been removed.")


def assert_error_when_trying_to_have_second_active_cluster(replica_cluster: HBaseDockerClient, expected_error: str):
    try:
        replica_cluster.disable_read_only_mode()
        raise RuntimeError(f"Expected an DockerExecCommandError with the following error message:\n\n"
                           f"{expected_error}")
    except DockerExecCommandError as e:
        assert expected_error in str(e), (f"Expected DockerExecCommandError to contain the following message:\n\n"
                                          f"{str(expected_error)}\n\n"
                                          f"Got the following message instead:\n\n{str(e)}")
        logger.info(f"Successfully prevented {replica_cluster.name} from becoming a second active cluster")


def run_test_iteration(active_cluster: HBaseDockerClient, replica_cluster: HBaseDockerClient, data_root: str):
    create_table_and_test_active_and_replica_clusters(active_cluster, replica_cluster, column_family='cf')
    assert_error_when_trying_to_have_second_active_cluster(replica_cluster, EXPECTED_ERROR_MSG)

    # Cluster should still be in read-only mode after failed transition from read-only to read-write mode
    replica_cluster.assert_read_only_error_occurs('create', 'test_table', COLUMN_FAMILY)

    assert_crud_operations_work_on_active_cluster(active_cluster)

    # Demote active cluster to replica and promote original replica to be the new active cluster
    active_cluster.enable_read_only_mode()
    replica_cluster.disable_read_only_mode()
    active_cluster = replica_cluster

    # Wait for active cluster file to be updated and verify its contents
    sleep(3)
    assert_correct_active_cluster_suffix(active_cluster, data_root)


def main():
    start_time = log_script_start(__file__, logger)
    parser = argparse.ArgumentParser()
    parser = add_common_skip_container_stop_or_restart_arg(parser)
    args = parser.parse_args()

    skip_container_restart = args.skip_container_start_or_restart

    if skip_container_restart:
        logger.info("Docker containers will NOT be started/restarted at the beginning of this test run")
    else:
        logger.info("Docker containers will be started/restarted at the beginning of this test run")

    cluster1, cluster2 = load_env_and_set_up_clients()
    data_store_root = get_env("HBASE_DATA_STORE_ROOT")
    docker_compose_file = get_env("DOCKER_COMPOSE_FILE")

    reset_cluster_setup(active_cluster=cluster1, replica_cluster=cluster2,
                        skip_container_restart=skip_container_restart, docker_compose_file=docker_compose_file,
                        data_store_root=data_store_root)

    assert_correct_active_cluster_suffix(cluster1, data_store_root)
    clean_up_tables(active_cluster=cluster1, replica_cluster=cluster2)

    test_iterations = 5
    for i in range(1, test_iterations+1):
        logger.info(f"---------- Iteration {i} ----------")
        if i % 2 == 1:
            run_test_iteration(active_cluster=cluster1, replica_cluster=cluster2, data_root=data_store_root)
        else:
            run_test_iteration(active_cluster=cluster2, replica_cluster=cluster1, data_root=data_store_root)
        logger.info(f"Finished iteration {i} of {test_iterations}")
    log_script_end(__file__, logger, start_time)


if __name__ == '__main__':
    main()
