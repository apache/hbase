#!/usr/bin/env python3

import argparse
import os
import time

from dotenv import load_dotenv

import python.proto.generated.ActiveClusterSuffix_pb2 as acs

from python.src.environment_loader import get_env
from python.src.hbase_docker_client import HBaseDockerClient
from python.src.logger_config import get_logger

logger = get_logger(__name__)


def log_script_start(file: str, script_logger=None):
    (script_logger or logger).info(f"========== START {os.path.basename(file)} ==========")
    return time.time()


def log_script_end(file: str, script_logger=None, start_time=None):
    elapsed = ""
    if start_time is not None:
        total_seconds = int(time.time() - start_time)
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        elapsed = f" ({hours}h {minutes}m {seconds}s)"
    (script_logger or logger).info(f"========== END {os.path.basename(file)}{elapsed} ==========")


def add_common_skip_table_cleanup_arg(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument('-t', '--skip-table-cleanup-on-start', action='store_true',
                        help='Skip cleaning up tables at the start of the test')
    return parser


def add_common_skip_container_stop_or_restart_arg(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument('-s', '--skip-container-start-or-restart', action='store_true',
                        help='Skip stopping, starting, and waiting for the Docker containers to be ready')
    return parser


def load_env_and_set_up_clients(cluster1_name: str = "Cluster 1",
                                cluster2_name: str = "Cluster 2") -> tuple[HBaseDockerClient, HBaseDockerClient]:
    load_dotenv()
    container_name = get_env("HBASE_CONTAINER_NAME")

    active_cluster = HBaseDockerClient(container_name=container_name,
                                       local_conf=f"{get_env('ACTIVE_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                       hbase_ui_port=get_env('ACTIVE_CLUSTER_PORT'),
                                       cluster_name=cluster1_name, hbase_host=get_env('HBASE_HOST'))
    replica_cluster = HBaseDockerClient(container_name=f'{container_name}-2',
                                        local_conf=f"{get_env('REPLICA_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                        hbase_ui_port=get_env('REPLICA_CLUSTER_PORT'),
                                        cluster_name=cluster2_name, hbase_host=get_env('HBASE_HOST'))
    return active_cluster, replica_cluster


def run_put_and_get(cluster: HBaseDockerClient, table: str, row: str, cf: str, data: str):
    cluster.put(table, row, cf, data)
    cluster.assert_table_row_count(table, expected_row_count=1)
    return cluster.assert_get_output(table, row, cf, expected_data=data)


def assert_crud_operations_work_on_active_cluster(cluster: HBaseDockerClient):
    table = 'crud-test-table1'
    cf = 'cf'
    row = 'r1'
    data = '1'

    # Create
    cluster.create_table(table, cf)
    cluster.assert_table_exists(table)

    # Retrieve
    run_put_and_get(cluster, table, row, cf, data)

    # "Update" (there are no true updates in HBase)
    data = '2'
    run_put_and_get(cluster, table, row, cf, data)

    # Delete
    # This row has two versions. This only deletes the first version
    cluster.delete(table, row, column=f"{cf}:")
    cluster.assert_table_row_count(table, expected_row_count=1)
    cluster.assert_get_output(table, row, cf, expected_data='1')

    # Delete the final version
    cluster.delete(table, row, column=f"{cf}:")
    cluster.assert_table_row_count(table, expected_row_count=0)

    # Drop table
    cluster.disable_table(table)
    cluster.drop_table(table)
    cluster.assert_table_does_not_exist(table)


def assert_correct_active_cluster_suffix(cluster: HBaseDockerClient, data_store_root: str):
    logger.info(f"Verifying active cluster suffix file matches 'hbase.meta.table.suffix' "
                f"in conf file for {cluster.name}")
    active_cluster_file = f'{data_store_root}/data-store/hbase/active.cluster.suffix.id'
    active_cluster_suffix = acs.ActiveClusterSuffix()

    # The active cluster suffix file may not get created right away
    retries = 0
    while not os.path.exists(active_cluster_file):
        if retries >= 5:
            raise RuntimeError(f"Timed out waiting for active cluster file to exist: {active_cluster_file}")
        logger.info(f"Waiting for active cluster file to exist: {active_cluster_file}")
        time.sleep(1)
        retries += 1

    # Parse the active cluster suffix protobuf message file
    with open(active_cluster_file, 'rb') as f:
        data = f.read()
        header = b'PBUF'
        if data.startswith(header):
            active_cluster_suffix.ParseFromString(data[len(header):])
        else:
            active_cluster_suffix.ParseFromString(data)
        actual_suffix = active_cluster_suffix.suffix

    # Assume the meta table suffix is blank if hbase.meta.table.suffix does not exist in HBase conf
    expected_suffix = cluster.get_hbase_conf_property_value('hbase.meta.table.suffix')
    if expected_suffix is None:
        expected_suffix = ''

    # Verify the active cluster suffix file has the expected meta table suffix
    assert actual_suffix == expected_suffix, (f"Expected {cluster.name} to have meta table suffix '{expected_suffix}', "
                                              f"but got '{actual_suffix}' instead")


def reset_cluster_setup(active_cluster: HBaseDockerClient, replica_cluster: HBaseDockerClient,
                        skip_container_restart: bool, docker_compose_file: str, data_store_root: str, sudo=False):
    """
    Resets the Read-Replica cluster setup where one cluster is the active cluster (read-write mode) and the other
    cluster is the replica cluster (read-only mode).
    """
    if not skip_container_restart:
        HBaseDockerClient.stop_containers(docker_compose_file=docker_compose_file, data_dir=data_store_root, sudo=sudo)

    # If the containers are still running, then we need to run update_all_config in the HBase shell to update
    # read-only mode on each cluster. Otherwise, we can just modify the conf files and the containers will be restarted
    # in the desired read-only mode.
    if skip_container_restart:
        run_update_all_config = True
    else:
        run_update_all_config = False

    # First, make sure both clusters are read-only to prevent an error due to trying to have two active clusters
    active_cluster.enable_read_only_mode(run_update_all_config=run_update_all_config)
    replica_cluster.enable_read_only_mode(run_update_all_config=run_update_all_config)

    # Now activate read-write mode on our active cluster
    active_cluster.disable_read_only_mode(run_update_all_config=run_update_all_config)

    if not skip_container_restart:
        HBaseDockerClient.start_or_restart_containers(docker_compose_file=docker_compose_file,
                                                      data_store_root=f'{data_store_root}')
        HBaseDockerClient.wait_for_clusters_to_start([active_cluster, replica_cluster])


def clean_up_tables(active_cluster: HBaseDockerClient, replica_cluster: HBaseDockerClient) -> None:
    """
    Drops all tables on the active cluster and then runs 'refresh_meta' on the
    read-replica cluster to remove those tables
    """
    tables = active_cluster.list_tables()
    if tables:
        logger.info(f"Removing all existing tables on {active_cluster.name}: {tables}")
        for table in tables:
            active_cluster.disable_table(table)
            active_cluster.drop_table(table)
        logger.info(f"Running 'refresh_meta' and 'refresh_hfiles' on {replica_cluster.name} to sync it with "
                    f"{active_cluster.name}")
        replica_cluster.refresh_meta()
        replica_cluster.refresh_hfiles()


def swap_cluster_roles(new_active_cluster, new_replica_cluster, run_update_all_config=True):
    logger.info(f"Making {new_active_cluster.name} the active cluster and "
                f"{new_replica_cluster.name} the replica cluster")
    new_replica_cluster.enable_read_only_mode(run_update_all_config=run_update_all_config)
    new_active_cluster.disable_read_only_mode(run_update_all_config=run_update_all_config)


def create_table_on_active_cluster(active_cluster: HBaseDockerClient, column_family: str):
    """Create a new table on the active cluster and assert it exists"""
    tables = active_cluster.list_tables()
    new_table = f't{len(tables)+1}'
    active_cluster.create_table(new_table, column_family)
    active_cluster.assert_table_exists(new_table)
    return new_table


def add_data_to_each_table_on_active_cluster(active_cluster: HBaseDockerClient, tables: list, column_family: str):
    """Add data to each table in the active cluster"""
    for i, table in enumerate(tables[::-1], 1):
        active_cluster.put(table, f'r{i}', column_family, i)
        active_cluster.flush(table)


def refresh_replica_and_verify_tables(replica_cluster: HBaseDockerClient, new_table: str, tables: list):
    """
    Refresh meta and HFiles on the replica cluster, and verify the new table
    exists and each table has the correct number of rows
    """
    replica_cluster.refresh_meta_and_hfiles()
    replica_cluster.assert_table_exists(new_table)
    for i, table in enumerate(tables[::-1], 1):
        replica_cluster.assert_table_row_count(table, i)


def create_table_and_test_active_and_replica_clusters(active_cluster: HBaseDockerClient,
                                                      replica_cluster: HBaseDockerClient,
                                                      column_family: str):
    """
    Creates a new table and iteratively adds data to each existing table, including the new one.
    Also verifies expected behavior for the replica cluster, such as verifying the new table is not
    on the replica before refreshing meta, and then verify new table and data existence after
    refreshing meta and HFiles.
    """
    new_table = create_table_on_active_cluster(active_cluster, column_family)

    # The new table should not exist on the replica cluster before refreshing meta
    replica_cluster.assert_table_does_not_exist(new_table)

    tables = active_cluster.list_tables()
    # HBase sorts table list by string: ['t1', 't10', 't2, ..., 't9']
    # We want the list sorted by creation time, so we're sorting on the integer: ['t1', 't2, ..., 't9', 't10']
    tables.sort(key=lambda x: int(x[1:]))
    add_data_to_each_table_on_active_cluster(active_cluster, tables, column_family)
    refresh_replica_and_verify_tables(replica_cluster, new_table, tables)
