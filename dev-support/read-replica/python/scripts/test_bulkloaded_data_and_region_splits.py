#!/usr/bin/env python3
"""
This script tests bulk-loading data with Read-Replica HBase clusters.
"""
import argparse
import time

from python.src import get_logger, HBaseDockerClient
from python.src.environment_loader import get_env
from python.src.hbase_docker_client import DockerExecCommandError
from python.src.utils import (add_common_skip_container_stop_or_restart_arg, reset_cluster_setup,
                              load_env_and_set_up_clients, swap_cluster_roles,
                              log_script_start, log_script_end)

logger = get_logger(__name__)


class Bulkloader:
    def __init__(self, bulkload_script: str):
        self.bulkload_script = bulkload_script

    def bulkload_data(self, active_cluster: HBaseDockerClient, table_name: str, column_family: str = 'cf',
                      num_rows: int = 500, initial_row_num: int = 0):
        logger.info(f"Running {self.bulkload_script} to bulkload {num_rows} rows into table '{table_name}' "
                    f"on {active_cluster.name}, starting with row {initial_row_num}")
        active_cluster.run_docker_exec_command(
            f"{self.bulkload_script} {table_name} {column_family} -n {num_rows} -i {initial_row_num}")


def assert_cannot_bulkload_data_onto_replica(bulkloader: Bulkloader, replica_cluster: HBaseDockerClient):
    logger.info(f"Verifying data cannot be loaded onto {replica_cluster.name} because read-only mode is enabled")
    try:
        bulkloader.bulkload_data(replica_cluster, table_name='replica-blt1', column_family='cf')
        raise RuntimeError(f"Expected bulkloading data onto replica cluster {replica_cluster.name} "
                           f"to result in an error")
    except DockerExecCommandError as e:
        expected_error_msg = ("org.apache.hadoop.hbase.WriteAttemptedOnReadOnlyClusterException: "
                              "Operation not allowed in Read-Only Mode")
        assert expected_error_msg in str(e), (f"Expected exception to contain the following error message after "
                                              f"attempting to bulkload data on to a replica cluster:\n"
                                              f"{expected_error_msg}\n"
                                              f"The actual exception was:\n{e}")
        logger.info(f"Bulkload onto replica cluster {replica_cluster.name} failed as expected")


def assert_cannot_split_regions_on_replica(replica_cluster: HBaseDockerClient, table: str):
    logger.info(f"Verifying regions cannot be split on {replica_cluster.name} because read-only mode is enabled")
    try:
        replica_cluster.split(table)
        raise RuntimeError(f"Expected region split on replica cluster {replica_cluster.name} to result in an error")
    except DockerExecCommandError as e:
        expected_error_msg = ("org.apache.hadoop.hbase.WriteAttemptedOnReadOnlyClusterException: "
                              "Operation not allowed in Read-Only Mode")
        assert expected_error_msg in str(e), (f"Expected exception to contain the following error message after "
                                              f"attempting to split a region on a replica cluster:\n"
                                              f"{expected_error_msg}\n"
                                              f"The actual exception was:\n{e}")
        logger.info(f"Region splitting on replica cluster {replica_cluster.name} failed as expected")


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
    container_utils_dir = get_env("CONTAINER_UTILS_DIR")

    table1 = 'blt1'
    table2 = 'blt2'
    table3 = 'blt3'
    tables = [table1, table2, table3]

    bulkloader = Bulkloader(bulkload_script=f"{container_utils_dir}/bulkload.sh")

    reset_cluster_setup(active_cluster=cluster1, replica_cluster=cluster2,
                        skip_container_restart=skip_container_restart, docker_compose_file=docker_compose_file,
                        data_store_root=data_store_root)

    logger.info(f"The active cluster is {cluster1.name} and the replica cluster is {cluster2.name}")

    assert_cannot_bulkload_data_onto_replica(bulkloader, replica_cluster=cluster2)

    # Bulkload data to active cluster and verify the data is there
    logger.info(f"Bulkloading data to '{table1}' on the active cluster and verifying the data is there")
    bulkloader.bulkload_data(active_cluster=cluster1, table_name=table1)
    cluster1.assert_table_exists(table1)
    cluster1.assert_table_row_count(table1, expected_row_count=500)

    # Replica cluster should not see bulkloaded data until meta and HFiles have been refreshed
    logger.info(f"The replica cluster {cluster2.name} should not see bulkloaded data until "
                f"meta and HFiles have been refreshed")
    cluster2.assert_table_does_not_exist(table1)
    cluster2.refresh_meta_and_hfiles()
    cluster2.assert_table_exists(table1)
    cluster2.assert_table_row_count(table1, expected_row_count=500)

    # Cluster 1 is now a replica and Cluster 2 is now the active cluster
    swap_cluster_roles(new_active_cluster=cluster2, new_replica_cluster=cluster1)

    # Bulkload more data into the existing table on Cluster 2
    logger.info(f"Bulkloading more data into the existing table on {cluster2.name}")
    bulkloader.bulkload_data(active_cluster=cluster2, table_name=table1, num_rows=300, initial_row_num=500)
    cluster2.assert_table_row_count(table1, expected_row_count=800)

    # Cluster 1 should not see the newly bulkloaded data until its meta and HFiles have been refreshed
    logger.info(f"The replica cluster {cluster1.name} should not see bulkloaded data until "
                f"meta and HFiles have been refreshed")
    cluster1.assert_table_row_count(table1, expected_row_count=500)
    cluster1.refresh_meta_and_hfiles()
    cluster1.assert_table_row_count(table1, expected_row_count=800)

    # Bulkload data into a new table on Cluster 2
    logger.info(f"Bulkloading data into new table '{table2}' on active cluster {cluster2.name}")
    bulkloader.bulkload_data(active_cluster=cluster2, table_name=table2, num_rows=600)
    cluster2.assert_table_exists(table2)
    cluster2.assert_table_row_count(table2, expected_row_count=600)

    # Cluster 1 should not see this new table until after refreshing meta and HFiles
    logger.info(f"The replica cluster {cluster1.name} should not see '{table2}' until after refreshing meta and HFiles")
    cluster1.assert_table_does_not_exist(table2)
    cluster1.refresh_meta_and_hfiles()
    cluster1.assert_table_exists(table2)
    cluster1.assert_table_row_count(table2, expected_row_count=600)
    cluster1.assert_table_row_count(table1, expected_row_count=800)

    # Cluster 1 is back to being the active cluster and Cluster 2 is once again the replica cluster
    swap_cluster_roles(new_active_cluster=cluster1, new_replica_cluster=cluster2)

    # Bulkload data onto both existing tables, and a new third table
    logger.info(f"Bulkloading data onto '{table1}' and '{table2}', as well as a new table '{table3}'")
    bulkloader.bulkload_data(active_cluster=cluster1, table_name=table1, num_rows=400, initial_row_num=800)
    bulkloader.bulkload_data(active_cluster=cluster1, table_name=table2, num_rows=600, initial_row_num=600)
    bulkloader.bulkload_data(active_cluster=cluster1, table_name=table3, num_rows=1200)
    for table in tables:
        cluster1.assert_table_row_count(table, expected_row_count=1200)

    # Cluster 2 should see the old row counts for the existing tables. It won't see the new table
    # or the updated row counts until after its meta and HFiles have been refreshed.
    logger.info(f"The replica cluster {cluster2.name} should not see '{table3}' or updated values for "
                f"'{table1}' and '{table2}' until after refreshing meta and HFiles")
    cluster2.assert_table_row_count(table1, expected_row_count=800)
    cluster2.assert_table_row_count(table2, expected_row_count=600)
    cluster2.assert_table_does_not_exist(table3)
    cluster2.refresh_meta_and_hfiles()
    for table in tables:
        cluster2.assert_table_row_count(table, expected_row_count=1200)

    # Cluster 2 is now the active cluster and Cluster 1 is the replica cluster
    swap_cluster_roles(new_active_cluster=cluster2, new_replica_cluster=cluster1)

    # Split regions on two tables on the active cluster
    for table in [table1, table2]:
        logger.info(f"Splitting table '{table}' on {cluster2.name}")
        cluster2.flush_and_split(table)
        cluster2.major_compact_and_wait(table)
        cluster2.catalogjanitor_run()
        time.sleep(5)
        cluster2.assert_region_count_for_table(table, expected_region_count=2)

    # Bulkload more rows into each table on Cluster 2 and into a new table
    table4 = 'blt4'
    logger.info(f"Bulkloading data onto '{table1}', '{table2}', and '{table3}', as well as a new table '{table4}'")
    for table in tables:
        bulkloader.bulkload_data(active_cluster=cluster2, table_name=table, num_rows=1200, initial_row_num=1200)
    bulkloader.bulkload_data(active_cluster=cluster2, table_name=table4, num_rows=2400)
    tables.append(table4)
    for table in tables:
        cluster2.assert_table_row_count(table, expected_row_count=2400)

    # The replica cluster should see the old row counts for the existing tables. It won't see the new table
    # or the updated row counts until after its meta and HFiles have been refreshed.
    logger.info(f"The replica cluster {cluster1.name} should not see '{table4}' or row counts/region splits for "
                f"'{table1}', '{table2}', and '{table3}' until after refreshing meta and HFiles")
    for table in tables[:-1]:
        cluster1.assert_table_row_count(table, expected_row_count=1200)
    cluster1.assert_table_does_not_exist(table4)

    # The replica cluster should not see any region splits until after refreshing meta and HFiles
    for table in tables[:-1]:
        cluster1.assert_region_count_for_table(table, expected_region_count=1)

    # The replica cluster will now see updated row counts and region splits
    cluster1.refresh_meta_and_hfiles()
    logger.info(f"Replica cluster {cluster1.name} should now see updated row counts and region splits")
    for table, num_regions in zip(tables, [2, 2, 1, 1]):
        cluster1.assert_table_row_count(table, expected_row_count=2400)
        cluster1.assert_region_count_for_table(table, expected_region_count=num_regions)

    # Make Cluster 1 the active cluster and Cluster 2 the replica cluster
    swap_cluster_roles(new_active_cluster=cluster1, new_replica_cluster=cluster2)

    assert_cannot_split_regions_on_replica(replica_cluster=cluster2, table=table3)

    # Split regions on the active cluster. The replica cluster won't see the updated region count until meta and HFiles
    # have been refreshed
    for table, num_regions in zip(tables, [4, 4, 2, 2]):
        cluster1.flush_and_split(table)
        cluster1.major_compact(table)
        cluster1.catalogjanitor_run()
        time.sleep(5)
        cluster1.assert_region_count_for_table(table, num_regions)

        # Replica cluster still has old region count
        cluster2.assert_region_count_for_table(table, num_regions/2)

        # Update the replica cluster and verify new region count
        cluster2.refresh_meta_and_hfiles()
        cluster2.assert_region_count_for_table(table, num_regions)

    log_script_end(__file__, logger, start_time)


if __name__ == '__main__':
    main()
