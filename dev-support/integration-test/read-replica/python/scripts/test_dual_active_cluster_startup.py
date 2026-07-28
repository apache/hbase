#!/usr/bin/env python3
"""
Verifies that two clusters cannot both start with read-only mode disabled (both as active clusters)
on the same shared data store. One cluster must fail to start, with the HMaster process not
running, and an error logged to the master log.

Usage: python3 ./python/scripts/test_dual_active_cluster_startup.py
"""
import argparse
import time

from python.src.environment_loader import get_env
from python.src.hbase_docker_client import HBaseDockerClient
from python.src.logger_config import get_logger
from python.src.utils import load_env_and_set_up_clients, log_script_start, log_script_end

logger = get_logger(__name__)

STARTUP_WAIT_SECONDS = 60
EXPECTED_ERROR_MSG = "Another cluster is running in active (read-write) mode on this storage location"


def is_process_running(cluster: HBaseDockerClient, process_name: str) -> bool:
    output = cluster.run_docker_exec_command("jps")
    return process_name in output


def check_cluster_processes(cluster: HBaseDockerClient) -> bool:
    hmaster_running = is_process_running(cluster, "HMaster")
    logger.info(f"  {cluster.name}: HMaster={'running' if hmaster_running else 'down'}")
    return hmaster_running


def assert_error_in_master_log(cluster: HBaseDockerClient):
    logger.info(f"Checking {cluster.name} master log for expected error message")
    log_output = cluster.run_docker_exec_command(
        "cat /opt/hbase/logs/hbase-*-master-*.log || true"
    )
    assert EXPECTED_ERROR_MSG in log_output, (
        f"Expected {cluster.name}'s master log to contain:\n"
        f"  '{EXPECTED_ERROR_MSG}'\n"
        f"but it was not found.\nLog tail:\n{log_output[-2000:]}"
    )
    logger.info(f"  [PASS] Found expected error message in {cluster.name}'s master log")


def main():
    log_script_start(__file__, logger)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--clean-up-containers', action='store_true',
                        help='Stop Docker containers and revert cluster configurations to one '
                             'active cluster and one replica cluster after the test finishes')
    args = parser.parse_args()

    cluster1, cluster2 = load_env_and_set_up_clients()
    data_store_root = get_env("HBASE_DATA_STORE_ROOT")
    docker_compose_file = get_env("DOCKER_COMPOSE_FILE")

    test_iterations = 3
    for i in range(1, test_iterations+1):
        logger.info(f"---------- Iteration {i} ----------")

        HBaseDockerClient.stop_containers(docker_compose_file=docker_compose_file, data_dir=f'{data_store_root}/*',
                                          sudo=True)

        # Make both clusters an active cluster (read-only disabled)
        cluster1.disable_read_only_mode(run_update_all_config=False)
        cluster2.disable_read_only_mode(run_update_all_config=False)

        # Start or restart containers so both attempt to start as active
        HBaseDockerClient.start_or_restart_containers(docker_compose_file=docker_compose_file,
                                                      data_store_root=f'{data_store_root}')

        # Wait for HBase to attempt startup on both containers
        logger.info(f"Waiting {STARTUP_WAIT_SECONDS}s for clusters to attempt startup...")
        time.sleep(STARTUP_WAIT_SECONDS)

        # Determine which cluster failed
        logger.info("Checking HBase processes on both clusters")
        cluster1_running = check_cluster_processes(cluster1)
        cluster2_running = check_cluster_processes(cluster2)

        if cluster1_running and not cluster2_running:
            failed_cluster = cluster2
            running_cluster = cluster1
        elif cluster2_running and not cluster1_running:
            failed_cluster = cluster1
            running_cluster = cluster2
        elif not cluster1_running and not cluster2_running:
            raise RuntimeError("Both clusters appear to be down — this is unexpected")
        else:
            raise RuntimeError(
                "Both clusters appear to be running — the test expects exactly one to have failed. "
                "This may indicate the clusters are using separate data stores or the feature is not working. "
                "Note: There is a rare occasion where this may occur due to a race condition, but it should "
                "not happen often."
            )

        logger.info(f"[PASS] {running_cluster.name} is running as the active cluster")
        logger.info(f"[PASS] {failed_cluster.name} failed to start (HMaster is down)")

        # Verify the failed cluster's master log contains the expected error
        assert_error_in_master_log(failed_cluster)

        logger.info(f"Finished iteration {i} of {test_iterations}")

    logger.info("=" * 70)
    logger.info("TEST PASSED: All dual active cluster startups were correctly rejected")
    logger.info("=" * 70)

    if args.clean_up_containers:
        logger.info("Stopping Docker containers and reverting test environment to having "
                    "one active cluster and one replica cluster")
        HBaseDockerClient.stop_containers(docker_compose_file=docker_compose_file, data_dir=f'{data_store_root}/*',
                                          sudo=True)
        cluster1.disable_read_only_mode(run_update_all_config=False)
        cluster2.enable_read_only_mode(run_update_all_config=False)

    log_script_end(__file__, logger)


if __name__ == '__main__':
    main()
