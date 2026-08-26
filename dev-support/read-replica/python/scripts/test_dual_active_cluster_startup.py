#!/usr/bin/env python3
"""
Verifies that two clusters cannot both start with read-only mode disabled (both as active clusters)
on the same shared data store. One cluster must fail to start, with the HMaster process not
running, and an error logged to the master log.

Usage: python3 ./python/scripts/test_dual_active_cluster_startup.py
"""
import argparse
import os
import time

from python.src.environment_loader import get_env
from python.src.hbase_docker_client import HBaseDockerClient
from python.src.logger_config import get_logger
from python.src.utils import load_env_and_set_up_clients, log_script_start, log_script_end

logger = get_logger(__name__)

STARTUP_WAIT_SECONDS = 60
EXPECTED_ERROR_MSG = "Another cluster is running in active (read-write) mode on this storage location"
CLUSTER1_SERVICE_NAME = "hbase"
CLUSTER2_SERVICE_NAME = "hbase2"


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


def wait_for_active_cluster_file(data_store_root: str, timeout_seconds: int = 30) -> None:
    file_path = f"{data_store_root}/data-store/hbase/active.cluster.suffix.id"
    logger.info(f"Waiting for active cluster suffix id file: {file_path}")
    start = time.time()
    while not os.path.exists(file_path):
        if time.time() - start > timeout_seconds:
            raise RuntimeError(f"Timed out after {timeout_seconds}s waiting for: {file_path}")
        time.sleep(1)
    logger.info(f"Active cluster suffix id file detected: {file_path}")


def main():
    start_time = log_script_start(__file__, logger)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--clean-up-containers', action='store_true',
                        help='Stop Docker containers and revert cluster configurations to one '
                             'active cluster and one replica cluster after the test finishes')
    args = parser.parse_args()

    cluster1, cluster2 = load_env_and_set_up_clients()
    data_store_root = get_env("HBASE_DATA_STORE_ROOT")
    docker_compose_file = get_env("DOCKER_COMPOSE_FILE")

    test_iterations = 4
    for i in range(1, test_iterations+1):
        logger.info(f"---------- Iteration {i} ----------")

        HBaseDockerClient.stop_containers(docker_compose_file=docker_compose_file, data_dir=f'{data_store_root}/*')

        # Make both clusters an active cluster (read-only disabled)
        cluster1.disable_read_only_mode(run_update_all_config=False)
        cluster2.disable_read_only_mode(run_update_all_config=False)

        HBaseDockerClient.set_up_data_store_dir(data_store_root)

        # Alternate which cluster starts first
        if i % 2 == 1:
            first_cluster, second_cluster = cluster1, cluster2
            first_service, second_service = CLUSTER1_SERVICE_NAME, CLUSTER2_SERVICE_NAME
        else:
            first_cluster, second_cluster = cluster2, cluster1
            first_service, second_service = CLUSTER2_SERVICE_NAME, CLUSTER1_SERVICE_NAME

        # Start the first cluster and wait for it to claim the active role
        logger.info(f"Starting {first_cluster.name} first (service: {first_service})")
        HBaseDockerClient.start_service(first_service, docker_compose_file=docker_compose_file)
        wait_for_active_cluster_file(data_store_root)

        # Start the second cluster — it should detect the existing active cluster and fail
        logger.info(f"Starting {second_cluster.name} second (service: {second_service})")
        HBaseDockerClient.start_service(second_service, docker_compose_file=docker_compose_file)

        logger.info(f"Waiting {STARTUP_WAIT_SECONDS}s for {second_cluster.name} to attempt startup...")
        time.sleep(STARTUP_WAIT_SECONDS)

        logger.info("Checking HBase processes on both clusters")
        first_running = check_cluster_processes(first_cluster)
        second_running = check_cluster_processes(second_cluster)

        assert first_running, (
            f"Expected {first_cluster.name} (started first) to be running, but HMaster is down"
        )
        assert not second_running, (
            f"Expected {second_cluster.name} (started second) to have failed, "
            f"but HMaster is still running"
        )

        logger.info(f"[PASS] {first_cluster.name} is running as the active cluster")
        logger.info(f"[PASS] {second_cluster.name} failed to start (HMaster is down)")

        assert_error_in_master_log(second_cluster)

        logger.info(f"Finished iteration {i} of {test_iterations}")

    logger.info("=" * 70)
    logger.info("TEST PASSED: All dual active cluster startups were correctly rejected")
    logger.info("=" * 70)

    if args.clean_up_containers:
        logger.info("Stopping Docker containers and reverting test environment to having "
                    "one active cluster and one replica cluster")
        HBaseDockerClient.stop_containers(docker_compose_file=docker_compose_file, data_dir=f'{data_store_root}/*')
        cluster1.disable_read_only_mode(run_update_all_config=False)
        cluster2.enable_read_only_mode(run_update_all_config=False)

    log_script_end(__file__, logger, start_time)


if __name__ == '__main__':
    main()
