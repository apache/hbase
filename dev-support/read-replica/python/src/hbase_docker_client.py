#!/usr/bin/env python3
import ast
import logging
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

import docker
import requests
import subprocess
import time
import xml.etree.ElementTree as ET

from .logger_config import get_logger

logger = get_logger(__name__)


class DockerExecCommandError(Exception):
    pass


class HBaseShellCommandError(DockerExecCommandError):
    pass


class DockerExecCommandTimeoutError(DockerExecCommandError):
    pass


class HBaseDockerClient:
    def __init__(self, container_name: str, local_conf: str, hbase_ui_port: int = 16010,
                 cluster_name: str = "HBase Cluster", max_retries: int = 12, sleep_time: int = 5,
                 hbase_host: str = "localhost") -> None:
        self._container_name = container_name
        self._local_conf = local_conf
        self._hbase_ui_port = hbase_ui_port
        self._cluster_name = cluster_name
        self._max_retries = max_retries
        self._sleep_time = sleep_time
        self._hbase_host = hbase_host
        self._docker_client = docker.from_env()

    @property
    def name(self) -> str:
        return self._cluster_name

    def run_docker_exec_command(self, bash_cmd: str, timeout: int | None = None) -> str:
        """
        Uses the Docker SDK to exec a Bash command in the object's Docker container.
        Equivalent to: docker exec <container> bash -c <bash_cmd>
        """
        cmd = ["bash", "-c", bash_cmd]
        cmd_str = f"docker exec {self._container_name} bash -c {bash_cmd}"
        logger.debug(f"Running command on {self._cluster_name}: {cmd_str}")

        try:
            container = self._docker_client.containers.get(self._container_name)

            if timeout is not None:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(container.exec_run, cmd, demux=True)
                    try:
                        result = future.result(timeout=timeout)
                    except FuturesTimeoutError:
                        raise DockerExecCommandTimeoutError(
                            f"Command timed out after {timeout}s on {self._cluster_name} "
                            f"({self._container_name}): {bash_cmd}\n"
                            f"The command used to run this was: {cmd_str}\n"
                        )
            else:
                result = container.exec_run(cmd, demux=True)
        except DockerExecCommandError:
            raise
        except docker.errors.DockerException as e:
            raise DockerExecCommandError(
                f"The following command failed on {self._cluster_name} ({self._container_name}): {bash_cmd}\n"
                f"The command used to run this was: {cmd_str}\n"
                f"Docker error: {e}\n"
            )

        exit_code, (stdout, stderr) = result
        stdout_str = (stdout or b'').decode('utf-8')
        if exit_code != 0:
            raise DockerExecCommandError(
                f"The following command failed on {self._cluster_name} ({self._container_name}): {bash_cmd}\n"
                f"The command used to run this was: {cmd_str}\n"
                f"The command's STDERR was:\n{(stderr or b'').decode('utf-8')}\n"
                f"The command's STDOUT was:\n{stdout_str}\n"
            )
        return stdout_str

    def run_hbase_shell_command(self, hbase_cmd: str, timeout: int | None = None) -> str:
        """
        Uses 'docker exec' to run the provided HBase shell command in the object's Docker container.
        The command looks like: docker exec <container> bash -c hbase shell -n <<< "<hbase_cmd>"
        """
        hbase_shell_cmd = f'''hbase shell -n <<< "{hbase_cmd}"'''
        try:
            return self.run_docker_exec_command(hbase_shell_cmd, timeout=timeout)
        except DockerExecCommandTimeoutError:
            # DockerExecCommandTimeoutError is a subclass of DockerExecCommandError, so we need to make sure
            # it's specifically caught and re-raised. Otherwise, it's swallowed when catching DockerExecCommandError
            raise
        except DockerExecCommandError as e:
            raise HBaseShellCommandError(e)

    def _get_pid_from_jps(self, process_name: str) -> int | None:
        """Runs jps inside the container and returns the PID of the named process, or None."""
        try:
            output = self.run_docker_exec_command("jps")
            for line in output.strip().splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[1] == process_name:
                    return int(parts[0])
        except DockerExecCommandError:
            pass
        return None

    def wait_for_hbase_ui(self) -> bool:
        """Checks for a 200 OK on the HBase Master UI."""
        # Read HBASE_HOST from environment, falling back to 'localhost' for host-native execution
        url = f"http://{self._hbase_host}:{self._hbase_ui_port}"
        logger.info(f"Waiting for HBase UI: {self._cluster_name} on {url}")
        last_exception = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    logger.info(f"SUCCESS: {self._cluster_name} UI is up.")
                    return True
            except requests.exceptions.ConnectionError as e:
                last_exception = e
            logging.info(f"Waiting {self._sleep_time} seconds before requesting HBase UI again")
            time.sleep(self._sleep_time)

        raise RuntimeError(f"\nTIMEOUT: {self._cluster_name} UI failed to respond after "
                           f"{self._max_retries} attempts. "
                           f"Last raised exception was: {last_exception}")

    def wait_for_master_initialization(self) -> bool:
        """Waits for the current HMaster process to log 'Master has completed initialization'."""
        logger.info(f"Waiting for Master initialization: {self._cluster_name} ({self._container_name})")
        for attempt in range(1, self._max_retries + 1):
            pid = self._get_pid_from_jps("HMaster")
            if pid is not None:
                awk_cmd = (
                    f"awk '/env:JVM_PID={pid}/{{seen=1; found=0}} "
                    f"seen && /Master has completed initialization/{{found=1}} "
                    f"END{{exit !found}}' /opt/hbase/logs/hbase-*-master-*.log"
                )
                try:
                    self.run_docker_exec_command(awk_cmd)
                    logger.info(f"SUCCESS: {self._cluster_name} Master has completed initialization.")
                    return True
                except DockerExecCommandError:
                    pass
            logging.info(f"Waiting {self._sleep_time} seconds before checking Master initialization again")
            time.sleep(self._sleep_time)

        raise RuntimeError(
            f"\nTIMEOUT: {self._cluster_name} Master failed to initialize after "
            f"{self._max_retries} attempts.")

    def wait_for_region_server_initialization(self) -> bool:
        """Waits for the current HRegionServer process to log 'Serving as' message."""
        logger.info(f"Waiting for RegionServer initialization: {self._cluster_name} ({self._container_name})")
        for attempt in range(1, self._max_retries + 1):
            pid = self._get_pid_from_jps("HRegionServer")
            if pid is not None:
                awk_cmd = (
                    f"awk '/env:JVM_PID={pid}/{{seen=1; found=0}} "
                    f"seen && /Serving as {self._container_name},/{{found=1}} "
                    f"END{{exit !found}}' /opt/hbase/logs/hbase-*-regionserver-*.log"
                )
                try:
                    self.run_docker_exec_command(awk_cmd)
                    logger.info(f"SUCCESS: {self._cluster_name} RegionServer is serving.")
                    return True
                except DockerExecCommandError:
                    pass
            logging.info(f"Waiting {self._sleep_time} seconds before checking RegionServer initialization again")
            time.sleep(self._sleep_time)

        raise RuntimeError(
            f"\nTIMEOUT: {self._cluster_name} RegionServer failed to initialize after "
            f"{self._max_retries} attempts.")

    def check_server_status(self, desired_status: dict | None = None) -> bool:
        """Runs 'status' inside the HBase shell and validates the output."""
        if desired_status is None:
            desired_status = {'masters': '1', 'region_servers': '1', 'dead_servers': '0'}
        logger.info(f"Validating Cluster Status: {self._cluster_name} ({self._container_name})")
        for attempt in range(1, self._max_retries + 1):
            try:
                output = self.get_hbase_status()

                # The cluster's status should have 1 active master, 1 region server,
                # and no dead servers
                validations = {
                    "Active Master": f"{desired_status['masters']} active master" in output,
                    "Region Server": f"{desired_status['region_servers']} servers" in output,
                    "No Dead Servers": f"{desired_status['dead_servers']} dead" in output
                }

                if all(validations.values()):
                    for check, status in validations.items():
                        logger.info(f"    [PASS] {check}")
                    logger.info(f"SUCCESS: {self._cluster_name} is fully operational.")
                    return True
                else:
                    logger.warning(f"{self._cluster_name} is responding, but not all "
                                   f"components are ready...")
                    logger.info(f"HBase 'status' command output:\n{output}")

            except HBaseShellCommandError:
                pass

            logging.info(f"Waiting {self._sleep_time} seconds before getting status on {self.name} again")
            time.sleep(self._sleep_time)

        raise RuntimeError(
            f"\nTIMEOUT: {self._cluster_name} shell check failed after {self._max_retries} attempts.")

    def get_hbase_status(self) -> str:
        logger.debug(f"Getting status of {self.name}")
        return self.run_hbase_shell_command("status")

    def wait_for_cluster_to_start(self) -> None:
        """curls the cluster's HBase UI to make sure it is up and then makes sure all desired servers are up"""
        self.wait_for_hbase_ui()
        self.wait_for_master_initialization()
        self.wait_for_region_server_initialization()
        self.check_server_status()

    def create_table(self, table_name: str, column_family: str) -> bool:
        logger.info(f"Creating table '{table_name}' on {self._cluster_name}")
        create_cmd = f"create '{table_name}', '{column_family}'"
        output = self.run_hbase_shell_command(create_cmd)

        if f"Created table {table_name}" not in output:
            logger.error(f"Could not create table '{table_name}' on {self._cluster_name}")
            return False
        return True

    def disable_table(self, table_name: str) -> None:
        logger.debug(f"Disabling table '{table_name}' on {self.name}")
        self.run_hbase_shell_command(f"disable '{table_name}'")

    def drop_table(self, table_name: str) -> None:
        logger.info(f"Dropping table '{table_name}' on {self.name}")
        self.run_hbase_shell_command(f"drop '{table_name}'")

    def list_tables(self) -> list:
        """Gets the list of HBase tables and returns it as a Python list"""
        logger.debug(f"Getting the list of tables in HBase on {self.name}")
        pattern = r'\[(.*?)\]'
        output = self.run_hbase_shell_command("list")
        output = output.replace('\n', ' ')
        match = re.search(pattern, output)
        return ast.literal_eval(match.group(0))

    def list_regions(self, table_name: str) -> str:
        """Gets list of regions and their info for the provided table"""
        logger.info(f"Getting list of regions for table '{table_name}'")
        return self.run_hbase_shell_command(f"list_regions '{table_name}'")

    def put(self, table_name: str, row: str, column: str, data: str, spec_map: str | None = None) -> None:
        """
        Performs an HBase put command.
        :param table_name: the table we are inserting data into
        :param row: row of the table we are inserting data into
        :param column: column of the table we are inserting data into
        :param data: the actual data we are inserting (as a string)
        :param spec_map: additional attributes input as a string
                         (e.g. "{ATTRIBUTES=>{'my-key'=>'my-value'}}")
        """
        logger.info(f"Adding data to table '{table_name}' on {self.name}")
        put_cmd = f"put '{table_name}', '{row}', '{column}', '{data}'"
        if spec_map:
            put_cmd += f", {spec_map}"
        self.run_hbase_shell_command(put_cmd)

    def get(self, table_name: str, row: str, column: str | None = None, spec_map: str | None = None) -> str:
        logger.info(f"Getting data from table '{table_name}' on {self.name}")
        get_cmd = f"get '{table_name}', '{row}'"
        if column:
            get_cmd += f", '{column}'"
        if spec_map:
            get_cmd += f", {spec_map}"
        output = self.run_hbase_shell_command(get_cmd)
        logger.debug(f"Got data:\n{output}")
        return output

    def delete(self, table_name: str, row: str, column: str, timestamp: int | None = None,
               spec_map: str | None = None) -> None:
        logger.info(f"Deleting data from table '{table_name}' on {self.name}")
        delete_cmd = f"delete '{table_name}', '{row}', '{column}'"
        if timestamp:
            delete_cmd += f", {table_name}"
        if spec_map:
            delete_cmd += f", {spec_map}"
        self.run_hbase_shell_command(delete_cmd)

    def scan(self, table_name: str, spec_map: str | None = None) -> str:
        log_msg = f"Scanning table '{table_name}' on {self.name}"
        scan_cmd = f"scan '{table_name}'"
        if spec_map:
            scan_cmd += f", {spec_map}"
            log_msg += f" with spec_map {spec_map}"
        logging.info(log_msg)
        return self.run_hbase_shell_command(scan_cmd)

    def count(self, table_name: str, spec: str | None = None) -> str:
        logger.info(f"Counting rows for table '{table_name}' on {self.name}")
        count_cmd = f"count '{table_name}'"
        if spec:
            count_cmd += f"{spec}"
        return self.run_hbase_shell_command(count_cmd)

    def flush(self, table_name: str, timeout: int | None = None) -> None:
        logger.debug(f"Flushing table '{table_name}' on {self.name}")
        self.run_hbase_shell_command(f"flush '{table_name}'", timeout=timeout)

    def split(self, thing_to_split: str, split_key: str | None = None) -> None:
        log_msg = f"Splitting '{thing_to_split}'"
        split_cmd = f"split '{thing_to_split}'"

        if split_key:
            log_msg += f" on key '{split_key}'"
            split_cmd += f", '{split_key}'"

        log_msg += f" on {self.name}"

        logger.info(log_msg)
        self.run_hbase_shell_command(split_cmd)

    def flush_and_split(self, thing_to_split: str, split_key: str | None = None) -> None:
        """
        Flushes the table and triggers an asynchronous region split. Split the entire table or pass a region to split an
        individual region. With the second parameter, you can specify an explicit split key for the region.

        thing_to_split - TABLENAME, REGIONNAME, or ENCODED_REGIONNAME
        split_key      - where to have the region split
        """
        self.flush(thing_to_split)
        self.split(thing_to_split, split_key)

    def major_compact(self, table_or_region: str, column_family: str | None = None, mob: str | None = None) -> None:
        log_msg = f"Running major_compact on '{table_or_region}'"
        command = f"major_compact '{table_or_region}'"

        if column_family:
            log_msg += f" for column family '{column_family}'"
            command += f", '{column_family}'"

        if mob and column_family:
            log_msg += " with MOB"
            command += f", 'MOB'"
        elif mob and not column_family:
            log_msg += " with MOB"
            command += ", nil, 'MOB'"

        self.run_hbase_shell_command(command)

    def major_compact_and_wait(self, table_or_region: str, column_family: str | None = None, mob: str | None = None,
                               timeout: int = 30, sleep_time: int = 1) -> bool:
        """Triggers major compaction on a table and blocks until it completes."""
        logger.info(f"Triggering major compaction on '{table_or_region}' on {self.name}...")
        self.major_compact(table_or_region, column_family, mob)

        start_time = time.time()
        while time.time() - start_time < timeout:
            output = self.run_hbase_shell_command(f"compaction_state '{table_or_region}'")

            # When all regions finish compacting, compaction_state returns NONE
            if "NONE" in output:
                logger.info(f"SUCCESS: Major compaction completed for '{table_or_region}'.")
                return True

            logger.debug(f"Compaction still in progress for '{table_or_region}'... waiting {sleep_time}s")
            time.sleep(sleep_time)

        raise RuntimeError(
            f"TIMEOUT: Major compaction on table '{table_or_region}' failed to complete within {timeout} seconds."
        )

    def catalogjanitor_run(self) -> None:
        """Forces the CatalogJanitor to immediately clean up split parent regions in hbase:meta."""
        logger.info(f"Running catalogjanitor_run on {self.name}")
        self.run_hbase_shell_command("catalogjanitor_run")

    def refresh_meta(self) -> None:
        logger.info(f"Refreshing meta on {self.name}")
        self.run_hbase_shell_command("refresh_meta")

    def refresh_hfiles(self) -> None:
        logger.info(f"Refreshing HFiles on {self.name}")
        self.run_hbase_shell_command("refresh_hfiles")

    def refresh_meta_and_hfiles(self) -> None:
        """Consecutively runs refresh_meta and refresh_hfiles in the HBase shell"""
        self.refresh_meta()
        self.refresh_hfiles()

    def enable_read_only_mode(self, run_update_all_config: bool = True) -> None:
        """
        Sets hbase.global.readonly.enabled to 'true' in the local hbase-site.xml file and runs update_all_config
        to dynamically update the configuration. This method assumes the hbase-site.xml file is a mounted volume
        in the docker-compose file, which allows the config file within the docker container to be updated as well.
        """
        self._set_read_only_mode(new_read_only_flag=True, run_update_all_config=run_update_all_config)

    def disable_read_only_mode(self, run_update_all_config: bool = True) -> None:
        """
        Sets hbase.global.readonly.enabled to 'false' in the local hbase-site.xml file and runs update_all_config
        to dynamically update the configuration. This method assumes the hbase-site.xml file is a mounted volume
        in the docker-compose file, which allows the config file within the docker container to be updated as well.
        """
        self._set_read_only_mode(new_read_only_flag=False, run_update_all_config=run_update_all_config)

    def _set_read_only_mode(self, new_read_only_flag: bool, run_update_all_config: bool = True) -> None:
        action = "Enabling" if new_read_only_flag else "Disabling"
        conjunction_adverb = "and then" if run_update_all_config else "but not"
        logger.info(f"{action} read-only mode in conf for {self.name} "
                    f"{conjunction_adverb} running update_all_config after")

        new_read_only_flag = str(new_read_only_flag).lower()
        self.set_hbase_conf_property_value('hbase.global.readonly.enabled', new_read_only_flag)
        actual = self.get_hbase_conf_property_value('hbase.global.readonly.enabled')
        assert actual == new_read_only_flag, (
            f"Expected hbase.global.readonly.enabled={new_read_only_flag} on {self.name}, but got '{actual}'"
        )
        if run_update_all_config:
            self.update_all_config()

    def update_all_config(self) -> None:
        logger.debug(f"Running update_all_config on {self.name} to dynamically update the configuration")
        self.run_hbase_shell_command("update_all_config")

    def get_hbase_conf_property_value(self, conf_prop: str) -> str | None:
        tree = ET.parse(self._local_conf)
        root = tree.getroot()
        for prop in root.findall('property'):
            name_elem = prop.find('name')
            if name_elem is not None and name_elem.text == conf_prop:
                return prop.find('value').text

    def set_hbase_conf_property_value(self, conf_prop: str, value: str) -> None:
        """Sets hbase.global.readonly.enabled to a new value in a local hbase-site.xml file"""
        tree = ET.parse(self._local_conf)
        root = tree.getroot()
        for prop in root.findall('property'):
            name_elem = prop.find('name')
            if name_elem is not None and name_elem.text == conf_prop:
                value_elem = prop.find('value')
                if value_elem is not None:
                    value_elem.text = str(value)
                    break
        tree.write(self._local_conf, encoding='utf-8', xml_declaration=True)
        # The conf file is a Docker volume - wait for the updated version to sync
        time.sleep(1)

    def assert_read_only_error_occurs(self, cmd_type: str, table_name: str, column: str,
                                      row: str | None = None, data: str | None = None) -> None:
        """
        Runs a command on read-only cluster and expects an error to occur as a result.
        """
        logger.info(f"Verifying we cannot perform a '{cmd_type}' on {self.name} "
                    f"since it is in read-only mode")
        try:
            # This should throw an exception
            match cmd_type.lower():
                case 'create':
                    self.create_table(table_name, column)
                case 'drop':
                    self.drop_table(table_name)
                case 'put':
                    self.put(table_name, row, column, data)
                case 'delete':
                    self.delete(table_name, row, column)
                case _:
                    raise RuntimeError(f"Unexpected command type: {cmd_type}")

            # If we get here, then the command succeeded on the read-replica cluster, which should
            # not have happened.
            raise RuntimeError(f"Expected {cmd_type} attempt on {self.name} "
                               f"to result in an error")
        except HBaseShellCommandError as e:
            # Verify the command we ran on the read-replica cluster produced the expected exception
            expected_error = ("org.apache.hadoop.hbase.WriteAttemptedOnReadOnlyClusterException: "
                              "Operation not allowed in Read-Only Mode")
            assert expected_error in str(e), (f"Expected exception to contain the following: "
                                              f"{expected_error}\n"
                                              f"The actual exception was:\n{e}")
        logger.info(f"{cmd_type.capitalize()} attempt on {self.name} failed as expected")

    def assert_table_does_not_exist(self, table_name: str) -> None:
        logger.info(f"Verifying '{table_name}' is not in the list of tables on {self.name}")
        assert table_name not in self.list_tables(), \
            f"Expected table '{table_name}' to not exist on {self.name}"

    def assert_table_exists(self, table_name: str) -> None:
        logger.info(f"Verifying '{table_name}' is in the list of tables on {self.name}")
        assert table_name in self.list_tables(), \
            f"Expected table '{table_name}' to exist on {self.name}"

    def assert_table_row_count(self, table_name: str, expected_row_count: int) -> None:
        logger.info(f"Verifying table '{table_name}' on {self.name} has {expected_row_count} row(s)")
        output = self.count(table_name)
        match = re.search(r'^(\d+) row\(s\)$', output, re.MULTILINE)
        actual_row_count = int(match.group(1)) if match else None
        assert actual_row_count == expected_row_count, \
            (f"Expected table '{table_name}' on {self.name} to have {expected_row_count} row(s). "
             f"Instead got {actual_row_count}")

    def assert_get_output(self, table: str, row: str, cf: str, expected_data: str) -> str:
        output = self.get(table, row, cf)
        assert f"value={expected_data}" in output, \
            f"Expected get command to retrieve a row with value={expected_data}. Output instead was:\n{output}"
        return output

    def assert_region_count_for_table(self, table_name: str, expected_region_count: int) -> None:
        logger.info(f"Verifying table '{table_name}' has {expected_region_count} region(s)")
        output = self.list_regions(table_name)
        match = re.search(r'^ (\d+) rows$', output, re.MULTILINE)
        actual_region_count = int(match.group(1)) if match else None
        assert actual_region_count == expected_region_count, \
            (f"Expected table '{table_name}' on {self.name} to have {expected_region_count} region(s). "
             f"Instead got {actual_region_count}")

    @staticmethod
    def __run_subprocess_command(command: list | str, error_msg: str,
                                 shell: bool = False) -> subprocess.CompletedProcess:
        if shell:
            cmd_msg = command
        else:
            cmd_msg = f"{' '.join(command)}"
        logger.info(f"Running: {cmd_msg}")
        result = subprocess.run(command, capture_output=True, text=True, shell=shell)
        if result.returncode != 0:
            raise RuntimeError(
                f"Command failed: {cmd_msg}\n"
                f"{error_msg} (exit {result.returncode}):\n"
                f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
            )
        return result

    @staticmethod
    def wait_for_clusters_to_start(clusters: list) -> None:
        for cluster in clusters:
            cluster.wait_for_cluster_to_start()
        logger.info("=" * 40)
        logger.info("ALL CLUSTERS VERIFIED AND READY")
        logger.info("=" * 40)

    @staticmethod
    def are_containers_running(docker_compose_file: str | None = None) -> bool:
        logger.info("Checking if docker containers are running")
        command = ["docker", "compose"]
        if docker_compose_file:
            command += ["-f", docker_compose_file]
        command += ["ps", "--status", "running", "-q"]
        result = HBaseDockerClient.__run_subprocess_command(command, "Failed to get docker container status")
        return bool(result.stdout.strip())

    @staticmethod
    def set_up_data_store_dir(data_store_root: str) -> None:
        command = ["mkdir", "-p", f"{data_store_root}/data-store/hbase", f"{data_store_root}/data-store/run",
                   f"{data_store_root}/data-store/logs", f"{data_store_root}/data-store/zk"]
        HBaseDockerClient.__run_subprocess_command(command,
                                                   error_msg=f"Failed to create {data_store_root} and its sub-dirs")
        command = ["chmod", "-R", "777", f"{data_store_root}"]
        HBaseDockerClient.__run_subprocess_command(command,
                                                   error_msg=f"Failed to give {data_store_root} "
                                                             f"and its sub-dirs full permissions")

    @staticmethod
    def start_or_restart_containers(docker_compose_file: str | None = None, data_store_root: str | None = None) -> None:
        if data_store_root:
            HBaseDockerClient.set_up_data_store_dir(data_store_root)

        if HBaseDockerClient.are_containers_running(docker_compose_file):
            logger.info("Restarting docker containers")
            command = ["docker", "compose"]
            if docker_compose_file:
                command += ["-f", docker_compose_file]
            command += ["restart"]
            action = "restart"
        else:
            logger.info("Starting docker containers")
            command = ["docker", "compose"]
            if docker_compose_file:
                command += ["-f", docker_compose_file]
            command += ["up", "-d"]
            action = "start"

        HBaseDockerClient.__run_subprocess_command(command, f"docker compose {action} failed")
        logger.info(f"docker compose {action} completed successfully")

    @staticmethod
    def start_service(service_name: str, docker_compose_file: str | None = None) -> None:
        logger.info(f"Starting docker compose service: {service_name}")
        command = ["docker", "compose"]
        if docker_compose_file:
            command += ["-f", docker_compose_file]
        command += ["up", "-d", service_name]
        HBaseDockerClient.__run_subprocess_command(command, f"Failed to start service '{service_name}'")

    @staticmethod
    def stop_containers(docker_compose_file: str | None = None, data_dir: str | None = None,
                        sudo: bool = False) -> None:
        command = "docker compose"
        if docker_compose_file:
            command += f" -f {docker_compose_file}"
        command += " down"
        log_msg = "Stopping docker containers"
        if data_dir:
            rm_cmd = "sudo rm -rf" if sudo else "rm -rf"
            command += f" && {rm_cmd} {data_dir}"
            log_msg += f" and deleting HBase data root dir at: {data_dir}"
        logger.info(f"{log_msg}")
        HBaseDockerClient.__run_subprocess_command(command, "stop_containers failed", shell=True)
        logger.info("Successfully stopped docker containers")
