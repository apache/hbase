#!/usr/bin/env python3
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

"""Check published Jetty exclusions and a MiniDFS tool using HBASE_CLASSPATH."""

import argparse
import os
from pathlib import Path
import signal
import socket
import subprocess
import tarfile
import tempfile
import time
from urllib.error import URLError
from urllib.request import urlopen
import xml.etree.ElementTree as ET
from zipfile import ZipFile


def unpack(archive, directory):
    with tarfile.open(archive) as tar:
        members = tar.getmembers()
        for member in members:
            target = (directory / member.name).resolve()
            if not (directory == target or directory in target.parents):
                raise ValueError("Archive path escapes the test directory: " + member.name)
            if not (member.isfile() or member.isdir()):
                raise ValueError("Unsupported archive member: " + member.name)
        tar.extractall(directory, members=members)
        return [Path(member.name) for member in members if member.isfile()]


def available_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def check_http(command, env, directory, output, url):
    process = subprocess.Popen(command, env=env, cwd=directory, stdout=output,
                               stderr=subprocess.STDOUT, start_new_session=True)
    try:
        deadline = time.monotonic() + 60
        while True:
            assert process.poll() is None, "Tool exited before its HTTP server started: " + command[-1]
            try:
                with urlopen(url, timeout=1) as response:
                    assert response.status == 200
                    return
            except (URLError, TimeoutError, socket.timeout):
                assert time.monotonic() < deadline, "HTTP startup timed out: " + url
                time.sleep(0.2)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)


def check(binary, client, test_classpath, runtime_tree):
    dependencies = runtime_tree.read_text()
    assert "org.apache.hbase:hbase-server:jar:" in dependencies, dependencies
    assert not any(group in dependencies for group in (
        "org.eclipse.jetty:", "org.eclipse.jetty.websocket:", "org.mortbay.jetty:")), dependencies
    print("PASS: independent hbase-server consumer has no unshaded Jetty runtime dependency")
    with tempfile.TemporaryDirectory(prefix="hbase-jetty-") as temporary:
        directory = Path(temporary).resolve()
        files = unpack(binary, directory)
        production_files = files + unpack(client, directory)
        raw = [p for p in production_files if p.suffix == ".jar" and p.name.startswith(
            ("jetty-", "websocket-", "javax-websocket-"))]
        assert not raw, "Production archive contains unshaded Jetty: " + str(raw)
        for path in production_files:
            if path.suffix == ".jar":
                with ZipFile(directory / path) as jar:
                    raw_classes = [name for name in jar.namelist() if name.endswith(".class")
                                   and name.startswith(("org/eclipse/jetty/", "org/mortbay/"))]
                    assert not raw_classes, str(path) + " contains " + str(raw_classes[:3])
        assert any(p.name.startswith("hbase-shaded-jetty-") for p in files)
        roots = {p.parts[0] for p in files}
        assert len(roots) == 1, roots
        home = directory / roots.pop()
        assert "org.apache.hbase:hbase-server:jar:" + home.name[len("hbase-"):] \
            + ":compile" in dependencies, "Consumer uses a different HBase version"
        core_prefix = "hadoop-mapreduce-client-core-"
        hadoop_versions = {p.name[len(core_prefix):-len(".jar")] for p in files
                           if p.name.startswith(core_prefix) and p.suffix == ".jar"}
        assert len(hadoop_versions) == 1, hadoop_versions
        assert "org.apache.hadoop:hadoop-mapreduce-client-core:jar:" + hadoop_versions.pop() \
            + ":compile" in dependencies, "Consumer uses a different Hadoop build profile"
        site = ET.parse(home / "conf/hbase-site.xml")
        prop = ET.SubElement(site.getroot(), "property")
        ET.SubElement(prop, "name").text = "hbase.rest.host"
        ET.SubElement(prop, "value").text = "127.0.0.1"
        site.write(home / "conf/hbase-site.xml", encoding="utf-8")
        env = dict(os.environ, HBASE_HOME=str(home), HBASE_CONF_DIR=str(home / "conf"),
                   HBASE_LOG_DIR=str(directory / "logs"), HBASE_CLASSPATH="",
                   HBASE_CLASSPATH_PREFIX="", HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP="true",
                   HBASE_HEAPSIZE="512")
        command = [str(home / "bin/hbase"), "--internal-classpath"]
        log = directory / "tools.log"
        try:
            with log.open("w") as output:
                port = available_port()
                check_http(command + ["rest", "-p", str(port), "-i", "-1", "start"], env,
                           directory, output, "http://127.0.0.1:" + str(port) + "/version")
                print("PASS: production REST /version responds without unshaded Jetty")
                print("PASS: server and client archives exclude unshaded Jetty")
                extra = [Path(p) for p in test_classpath.read_text().strip().split(os.pathsep)]
                assert extra and all(p.is_file() for p in extra), "Invalid test classpath"
                assert not any(p.name.startswith("hbase-") for p in extra), \
                    "Test classpath must not replace packaged HBase classes"
                test_env = dict(env, HBASE_CLASSPATH=os.pathsep.join(map(str, extra)))
                tool = command + ["org.apache.hadoop.hbase.util.MiniDFSClusterTool"]
                subprocess.run(tool, env=test_env,
                               cwd=directory, stdout=output, stderr=subprocess.STDOUT,
                               check=True, timeout=120)
                for marker in ("MINIDFS_STARTED", "MINIDFS_READ_WRITE_OK", "MINIDFS_CLOSED"):
                    assert marker in log.read_text(), "Missing MiniDFS stage: " + marker
                print("PASS: ordinary bin/hbase tool starts, reads/writes and closes MiniDFS")
                # Remove MiniDFS from both the installation and supplemental classpath.
                removed = []
                try:
                    for jar in home.rglob("hadoop-hdfs-*-tests.jar"):
                        backup = directory / "removed" / jar.relative_to(home)
                        backup.parent.mkdir(parents=True, exist_ok=True)
                        jar.rename(backup)
                        removed.append((jar, backup))
                    missing_env = dict(test_env, HBASE_CLASSPATH=os.pathsep.join(
                        str(p) for p in extra if not p.match("hadoop-hdfs-*-tests.jar")))
                    result = subprocess.run(tool, env=missing_env, cwd=directory,
                                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                            text=True, timeout=60)
                    output.write(result.stdout)
                    output.flush()
                    assert result.returncode != 0, "Tool succeeded without MiniDFSCluster"
                    assert "NoClassDefFoundError: org/apache/hadoop/hdfs/MiniDFSCluster" \
                        in result.stdout, result.stdout
                finally:
                    for jar, backup in removed:
                        backup.rename(jar)
                print("PASS: missing MiniDFSCluster fails the ordinary tool entry point")
        except Exception:
            if log.exists():
                print(log.read_text(errors="replace"))
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("client", type=Path)
    parser.add_argument("test_classpath", type=Path,
                        help="Maven test classpath, excluding HBase artifacts and jdk.tools")
    parser.add_argument("runtime_tree", type=Path,
                        help="Runtime dependency tree of the independent jetty-consumer project")
    args = parser.parse_args()
    check(args.binary, args.client, args.test_classpath, args.runtime_tree)
