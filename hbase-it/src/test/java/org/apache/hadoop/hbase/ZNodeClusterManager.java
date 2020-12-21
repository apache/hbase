/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configured;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class ZNodeClusterManager extends Configured implements ClusterManager {
  private static final Logger LOG = LoggerFactory.getLogger(ZNodeClusterManager.class.getName());
  private static final String SIGKILL = "SIGKILL";
  private static final String SIGSTOP = "SIGSTOP";
  private static final String SIGCONT = "SIGCONT";
  public ZNodeClusterManager() {
  }

  private String getZKQuorumServersStringFromHbaseConfig() {
    String port =
      Integer.toString(getConf().getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181));
    String[] serverHosts = getConf().getStrings(HConstants.ZOOKEEPER_QUORUM, "localhost");
    for (int i = 0; i < serverHosts.length; i++) {
      serverHosts[i] = serverHosts[i] + ":" + port;
    }
    return Arrays.asList(serverHosts).stream().collect(Collectors.joining(","));
  }

  private String createZNode(String hostname, String cmd) throws IOException{
    LOG.info("Zookeeper Mode enabled sending command to zookeeper + " +
      cmd + "hostname:" + hostname);
    ChaosZKClient chaosZKClient = new ChaosZKClient(getZKQuorumServersStringFromHbaseConfig());
    return chaosZKClient.submitTask(new ChaosZKClient.TaskObject(cmd, hostname));
  }

  protected HBaseClusterManager.CommandProvider getCommandProvider(ServiceType service)
    throws IOException {
    switch (service) {
      case HADOOP_DATANODE:
      case HADOOP_NAMENODE:
        return new HBaseClusterManager.HadoopShellCommandProvider(getConf());
      case ZOOKEEPER_SERVER:
        return new HBaseClusterManager.ZookeeperShellCommandProvider(getConf());
      default:
        return new HBaseClusterManager.HBaseShellCommandProvider(getConf());
    }
  }

  public void signal(ServiceType service, String signal, String hostname) throws IOException {
    createZNode(hostname, CmdType.exec.toString() +
      getCommandProvider(service).signalCommand(service, signal));
  }

  private void createOpCommand(String hostname, ServiceType service,
    HBaseClusterManager.CommandProvider.Operation op) throws IOException{
    createZNode(hostname, CmdType.exec.toString() +
      getCommandProvider(service).getCommand(service, op));
  }

  @Override
  public void start(ServiceType service, String hostname, int port) throws IOException {
    createOpCommand(hostname, service, HBaseClusterManager.CommandProvider.Operation.START);
  }

  @Override
  public void stop(ServiceType service, String hostname, int port) throws IOException {
    createOpCommand(hostname, service, HBaseClusterManager.CommandProvider.Operation.STOP);
  }

  @Override
  public void restart(ServiceType service, String hostname, int port) throws IOException {
    createOpCommand(hostname, service, HBaseClusterManager.CommandProvider.Operation.RESTART);
  }

  @Override
  public void kill(ServiceType service, String hostname, int port) throws IOException {
    signal(service, SIGKILL, hostname);
  }

  @Override
  public void suspend(ServiceType service, String hostname, int port) throws IOException {
    signal(service, SIGSTOP, hostname);
  }

  @Override
  public void resume(ServiceType service, String hostname, int port) throws IOException {
    signal(service, SIGCONT, hostname);
  }

  @Override
  public boolean isRunning(ServiceType service, String hostname, int port) throws IOException {
    return Boolean.parseBoolean(createZNode(hostname, CmdType.bool.toString() +
      getCommandProvider(service).isRunningCommand(service)));
  }

  enum CmdType {
    exec,
    bool
  }
}
