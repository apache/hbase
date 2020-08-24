package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configured;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

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
    LOG.info("Zookeeper Mode enabled sending command to zookeeper + " + cmd + "hostname:" + hostname );
    ChaosZKClient chaosZKClient = new ChaosZKClient(getZKQuorumServersStringFromHbaseConfig());
    return chaosZKClient.submitTask(new ChaosZKClient.TaskObject(cmd, hostname));
  }

  protected HBaseClusterManager.CommandProvider getCommandProvider(ServiceType service) throws IOException {
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
    createZNode(hostname, CmdType.exec.toString() + getCommandProvider(service).signalCommand(service, signal));
  }

  private void createOpCommand(String hostname, ServiceType service, HBaseClusterManager.CommandProvider.Operation op) throws IOException{
    createZNode(hostname, CmdType.exec.toString() + getCommandProvider(service).getCommand(service, op));
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
    return Boolean.parseBoolean(createZNode(hostname, CmdType.bool.toString() + getCommandProvider(service).isRunningCommand(service) ));
  }

  enum CmdType {
    exec,
    bool
  }
}
