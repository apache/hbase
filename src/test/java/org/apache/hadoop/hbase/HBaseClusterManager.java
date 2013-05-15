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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClusterManager.CommandProvider.Operation;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.Shell;

/**
 * A default cluster manager for HBase. Uses SSH, and hbase shell scripts
 * to manage the cluster. Assumes Unix-like commands are available like 'ps',
 * 'kill', etc. Also assumes the user running the test has enough "power" to start & stop
 * servers on the remote machines (for example, the test user could be the same user as the
 * user the daemon isrunning as)
 */
@InterfaceAudience.Private
public class HBaseClusterManager extends ClusterManager {
  private String sshUserName;
  private String sshOptions;

  /**
   * The command format that is used to execute the remote command. Arguments:
   * 1 SSH options, 2 user name , 3 "@" if username is set, 4 host, 5 original command.
   */
  private static final String DEFAULT_TUNNEL_CMD = "/usr/bin/ssh %1$s %2$s%3$s%4$s \"%5$s\"";
  private String tunnelCmd;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) {
      // Configured gets passed null before real conf. Why? I don't know.
      return;
    }
    sshUserName = conf.get("hbase.it.clustermanager.ssh.user", "");
    String extraSshOptions = conf.get("hbase.it.clustermanager.ssh.opts", "");
    sshOptions = System.getenv("HBASE_SSH_OPTS");
    if (!extraSshOptions.isEmpty()) {
      sshOptions = StringUtils.join(new Object[] { sshOptions, extraSshOptions }, " ");
    }
    sshOptions = (sshOptions == null) ? "" : sshOptions;
    tunnelCmd = conf.get("hbase.it.clustermanager.ssh.cmd", DEFAULT_TUNNEL_CMD);
    LOG.info("Running with SSH user [" + sshUserName + "] and options [" + sshOptions + "]");
  }

  /**
   * Executes commands over SSH
   */
  protected class RemoteShell extends Shell.ShellCommandExecutor {
    private String hostname;

    public RemoteShell(String hostname, String[] execString, File dir, Map<String, String> env,
        long timeout) {
      super(execString, dir, env, timeout);
      this.hostname = hostname;
    }

    public RemoteShell(String hostname, String[] execString, File dir, Map<String, String> env) {
      super(execString, dir, env);
      this.hostname = hostname;
    }

    public RemoteShell(String hostname, String[] execString, File dir) {
      super(execString, dir);
      this.hostname = hostname;
    }

    public RemoteShell(String hostname, String[] execString) {
      super(execString);
      this.hostname = hostname;
    }

    public String[] getExecString() {
      String at = sshUserName.isEmpty() ? "" : "@";
      String remoteCmd = StringUtils.join(super.getExecString(), " ");
      String cmd = String.format(tunnelCmd, sshOptions, sshUserName, at, hostname, remoteCmd);
      LOG.info("Executing full command [" + cmd + "]");
      return new String[] { "/usr/bin/env", "bash", "-c", cmd };
    }

    @Override
    public void execute() throws IOException {
      super.execute();
    }
  }

  /**
   * Provides command strings for services to be executed by Shell. CommandProviders are
   * pluggable, and different deployments(windows, bigtop, etc) can be managed by
   * plugging-in custom CommandProvider's or ClusterManager's.
   */
  static abstract class CommandProvider {

    enum Operation {
      START, STOP, RESTART
    }

    public abstract String getCommand(ServiceType service, Operation op);

    public String isRunningCommand(ServiceType service) {
      return findPidCommand(service);
    }

    protected String findPidCommand(ServiceType service) {
      String servicePathFilter = "";
      if (service == ServiceType.HBASE_MASTER || service == ServiceType.HBASE_REGIONSERVER) {
        servicePathFilter = " | grep hbase";
      }
      return String.format("ps ux | grep %s %s | grep -v grep | tr -s ' ' | cut -d ' ' -f2",
          service, servicePathFilter);
    }

    public String signalCommand(ServiceType service, String signal) {
      return String.format("%s | xargs kill -s %s", findPidCommand(service), signal);
    }
  }

  /**
   * CommandProvider to manage the service using bin/hbase-* scripts
   */
  static class HBaseShellCommandProvider extends CommandProvider {
    private String getHBaseHome() {
      return System.getenv("HBASE_HOME");
    }

    private String getConfig() {
      String confDir = System.getenv("HBASE_CONF_DIR");
      if (confDir != null) {
        return String.format("--config %s", confDir);
      }
      return "";
    }

    @Override
    public String getCommand(ServiceType service, Operation op) {
      return String.format("%s/bin/hbase-daemon.sh %s %s %s", getHBaseHome(), getConfig(),
          op.toString().toLowerCase(), service);
    }
  }

  public HBaseClusterManager() {
    super();
  }

  protected CommandProvider getCommandProvider(ServiceType service) {
    //TODO: make it pluggable, or auto-detect the best command provider, should work with
    //hadoop daemons as well
    return new HBaseShellCommandProvider();
  }

  /**
   * Execute the given command on the host using SSH
   * @return pair of exit code and command output
   * @throws IOException if something goes wrong.
   */
  private Pair<Integer, String> exec(String hostname, String... cmd) throws IOException {
    LOG.info("Executing remote command: " + StringUtils.join(cmd, " ") + " , hostname:" + hostname);

    RemoteShell shell = new RemoteShell(hostname, cmd);
    shell.execute();

    LOG.info("Executed remote command, exit code:" + shell.getExitCode()
        + " , output:" + shell.getOutput());

    return new Pair<Integer, String>(shell.getExitCode(), shell.getOutput());
  }

  private void exec(String hostname, ServiceType service, Operation op) throws IOException {
    exec(hostname, getCommandProvider(service).getCommand(service, op));
  }

  @Override
  public void start(ServiceType service, String hostname) throws IOException {
    exec(hostname, service, Operation.START);
  }

  @Override
  public void stop(ServiceType service, String hostname) throws IOException {
    exec(hostname, service, Operation.STOP);
  }

  @Override
  public void restart(ServiceType service, String hostname) throws IOException {
    exec(hostname, service, Operation.RESTART);
  }

  @Override
  public void signal(ServiceType service, String signal, String hostname) throws IOException {
    exec(hostname, getCommandProvider(service).signalCommand(service, signal));
  }

  @Override
  public boolean isRunning(ServiceType service, String hostname) throws IOException {
    String ret = exec(hostname, getCommandProvider(service).isRunningCommand(service))
        .getSecond();
    return ret.length() > 0;
  }

}
