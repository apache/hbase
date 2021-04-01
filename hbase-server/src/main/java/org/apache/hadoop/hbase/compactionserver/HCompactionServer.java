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
package org.apache.hadoop.hbase.compactionserver;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AbstractRpcServices;
import org.apache.hadoop.hbase.AbstractServer;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.security.SecurityConstants;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.ipc.RemoteException;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionServerStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionServerStatusProtos.CompactionServerStatusService;

@InterfaceAudience.Private
public class HCompactionServer extends AbstractServer implements CompactionServerServices {

  /** compaction server process name */
  public static final String COMPACTIONSERVER = "compactionserver";
  private static final Logger LOG = LoggerFactory.getLogger(HCompactionServer.class);

  protected String getProcessName() {
    return COMPACTIONSERVER;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return null;
  }

  @Override
  public ChoreService getChoreService() {
    return null;
  }

  protected final CSRpcServices rpcServices;

  // Stub to do region server status calls against the master.
  private volatile CompactionServerStatusService.BlockingInterface cssStub;

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it. To get a fresh
   * connection, the current cssStub must be null. Method will block until a master is available.
   * You can break from this block by requesting the server stop.
   * @return master + port, or null if server has been stopped
   */
  private synchronized ServerName createCompactionServerStatusStub() {
    if (this.cssStub != null) {
      return masterAddressTracker.getMasterAddress();
    }
    ServerName sn = null;
    long previousLogTime = 0;
    CompactionServerStatusProtos.CompactionServerStatusService.BlockingInterface intCssStub = null;
    boolean interrupted = false;
    try {
      while (!isStopped()) {
        sn = this.masterAddressTracker.getMasterAddress(true);
        if (sn == null) {
          if (isStopped()) {
            // give up with no connection.
            LOG.debug("No master found and cluster is stopped; bailing out");
            return null;
          }
          if (System.currentTimeMillis() > (previousLogTime + 1000)) {
            LOG.debug("No master found; retry");
            previousLogTime = System.currentTimeMillis();
          }
          if (sleepInterrupted(200)) {
            interrupted = true;
          }
          continue;
        }

        try {
          BlockingRpcChannel channel = this.rpcClient.createBlockingRpcChannel(sn,
            userProvider.getCurrent(), shortOperationTimeout);
          intCssStub =
              CompactionServerStatusProtos.CompactionServerStatusService.newBlockingStub(channel);
          break;
        } catch (IOException e) {
          if (System.currentTimeMillis() > (previousLogTime + 1000)) {
            e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
            if (e instanceof ServerNotRunningYetException) {
              LOG.info("Master isn't available yet, retrying");
            } else {
              LOG.warn("Unable to connect to master. Retrying. Error was:", e);
            }
            previousLogTime = System.currentTimeMillis();
          }
          if (sleepInterrupted(200)) {
            interrupted = true;
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    this.cssStub = intCssStub;
    return sn;
  }


  protected CSRpcServices createRpcServices() throws IOException {
    return new CSRpcServices(this);
  }

  public HCompactionServer(final Configuration conf) throws IOException {
    super(conf, "CompactionServer"); // thread name
    this.msgInterval = conf.getInt(HConstants.COMPACTION_SERVER_MSG_INTERVAL, 3 * 1000);
    this.sleeper = new Sleeper(this.msgInterval, this);
    this.startcode = System.currentTimeMillis();
    this.rpcServices = createRpcServices();
    setServerName(ServerName.valueOf(this.rpcServices.getIsa().getHostName(),
      this.rpcServices.getIsa().getPort(), this.startcode));
    zooKeeper = new ZKWatcher(conf, getProcessName() + ":" + rpcServices.getIsa().getPort(), this,
        canCreateBaseZNode());
    if (!this.masterless) {
      masterAddressTracker = new MasterAddressTracker(getZooKeeper(), this);
      masterAddressTracker.start();
    } else {
      masterAddressTracker = null;
    }

    ZKUtil.loginClient(this.conf, HConstants.ZK_CLIENT_KEYTAB_FILE,
      HConstants.ZK_CLIENT_KERBEROS_PRINCIPAL, this.rpcServices.getIsa().getHostName());
    // login the server principal (if using secure Hadoop)
    login(userProvider, this.rpcServices.getIsa().getHostName());
    Superusers.initialize(conf);
    this.rpcServices.start();
  }

  @Override
  protected AbstractRpcServices getRpcService(){
    return rpcServices;
  }

  protected void login(UserProvider user, String host) throws IOException {
    user.login(SecurityConstants.COMPACTION_SERVER_KRB_KEYTAB_FILE,
      SecurityConstants.COMPACTION_SERVER_KRB_PRINCIPAL, host);
  }


  private void tryCompactionServerReport() throws IOException {
    LOG.info("compaction server invoke tryCompactionServerReport");
    if (this.cssStub == null) {
      LOG.error("master servername is " + createCompactionServerStatusStub());
    }
    try {
      CompactionServerStatusProtos.CompactionServerReportRequest.Builder request =
          CompactionServerStatusProtos.CompactionServerReportRequest.newBuilder();
      request.setServer(ProtobufUtil.toServerName(getServerName()));
      this.cssStub.compactionServerReport(null, request.build());
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof YouAreDeadException) {
        // This will be caught and handled as a fatal error in run()
        throw ioe;
      }
      this.cssStub = null;
    }
  }

  @Override
  public boolean isAborted() {
    return false;
  }

  /**
   * The HCompactionServer sticks in this loop until closed.
   */
  @Override
  public void run() {
    if (isStopped()) {
      LOG.info("Skipping run; stopped");
      return;
    }

    try {
      preRegistrationInitialization();
    } catch (Throwable e) {
      abort("Fatal exception during initialization", e);
    }
    try {
      // We registered with the Master. Go into run mode.
      long lastMsg = System.currentTimeMillis();
      // The main run loop.
      while (!isStopped()) {
        long now = System.currentTimeMillis();
        if ((now - lastMsg) >= msgInterval) {
          tryCompactionServerReport();
          lastMsg = System.currentTimeMillis();
        }
        if (!isStopped()) {
          this.sleeper.sleep();
        }
      }
    } catch (Throwable t) {
      LOG.error("catch exception when compactionserver run ", t);
      if (!rpcServices.checkOOME(t)) {
        String prefix = t instanceof YouAreDeadException ? "" : "Unhandled: ";
        abort(prefix + t.getMessage(), t);
      }
    }
  }

  @Override
  public void abort(String reason, Throwable cause) {
    String msg = "***** ABORTING compaction server " + this + ": " + reason + " *****";
    if (cause != null) {
      LOG.error(HBaseMarkers.FATAL, msg, cause);
    } else {
      LOG.error(HBaseMarkers.FATAL, msg);
    }
    stop(msg);
  }


  @Override
  public void stop(final String msg) {
    if (!this.stopped) {
      LOG.info("***** STOPPING compaction server '" + this + "' *****");
    }
    this.stopped = true;
    LOG.info("STOPPED: " + msg);
  }

  /**
   * @see org.apache.hadoop.hbase.compactionserver.HCompactionServerCommandLine
   */
  public static void main(String[] args) {
    LOG.info("STARTING executorService " + HCompactionServer.class.getSimpleName());
    VersionInfo.logVersion();
    Configuration conf = HBaseConfiguration.create();
    @SuppressWarnings("unchecked")
    Class<? extends HCompactionServer> compactionServerClass =
        (Class<? extends HCompactionServer>) conf.getClass(HConstants.COMPACTION_SERVER_IMPL,
          HCompactionServer.class);

    new HCompactionServerCommandLine(compactionServerClass).doMain(args);
  }

  /**
   * Utility for constructing an instance of the passed HCompactionServer class.
   */
  static HCompactionServer constructCompactionServer(
      final Class<? extends HCompactionServer> compactionServerClass, final Configuration conf) {
    try {
      Constructor<? extends HCompactionServer> c =
          compactionServerClass.getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed construction of " + "Compactionserver: " + compactionServerClass.toString(), e);
    }
  }

}
