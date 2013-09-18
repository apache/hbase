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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectionImplementation;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Connection to an HTable from within a Coprocessor. Can do some nice tricks since we know we are
 * on a regionserver.
 * <p>
 * This shouldn't be used by usual HBase clients - its merely in this package to maintain visibility
 * considerations for the {@link HConnectionImplementation}.
 */
@SuppressWarnings("javadoc")
public class CoprocessorHConnection extends HConnectionManager.HConnectionImplementation {

  /**
   * Create an unmanaged {@link HConnection} based on the environment in which we are running the
   * coprocessor. The {@link HConnection} must be externally cleaned up (we bypass the usual HTable
   * cleanup mechanisms since we own everything).
   * @param env environment hosting the {@link HConnection}
   * @return an unmanaged {@link HConnection}.
   * @throws IOException if we cannot create the basic connection
   */
  @SuppressWarnings("resource")
  public static HConnection getConnectionForEnvironment(CoprocessorEnvironment env)
      throws IOException {
    Configuration conf = env.getConfiguration();
    HConnection connection = null;
    // this bit is a little hacky - we need to reach kind far into the internals. However, since we
    // are in a coprocessor (which is part of the internals), this is more ok.
    if (env instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment e = (RegionCoprocessorEnvironment) env;
      RegionServerServices services = e.getRegionServerServices();
      if (services instanceof HRegionServer) {
        connection = new CoprocessorHConnection(conf, (HRegionServer) services);
      }
    }
    // didn't create the custom HConnection, so just create the usual connection. Saves us some conf
    // lookups, but no network accesses or anything else with excessive overhead.
    if (connection == null) {
      connection = HConnectionManager.createConnection(conf);
    }
    return connection;
  }

  private ServerName serverName;
  private HRegionServer server;

  CoprocessorHConnection(Configuration conf, HRegionServer server) throws IOException {
    super(conf, false, null);
    this.server = server;
    this.serverName = server.getServerName();
  }

  public HRegionInterface getHRegionConnection(String hostname, int port) throws IOException {
    // if we aren't talking to the local HRegionServer, then do the usual thing
    if (!this.serverName.getHostname().equals(hostname) || !(this.serverName.getPort() == port)) {
      return super.getHRegionConnection(hostname, port);
    }

    // in the usual HConnectionImplementation we would check a cache for the server. However,
    // because we can just return the actual server, we don't need to do anything special.
    return this.server;
  }

  @Deprecated
  public HRegionInterface getHRegionConnection(HServerAddress regionServer) throws IOException {
    throw new UnsupportedOperationException(
        "Coprocessors only find tables via #getHRegionConnection(String, int)");
  }

  @Deprecated
  public HRegionInterface getHRegionConnection(HServerAddress regionServer, boolean getMaster)
      throws IOException {
    throw new UnsupportedOperationException(
        "Coprocessors only find tables via #getHRegionConnection(String, int)");
  }

  @Deprecated
  public HRegionInterface getHRegionConnection(String hostname, int port, boolean getMaster)
      throws IOException {
    throw new UnsupportedOperationException(
        "Coprocessors only find tables via #getHRegionConnection(String, int)");
  }
}