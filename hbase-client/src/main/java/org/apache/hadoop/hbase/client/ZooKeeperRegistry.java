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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;

/**
 * A cluster registry that stores to zookeeper.
 */
class ZooKeeperRegistry implements Registry {
  static final Log LOG = LogFactory.getLog(ZooKeeperRegistry.class);
  // Needs an instance of hci to function.  Set after construct this instance.
  ConnectionManager.HConnectionImplementation hci;

  @Override
  public void init(Connection connection) {
    if (!(connection instanceof ConnectionManager.HConnectionImplementation)) {
      throw new RuntimeException("This registry depends on HConnectionImplementation");
    }
    this.hci = (ConnectionManager.HConnectionImplementation)connection;
  }

  @Override
  public RegionLocations getMetaRegionLocation() throws IOException {
    ZooKeeperKeepAliveConnection zkw = hci.getKeepAliveZooKeeperWatcher();

    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Looking up meta region location in ZK," + " connection=" + this);
      }
      ServerName servername = new MetaTableLocator().blockUntilAvailable(zkw, hci.rpcTimeout);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Looked up meta region location, connection=" + this +
          "; serverName=" + ((servername == null) ? "null" : servername));
      }
      if (servername == null) return null;
      HRegionLocation loc = new HRegionLocation(HRegionInfo.FIRST_META_REGIONINFO, servername, 0);
      return new RegionLocations(new HRegionLocation[] {loc});
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } finally {
      zkw.close();
    }
  }

  private String clusterId = null;

  @Override
  public String getClusterId() {
    if (this.clusterId != null) return this.clusterId;
    // No synchronized here, worse case we will retrieve it twice, that's
    //  not an issue.
    ZooKeeperKeepAliveConnection zkw = null;
    try {
      zkw = hci.getKeepAliveZooKeeperWatcher();
      this.clusterId = ZKClusterId.readClusterIdZNode(zkw);
      if (this.clusterId == null) {
        LOG.info("ClusterId read in ZooKeeper is null");
      }
    } catch (KeeperException e) {
      LOG.warn("Can't retrieve clusterId from Zookeeper", e);
    } catch (IOException e) {
      LOG.warn("Can't retrieve clusterId from Zookeeper", e);
    } finally {
      if (zkw != null) zkw.close();
    }
    return this.clusterId;
  }

  @Override
  public int getCurrentNrHRS() throws IOException {
    ZooKeeperKeepAliveConnection zkw = hci.getKeepAliveZooKeeperWatcher();
    try {
      // We go to zk rather than to master to get count of regions to avoid
      // HTable having a Master dependency.  See HBase-2828
      return ZKUtil.getNumberOfChildren(zkw, zkw.rsZNode);
    } catch (KeeperException ke) {
      throw new IOException("Unexpected ZooKeeper exception", ke);
    } finally {
        zkw.close();
    }
  }
}
