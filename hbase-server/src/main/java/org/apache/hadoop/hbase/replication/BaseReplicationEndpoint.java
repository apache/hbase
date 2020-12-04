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

package org.apache.hadoop.hbase.replication;

import static org.apache.hadoop.hbase.client.replication.ReplicationAdmin.REPLICATION_WALENTRYFILTER_CONFIG_KEY;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.AbstractService;

/**
 * A Base implementation for {@link ReplicationEndpoint}s. Users should consider extending this
 * class rather than implementing {@link ReplicationEndpoint} directly for better backwards
 * compatibility.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public abstract class BaseReplicationEndpoint extends AbstractService
  implements ReplicationEndpoint {

  private static final Log LOG = LogFactory.getLog(BaseReplicationEndpoint.class);
  protected Context ctx;

  @Override
  public void init(Context context) throws IOException {
    this.ctx = context;

    if (this.ctx != null){
      ReplicationPeer peer = this.ctx.getReplicationPeer();
      if (peer != null){
        peer.trackPeerConfigChanges(this);
      } else {
        LOG.warn("Not tracking replication peer config changes for Peer Id " + this.ctx.getPeerId() +
            " because there's no such peer");
      }
    }
  }

  @Override
  /**
   * No-op implementation for subclasses to override if they wish to execute logic if their config changes
   */
  public void peerConfigUpdated(ReplicationPeerConfig rpc){

  }

  /** Returns a default set of filters */
  @Override
  public WALEntryFilter getWALEntryfilter() {
    ArrayList<WALEntryFilter> filters = Lists.newArrayList();
    WALEntryFilter scopeFilter = getScopeWALEntryFilter();
    if (scopeFilter != null) {
      filters.add(scopeFilter);
    }
    WALEntryFilter tableCfFilter = getTableCfWALEntryFilter();
    if (tableCfFilter != null) {
      filters.add(tableCfFilter);
    }
    if (ctx != null && ctx.getPeerConfig() != null) {
      String filterNameCSV = ctx.getPeerConfig().getConfiguration().get(REPLICATION_WALENTRYFILTER_CONFIG_KEY);
      if (filterNameCSV != null && !filterNameCSV.isEmpty()) {
          String[] filterNames = filterNameCSV.split(",");
          for (String filterName : filterNames) {
              try {
                  Class<?> clazz = Class.forName(filterName);
                  filters.add((WALEntryFilter) clazz.newInstance());
                } catch (Exception e) {
                  LOG.error("Unable to create WALEntryFilter " + filterName, e);
                }
            }
        }
    }
    return filters.isEmpty() ? null : new ChainWALEntryFilter(filters);
  }

  /** Returns a WALEntryFilter for checking the scope. Subclasses can
   * return null if they don't want this filter */
  protected WALEntryFilter getScopeWALEntryFilter() {
    return new ScopeWALEntryFilter();
  }

  /** Returns a WALEntryFilter for checking replication per table and CF. Subclasses can
   * return null if they don't want this filter */
  protected WALEntryFilter getTableCfWALEntryFilter() {
    return new TableCfWALEntryFilter(ctx.getReplicationPeer());
  }

  @Override
  public boolean canReplicateToSameCluster() {
    return false;
  }

  public void close(){
    if(this.ctx != null) {
      ReplicationPeer peer = this.ctx.getReplicationPeer();
      peer.removeListenerOfPeerConfig(this);
    }
  }
}
