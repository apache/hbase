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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.AbstractService;

/**
 * A Base implementation for {@link ReplicationEndpoint}s. For internal use. Uses our internal
 * Guava.
 */
// This class has been made InterfaceAudience.Private in 2.0.0. It used to be
// LimitedPrivate. See HBASE-15982.
@InterfaceAudience.Private
public abstract class BaseReplicationEndpoint extends AbstractService
  implements ReplicationEndpoint {

  private static final Logger LOG = LoggerFactory.getLogger(BaseReplicationEndpoint.class);
  public static final String REPLICATION_WALENTRYFILTER_CONFIG_KEY
      = "hbase.replication.source.custom.walentryfilters";
  protected Context ctx;

  @Override
  public void init(Context context) throws IOException {
    this.ctx = context;

    if (this.ctx != null){
      ReplicationPeer peer = this.ctx.getReplicationPeer();
      if (peer != null){
        peer.registerPeerConfigListener(this);
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
    WALEntryFilter tableCfFilter = getNamespaceTableCfWALEntryFilter();
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
            filters.add((WALEntryFilter) clazz.getDeclaredConstructor().newInstance());
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
  protected WALEntryFilter getNamespaceTableCfWALEntryFilter() {
    return new NamespaceTableCfWALEntryFilter(ctx.getReplicationPeer());
  }

  @Override
  public boolean canReplicateToSameCluster() {
    return false;
  }

  @Override
  public boolean isStarting() {
    return state() == State.STARTING;
  }
}
