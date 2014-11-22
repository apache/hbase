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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;

/**
 * A Base implementation for {@link ReplicationEndpoint}s. Users should consider extending this
 * class rather than implementing {@link ReplicationEndpoint} directly for better backwards
 * compatibility.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public abstract class BaseReplicationEndpoint extends AbstractService
  implements ReplicationEndpoint {

  protected Context ctx;

  @Override
  public void init(Context context) throws IOException {
    this.ctx = context;
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

}
