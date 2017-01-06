/*
 *
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

package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Pair;

/**
 * An Observer to facilitate replication operations
 */

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ReplicationObserver extends BaseRegionObserver {
  private static final Log LOG = LogFactory.getLog(ReplicationObserver.class);

  @Override
  public void preCommitStoreFile(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      final byte[] family, final List<Pair<Path, Path>> pairs) throws IOException {
    RegionCoprocessorEnvironment env = ctx.getEnvironment();
    Configuration c = env.getConfiguration();
    if (pairs == null || pairs.isEmpty()
        || !c.getBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
          HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT)) {
      LOG.debug("Skipping recording bulk load entries in preCommitStoreFile for bulkloaded "
          + "data replication.");
      return;
    }
    HRegionServer rs = (HRegionServer) env.getRegionServerServices();
    Replication rep = (Replication) rs.getReplicationSourceService();
    rep.addHFileRefsToQueue(env.getRegionInfo().getTable(), family, pairs);
  }
}
