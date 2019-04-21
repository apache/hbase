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
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Observer to add HFile References to replication queue.
 */
@CoreCoprocessor
@InterfaceAudience.Private
public class ReplicationObserver implements RegionCoprocessor, RegionObserver {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationObserver.class);

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
      justification="NPE should never happen; if it does it is a bigger issue")
  public void preCommitStoreFile(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      final byte[] family, final List<Pair<Path, Path>> pairs) throws IOException {
    RegionCoprocessorEnvironment env = ctx.getEnvironment();
    Configuration c = env.getConfiguration();
    if (pairs == null || pairs.isEmpty() ||
        !c.getBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
          HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT)) {
      LOG.debug("Skipping recording bulk load entries in preCommitStoreFile for bulkloaded "
          + "data replication.");
      return;
    }
    // This is completely cheating AND getting a HRegionServer from a RegionServerEnvironment is
    // just going to break. This is all private. Not allowed. Regions shouldn't assume they are
    // hosted in a RegionServer. TODO: fix.
    RegionServerServices rss = ((HasRegionServerServices)env).getRegionServerServices();
    Replication rep = (Replication)((HRegionServer)rss).getReplicationSourceService();
    rep.addHFileRefsToQueue(env.getRegionInfo().getTable(), family, pairs);
  }
}
