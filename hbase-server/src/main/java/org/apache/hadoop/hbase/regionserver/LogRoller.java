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
package org.apache.hadoop.hbase.regionserver;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.wal.AbstractWALRoller;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs periodically to determine if the WAL should be rolled.
 *
 * NOTE: This class extends Thread rather than Chore because the sleep time
 * can be interrupted when there is something to do, rather than the Chore
 * sleep time which is invariant.
 *
 * TODO: change to a pool of threads
 */
@InterfaceAudience.Private
public class LogRoller extends AbstractWALRoller<RegionServerServices> {
  private static final Logger LOG = LoggerFactory.getLogger(LogRoller.class);

  public LogRoller(RegionServerServices services) {
    super("LogRoller", services.getConfiguration(), services);
  }

  protected void scheduleFlush(String encodedRegionName, List<byte[]> families) {
    RegionServerServices services = this.abortable;
    HRegion r = (HRegion) services.getRegion(encodedRegionName);
    if (r == null) {
      LOG.warn("Failed to schedule flush of {}, because it is not online on us", encodedRegionName);
      return;
    }
    FlushRequester requester = services.getFlushRequester();
    if (requester == null) {
      LOG.warn("Failed to schedule flush of {}, region={}, because FlushRequester is null",
        encodedRegionName, r);
      return;
    }
    // flush specified stores to clean old logs
    requester.requestFlush(r, families, FlushLifeCycleTracker.DUMMY);
  }

  Map<WAL, RollController> getWalNeedsRoll() {
    return this.wals;
  }
}
