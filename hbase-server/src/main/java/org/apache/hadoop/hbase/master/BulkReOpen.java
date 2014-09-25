/**
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;

/**
 * Performs bulk reopen of the list of regions provided to it.
 */
@InterfaceAudience.Private
public class BulkReOpen extends BulkAssigner {
  private final Map<ServerName, List<HRegionInfo>> rsToRegions;
  private final AssignmentManager assignmentManager;
  private static final Log LOG = LogFactory.getLog(BulkReOpen.class);

  public BulkReOpen(final Server server,
      final Map<ServerName, List<HRegionInfo>> serverToRegions,
    final AssignmentManager am) {
    super(server);
    this.assignmentManager = am;
    this.rsToRegions = serverToRegions;
  }

  /**
   * Unassign all regions, so that they go through the regular region
   * assignment flow (in assignment manager) and are re-opened.
   */
  @Override
  protected void populatePool(ExecutorService pool) {
    LOG.debug("Creating threads for each region server ");
    for (Map.Entry<ServerName, List<HRegionInfo>> e : rsToRegions
        .entrySet()) {
      final List<HRegionInfo> hris = e.getValue();
      // add plans for the regions that need to be reopened
      Map<String, RegionPlan> plans = new HashMap<String, RegionPlan>();
      for (HRegionInfo hri : hris) {
        RegionPlan reOpenPlan = assignmentManager.getRegionReopenPlan(hri);
        plans.put(hri.getEncodedName(), reOpenPlan);
      }
      assignmentManager.addPlans(plans);
      pool.execute(new Runnable() {
        public void run() {
          try {
            unassign(hris);
          } catch (Throwable t) {
            LOG.warn("Failed bulking re-open " + hris.size()
              + " region(s)", t);
          }
        }
      });
    }
  }

 /**
  * Reopen the regions asynchronously, so always returns true immediately.
  * @return true
  */
  @Override
  protected boolean waitUntilDone(long timeout) {
    return true;
  }

  /**
   * Configuration knobs "hbase.bulk.reopen.threadpool.size" number of regions
   * that can be reopened concurrently. The maximum number of threads the master
   * creates is never more than the number of region servers.
   * If configuration is not defined it defaults to 20
   */
  protected int getThreadCount() {
    int defaultThreadCount = super.getThreadCount();
    return this.server.getConfiguration().getInt(
        "hbase.bulk.reopen.threadpool.size", defaultThreadCount);
  }

  public boolean bulkReOpen() throws InterruptedException, IOException {
    return bulkAssign();
  }

  /**
   * Unassign the list of regions. Configuration knobs:
   * hbase.bulk.waitbetween.reopen indicates the number of milliseconds to
   * wait before unassigning another region from this region server
   *
   * @param regions
   * @throws InterruptedException
   */
  private void unassign(
      List<HRegionInfo> regions) throws InterruptedException {
    int waitTime = this.server.getConfiguration().getInt(
        "hbase.bulk.waitbetween.reopen", 0);
    RegionStates regionStates = assignmentManager.getRegionStates();
    for (HRegionInfo region : regions) {
      if (server.isStopped()) {
        return;
      }
      if (regionStates.isRegionInTransition(region)) {
        continue;
      }
      assignmentManager.unassign(region, false);
      while (regionStates.isRegionInTransition(region)
          && !server.isStopped()) {
        regionStates.waitForUpdate(100);
      }
      if (waitTime > 0 && !server.isStopped()) {
        Thread.sleep(waitTime);
      }
    }
  }
}
