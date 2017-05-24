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
package org.apache.hadoop.hbase.master.assignment;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.util.Threads;

import static org.junit.Assert.assertEquals;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AssignmentTestingUtil {
  private static final Log LOG = LogFactory.getLog(AssignmentTestingUtil.class);

  private AssignmentTestingUtil() {}

  public static void waitForRegionToBeInTransition(final HBaseTestingUtility util,
      final HRegionInfo hri) throws Exception {
    while (!getMaster(util).getAssignmentManager().getRegionStates().isRegionInTransition(hri)) {
      Threads.sleep(10);
    }
  }

  public static void waitForRsToBeDead(final HBaseTestingUtility util,
      final ServerName serverName) throws Exception {
    util.waitFor(60000, new ExplainingPredicate<Exception>() {
      @Override
      public boolean evaluate() {
        return getMaster(util).getServerManager().isServerDead(serverName);
      }

      @Override
      public String explainFailure() {
        return "Server " + serverName + " is not dead";
      }
    });
  }

  public static void stopRs(final HBaseTestingUtility util, final ServerName serverName)
      throws Exception {
    LOG.info("STOP REGION SERVER " + serverName);
    util.getMiniHBaseCluster().stopRegionServer(serverName);
    waitForRsToBeDead(util, serverName);
  }

  public static void killRs(final HBaseTestingUtility util, final ServerName serverName)
      throws Exception {
    LOG.info("KILL REGION SERVER " + serverName);
    util.getMiniHBaseCluster().killRegionServer(serverName);
    waitForRsToBeDead(util, serverName);
  }

  public static void crashRs(final HBaseTestingUtility util, final ServerName serverName,
      final boolean kill) throws Exception {
    if (kill) {
      killRs(util, serverName);
    } else {
      stopRs(util, serverName);
    }
  }

  public static ServerName crashRsWithRegion(final HBaseTestingUtility util,
      final HRegionInfo hri, final boolean kill) throws Exception {
    ServerName serverName = getServerHoldingRegion(util, hri);
    crashRs(util, serverName, kill);
    return serverName;
  }

  public static ServerName getServerHoldingRegion(final HBaseTestingUtility util,
      final HRegionInfo hri) throws Exception {
    ServerName serverName = util.getMiniHBaseCluster().getServerHoldingRegion(
      hri.getTable(), hri.getRegionName());
    ServerName amServerName = getMaster(util).getAssignmentManager().getRegionStates()
      .getRegionServerOfRegion(hri);

    // Make sure AM and MiniCluster agrees on the Server holding the region
    // and that the server is online.
    assertEquals(amServerName, serverName);
    assertEquals(true, getMaster(util).getServerManager().isServerOnline(serverName));
    return serverName;
  }

  public static boolean isServerHoldingMeta(final HBaseTestingUtility util,
      final ServerName serverName) throws Exception {
    for (HRegionInfo hri: getMetaRegions(util)) {
      if (serverName.equals(getServerHoldingRegion(util, hri))) {
        return true;
      }
    }
    return false;
  }

  public static Set<HRegionInfo> getMetaRegions(final HBaseTestingUtility util) {
    return getMaster(util).getAssignmentManager().getMetaRegionSet();
  }

  private static HMaster getMaster(final HBaseTestingUtility util) {
    return util.getMiniHBaseCluster().getMaster();
  }
}