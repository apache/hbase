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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertNotSame;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Package scoped mocking utility.
 */
public class Mocking {

  static void waitForRegionFailedToCloseAndSetToPendingClose(
      AssignmentManager am, HRegionInfo hri) throws InterruptedException {
    // Since region server is fake, sendRegionClose will fail, and closing
    // region will fail. For testing purpose, moving it back to pending close
    boolean wait = true;
    while (wait) {
      RegionState state = am.getRegionStates().getRegionState(hri);
      if (state != null && state.isFailedClose()){
        am.getRegionStates().updateRegionState(hri, State.PENDING_CLOSE);
        wait = false;
      } else {
        Thread.sleep(1);
      }
    }
  }

  static void waitForRegionPendingOpenInRIT(AssignmentManager am, String encodedName)
    throws InterruptedException {
    // We used to do a check like this:
    //!Mocking.verifyRegionState(this.watcher, REGIONINFO, EventType.M_ZK_REGION_OFFLINE)) {
    // There is a race condition with this: because we may do the transition to
    // RS_ZK_REGION_OPENING before the RIT is internally updated. We need to wait for the
    // RIT to be as we need it to be instead. This cannot happen in a real cluster as we
    // update the RIT before sending the openRegion request.

    boolean wait = true;
    while (wait) {
      RegionState state = am.getRegionStates()
        .getRegionsInTransition().get(encodedName);
      if (state != null && state.isPendingOpen()){
        wait = false;
      } else {
        Thread.sleep(1);
      }
    }
  }

  /**
   * Verifies that the specified region is in the specified state in ZooKeeper.
   * <p>
   * Returns true if region is in transition and in the specified state in
   * ZooKeeper.  Returns false if the region does not exist in ZK or is in
   * a different state.
   * <p>
   * Method synchronizes() with ZK so will yield an up-to-date result but is
   * a slow read.
   * @param zkw
   * @param region
   * @param expectedState
   * @return true if region exists and is in expected state
   * @throws DeserializationException
   */
  static boolean verifyRegionState(ZooKeeperWatcher zkw, HRegionInfo region, EventType expectedState)
  throws KeeperException, DeserializationException {
    String encoded = region.getEncodedName();

    String node = ZKAssign.getNodeName(zkw, encoded);
    zkw.sync(node);

    // Read existing data of the node
    byte [] existingBytes = null;
    try {
      existingBytes = ZKUtil.getDataAndWatch(zkw, node);
    } catch (KeeperException.NoNodeException nne) {
      return false;
    } catch (KeeperException e) {
      throw e;
    }
    if (existingBytes == null) return false;
    RegionTransition rt = RegionTransition.parseFrom(existingBytes);
    return rt.getEventType().equals(expectedState);
  }
}
