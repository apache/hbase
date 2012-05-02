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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.DeserializationException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Package scoped mocking utility.
 */
public class Mocking {
  /**
   * @param sn ServerName to use making startcode and server in meta
   * @param hri Region to serialize into HRegionInfo
   * @return A mocked up Result that fakes a Get on a row in the
   * <code>.META.</code> table.
   * @throws IOException 
   */
  static Result getMetaTableRowResult(final HRegionInfo hri,
      final ServerName sn)
  throws IOException {
    // TODO: Move to a utilities class.  More than one test case can make use
    // of this facility.
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY,
      HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
      Writables.getBytes(hri)));
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY,
      HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
      Bytes.toBytes(sn.getHostAndPort())));
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY,
      HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
      Bytes.toBytes(sn.getStartcode())));
    return new Result(kvs);
  }

  /**
   * Fakes the regionserver-side zk transitions of a region open.
   * @param w ZooKeeperWatcher to use.
   * @param sn Name of the regionserver doing the 'opening'
   * @param hri Region we're 'opening'.
   * @throws KeeperException
   * @throws DeserializationException 
   */
  static void fakeRegionServerRegionOpenInZK(final ZooKeeperWatcher w,
      final ServerName sn, final HRegionInfo hri)
  throws KeeperException, DeserializationException {
    // Wait till we see the OFFLINE zk node before we proceed.
    while (!verifyRegionState(w, hri, EventType.M_ZK_REGION_OFFLINE)) {
      Threads.sleep(1);
    }
    // Get current versionid else will fail on transition from OFFLINE to OPENING below
    int versionid = ZKAssign.getVersion(w, hri);
    assertNotSame(-1, versionid);
    // This uglyness below is what the openregionhandler on RS side does.  I
    // looked at exposing the method over in openregionhandler but its just a
    // one liner and its deep over in another package so just repeat it below.
    versionid = ZKAssign.transitionNode(w, hri, sn,
      EventType.M_ZK_REGION_OFFLINE, EventType.RS_ZK_REGION_OPENING, versionid);
    assertNotSame(-1, versionid);
    // Move znode from OPENING to OPENED as RS does on successful open.
    versionid = ZKAssign.transitionNodeOpened(w, hri, sn, versionid);
    assertNotSame(-1, versionid);
    // We should be done now.  The master open handler will notice the
    // transition and remove this regions znode.
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
