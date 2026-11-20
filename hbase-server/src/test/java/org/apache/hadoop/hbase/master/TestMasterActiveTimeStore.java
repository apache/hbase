/*
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

import static org.apache.hadoop.hbase.master.region.MasterRegionFactory.STATE_FAMILY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestMasterActiveTimeStore extends MasterStateStoreTestBase {

  @AfterEach
  public void tearDown() throws Exception {
    cleanup();
  }

  @Test
  public void testFirstActiveMasterUsesItsClock() throws Exception {
    assertEquals(1000, new MasterActiveTimeStore(REGION).advance(1000));
  }

  @Test
  public void testActiveTimeIncreasesWhenClockIsBehind() throws Exception {
    assertEquals(1000, new MasterActiveTimeStore(REGION).advance(1000));
    // the next active master's clock is behind the previous one's
    assertEquals(1001, new MasterActiveTimeStore(REGION).advance(900));
    // the same time as the last active time
    assertEquals(1002, new MasterActiveTimeStore(REGION).advance(1001));
  }

  @Test
  public void testActiveTimeFollowsClockWhenAhead() throws Exception {
    assertEquals(1000, new MasterActiveTimeStore(REGION).advance(1000));
    assertEquals(5000, new MasterActiveTimeStore(REGION).advance(5000));
    assertEquals(5001, new MasterActiveTimeStore(REGION).advance(4000));
  }

  @Test
  public void testMalformedValueIsIgnored() throws Exception {
    REGION.update(r -> r.put(new Put(Bytes.toBytes(MasterActiveTimeStore.STATE_NAME))
      .addColumn(STATE_FAMILY, Bytes.toBytes("d"), new byte[3])));
    assertEquals(1000, new MasterActiveTimeStore(REGION).advance(1000));
    assertEquals(1001, new MasterActiveTimeStore(REGION).advance(900));
  }
}
