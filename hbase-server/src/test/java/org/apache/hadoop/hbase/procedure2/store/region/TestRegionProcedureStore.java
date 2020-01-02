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
package org.apache.hadoop.hbase.procedure2.store.region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.LoadCounter;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestRegionProcedureStore extends RegionProcedureStoreTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionProcedureStore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionProcedureStore.class);

  private void verifyProcIdsOnRestart(final Set<Long> procIds) throws Exception {
    LOG.debug("expected: " + procIds);
    LoadCounter loader = new LoadCounter();
    ProcedureTestingUtility.storeRestart(store, true, loader);
    assertEquals(procIds.size(), loader.getLoadedCount());
    assertEquals(0, loader.getCorruptedCount());
  }

  @Test
  public void testLoad() throws Exception {
    Set<Long> procIds = new HashSet<>();

    // Insert something in the log
    RegionProcedureStoreTestProcedure proc1 = new RegionProcedureStoreTestProcedure();
    procIds.add(proc1.getProcId());
    store.insert(proc1, null);

    RegionProcedureStoreTestProcedure proc2 = new RegionProcedureStoreTestProcedure();
    RegionProcedureStoreTestProcedure proc3 = new RegionProcedureStoreTestProcedure();
    proc3.setParent(proc2);
    RegionProcedureStoreTestProcedure proc4 = new RegionProcedureStoreTestProcedure();
    proc4.setParent(proc2);

    procIds.add(proc2.getProcId());
    procIds.add(proc3.getProcId());
    procIds.add(proc4.getProcId());
    store.insert(proc2, new Procedure[] { proc3, proc4 });

    // Verify that everything is there
    verifyProcIdsOnRestart(procIds);

    // Update and delete something
    proc1.finish();
    store.update(proc1);
    proc4.finish();
    store.update(proc4);
    store.delete(proc4.getProcId());
    procIds.remove(proc4.getProcId());

    // Verify that everything is there
    verifyProcIdsOnRestart(procIds);
  }

  @Test
  public void testCleanup() throws Exception {
    RegionProcedureStoreTestProcedure proc1 = new RegionProcedureStoreTestProcedure();
    store.insert(proc1, null);
    RegionProcedureStoreTestProcedure proc2 = new RegionProcedureStoreTestProcedure();
    store.insert(proc2, null);
    RegionProcedureStoreTestProcedure proc3 = new RegionProcedureStoreTestProcedure();
    store.insert(proc3, null);
    LoadCounter loader = new LoadCounter();
    store.load(loader);
    assertEquals(proc3.getProcId(), loader.getMaxProcId());
    assertEquals(3, loader.getRunnableCount());

    store.delete(proc3.getProcId());
    store.delete(proc2.getProcId());
    loader = new LoadCounter();
    store.load(loader);
    assertEquals(proc3.getProcId(), loader.getMaxProcId());
    assertEquals(1, loader.getRunnableCount());

    // the row should still be there
    assertTrue(store.region
      .get(new Get(Bytes.toBytes(proc3.getProcId())).setCheckExistenceOnly(true)).getExists());
    assertTrue(store.region
      .get(new Get(Bytes.toBytes(proc2.getProcId())).setCheckExistenceOnly(true)).getExists());

    // proc2 will be deleted after cleanup, but proc3 should still be there as it holds the max proc
    // id
    store.cleanup();
    assertTrue(store.region
      .get(new Get(Bytes.toBytes(proc3.getProcId())).setCheckExistenceOnly(true)).getExists());
    assertFalse(store.region
      .get(new Get(Bytes.toBytes(proc2.getProcId())).setCheckExistenceOnly(true)).getExists());

    RegionProcedureStoreTestProcedure proc4 = new RegionProcedureStoreTestProcedure();
    store.insert(proc4, null);
    store.cleanup();
    // proc3 should also be deleted as now proc4 holds the max proc id
    assertFalse(store.region
      .get(new Get(Bytes.toBytes(proc3.getProcId())).setCheckExistenceOnly(true)).getExists());
  }
}