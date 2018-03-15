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
package org.apache.hadoop.hbase.coprocessor.example;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.CompactingMemStore;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ CoprocessorTests.class, MediumTests.class })
public class TestWriteHeavyIncrementObserverWithMemStoreCompaction
    extends WriteHeavyIncrementObserverTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWriteHeavyIncrementObserverWithMemStoreCompaction.class);

  @BeforeClass
  public static void setUp() throws Exception {
    WriteHeavyIncrementObserverTestBase.setUp();
    UTIL.getAdmin()
        .createTable(TableDescriptorBuilder.newBuilder(NAME)
            .setCoprocessor(WriteHeavyIncrementObserver.class.getName())
            .setValue(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
              MemoryCompactionPolicy.EAGER.name())
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build());
    TABLE = UTIL.getConnection().getTable(NAME);
  }

  @Test
  public void test() throws Exception {
    // sleep every 10 loops to give memstore compaction enough time to finish before reaching the
    // flush size.
    doIncrement(10);
    assertSum();
    HStore store = UTIL.getHBaseCluster().findRegionsForTable(NAME).get(0).getStore(FAMILY);
    // should have no store files created as we have done aggregating all in memory
    assertEquals(0, store.getStorefilesCount());
  }
}
