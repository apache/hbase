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
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ CoprocessorTests.class, MediumTests.class })
public class TestWriteHeavyIncrementObserver extends WriteHeavyIncrementObserverTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWriteHeavyIncrementObserver.class);

  @BeforeClass
  public static void setUp() throws Exception {
    WriteHeavyIncrementObserverTestBase.setUp();
    UTIL.getAdmin()
        .createTable(TableDescriptorBuilder.newBuilder(NAME)
            .setCoprocessor(WriteHeavyIncrementObserver.class.getName())
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build());
    TABLE = UTIL.getConnection().getTable(NAME);
  }

  @Test
  public void test() throws Exception {
    doIncrement(0);
    assertSum();
    // we do not hack scan operation so using scan we could get the original values added into the
    // table.
    try (ResultScanner scanner = TABLE.getScanner(new Scan().withStartRow(ROW)
      .withStopRow(ROW, true).addFamily(FAMILY).readAllVersions().setAllowPartialResults(true))) {
      Result r = scanner.next();
      assertTrue(r.rawCells().length > 2);
    }
    UTIL.flush(NAME);
    HRegion region = UTIL.getHBaseCluster().findRegionsForTable(NAME).get(0);
    HStore store = region.getStore(FAMILY);
    for (;;) {
      region.compact(true);
      if (store.getStorefilesCount() == 1) {
        break;
      }
    }
    assertSum();
    // Should only have two cells after flush and major compaction
    try (ResultScanner scanner = TABLE.getScanner(new Scan().withStartRow(ROW)
      .withStopRow(ROW, true).addFamily(FAMILY).readAllVersions().setAllowPartialResults(true))) {
      Result r = scanner.next();
      assertEquals(2, r.rawCells().length);
    }
  }
}
