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
package org.apache.hadoop.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, MediumTests.class })
public class TestMetaSplitBySize {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetaSplitBySize.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setLong("hbase.increasing.policy.initial.size", 1024);
    UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws IOException {
    byte[] family = Bytes.toBytes("family");
    for (int i = 0; i < 10; i++) {
      UTIL.createTable(TableName.valueOf("table_" + i), family);
    }
    HRegion region = UTIL.getMiniHBaseCluster().getRegions(TableName.META_TABLE_NAME).get(0);
    region.requestFlush(FlushLifeCycleTracker.DUMMY);
    UTIL.waitFor(30000, () -> UTIL.getAdmin().getRegions(TableName.META_TABLE_NAME).size() > 1);
  }
}
