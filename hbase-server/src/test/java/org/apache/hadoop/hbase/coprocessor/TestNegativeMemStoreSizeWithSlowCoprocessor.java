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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that verifies we do not have memstore size negative when a postPut/Delete hook is
 * slow/expensive and a flush is triggered at the same time the coprocessow is doing its work. To
 * simulate this we call flush from the coprocessor itself
 */
@Category(MediumTests.class)
public class TestNegativeMemStoreSizeWithSlowCoprocessor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNegativeMemStoreSizeWithSlowCoprocessor.class);

  static final Logger LOG =
      LoggerFactory.getLogger(TestNegativeMemStoreSizeWithSlowCoprocessor.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] tableName = Bytes.toBytes("test_table");
  private static final byte[] family = Bytes.toBytes("f");
  private static final byte[] qualifier = Bytes.toBytes("q");

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      FlushingRegionObserver.class.getName());
    conf.setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, true);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2); // Let's fail fast.
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TableName.valueOf(tableName), family);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNegativeMemstoreSize() throws IOException, InterruptedException {
    boolean IOEthrown = false;
    Table table = null;
    try {
      table = TEST_UTIL.getConnection().getTable(TableName.valueOf(tableName));

      // Adding data
      Put put1 = new Put(Bytes.toBytes("row1"));
      put1.addColumn(family, qualifier, Bytes.toBytes("Value1"));
      table.put(put1);
      Put put2 = new Put(Bytes.toBytes("row2"));
      put2.addColumn(family, qualifier, Bytes.toBytes("Value2"));
      table.put(put2);
      table.put(put2);
    } catch (IOException e) {
      IOEthrown = true;
    } finally {
      Assert.assertFalse("Shouldn't have thrown an exception", IOEthrown);
      if (table != null) {
        table.close();
      }
    }
  }

  public static class FlushingRegionObserver extends SimpleRegionObserver {

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c, final Put put,
        final WALEdit edit, final Durability durability) throws IOException {
      HRegion region = (HRegion) c.getEnvironment().getRegion();
      super.postPut(c, put, edit, durability);

      if (Bytes.equals(put.getRow(), Bytes.toBytes("row2"))) {
        region.flush(false);
        Assert.assertTrue(region.getMemStoreDataSize() >= 0);
      }
    }
  }
}
