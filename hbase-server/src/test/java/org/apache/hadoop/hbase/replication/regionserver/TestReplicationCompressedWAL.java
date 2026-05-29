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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestReplicationCompressedWAL extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationCompressedWAL.class);

  static final Logger LOG = LoggerFactory.getLogger(TestReplicationCompressedWAL.class);
  static final int NUM_BATCHES = 20;
  static final int NUM_ROWS_PER_BATCH = 100;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    CONF1.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    TestReplicationBase.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TestReplicationBase.tearDownAfterClass();
  }

  @Test
  public void testMultiplePuts() throws Exception {
    runMultiplePutTest();
  }

  protected static void runMultiplePutTest() throws IOException, InterruptedException {
    for (int i = 0; i < NUM_BATCHES; i++) {
      putBatch(i);
      getBatch(i);
    }
  }

  protected static void getBatch(int batch) throws IOException, InterruptedException {
    for (int i = 0; i < NUM_ROWS_PER_BATCH; i++) {
      byte[] row = getRowKey(batch, i);
      Get get = new Get(row);
      for (int j = 0; j < NB_RETRIES; j++) {
        if (j == NB_RETRIES - 1) {
          fail("Waited too much time for replication");
        }
        Result res = htable2.get(get);
        if (res.isEmpty()) {
          LOG.info("Row not available");
          Thread.sleep(SLEEP_TIME);
        } else {
          assertArrayEquals(row, res.value());
          break;
        }
      }
    }
  }

  protected static byte[] getRowKey(int batch, int count) {
    return Bytes.toBytes("row" + ((batch * NUM_ROWS_PER_BATCH) + count));
  }

  protected static void putBatch(int batch) throws IOException {
    for (int i = 0; i < NUM_ROWS_PER_BATCH; i++) {
      byte[] row = getRowKey(batch, i);
      Put put = new Put(row);
      put.addColumn(famName, row, row);
      htable1.put(put);
    }
  }

}
