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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WALCorruptionWithMultiPutDueToDanglingByteBufferTestBase {

  private static final Logger LOG = LoggerFactory
    .getLogger(WALCorruptionWithMultiPutDueToDanglingByteBufferTestBase.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  protected static CountDownLatch ARRIVE;

  protected static CountDownLatch RESUME;

  protected static TableName TABLE_NAME = TableName
    .valueOf("WALCorruptionWithMultiPutDueToDanglingByteBufferTestBase");

  protected static byte[] CF = Bytes.toBytes("cf");

  protected static byte[] CQ = Bytes.toBytes("cq");

  private byte[] getBytes(String prefix, int index) {
    return Bytes.toBytes(String.format("%s-%08d", prefix, index));
  }

  @Test
  public void test() throws Exception {
    LOG.info("Stop WAL appending...");
    ARRIVE = new CountDownLatch(1);
    RESUME = new CountDownLatch(1);
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      LOG.info("Put totally 100 rows in batches of 5 with " + Durability.ASYNC_WAL + "...");
      int batchSize = 5;
      List<Put> puts = new ArrayList<>(batchSize);
      for (int i = 1; i <= 100; i++) {
        Put p = new Put(getBytes("row", i)).addColumn(CF, CQ, getBytes("value", i))
          .setDurability(Durability.ASYNC_WAL);
        puts.add(p);
        if (i % batchSize == 0) {
          table.put(puts);
          LOG.info("Wrote batch of {} rows from row {}", batchSize,
            Bytes.toString(puts.get(0).getRow()));
          puts.clear();
          // Wait for few of the minibatches in 1st batch of puts to go through the WAL write.
          // The WAL write will pause then
          if (ARRIVE != null) {
            ARRIVE.await();
            ARRIVE = null;
          }
        }
      }
      LOG.info("Resume WAL appending...");
      RESUME.countDown();
      LOG.info("Put a single row to force a WAL sync...");
      table.put(new Put(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("value")));
      LOG.info("Abort the only region server");
      UTIL.getMiniHBaseCluster().abortRegionServer(0);
      LOG.info("Start a new region server");
      UTIL.getMiniHBaseCluster().startRegionServerAndWait(30000);
      UTIL.waitTableAvailable(TABLE_NAME);
      LOG.info("Check if all rows are still valid");
      for (int i = 1; i <= 100; i++) {
        Result result = table.get(new Get(getBytes("row", i)));
        assertEquals(Bytes.toString(getBytes("value", i)), Bytes.toString(result.getValue(CF, CQ)));
      }
      Result result = table.get(new Get(Bytes.toBytes("row")));
      assertEquals("value", Bytes.toString(result.getValue(CF, CQ)));
    }
  }
}
