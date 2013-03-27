/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.encoding;

import static org.apache.hadoop.hbase.io.encoding.TestChangingEncoding.CF;
import static org.apache.hadoop.hbase.io.encoding.TestChangingEncoding.CF_BYTES;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestUpgradeFromHFileV1ToEncoding {

  private static final Log LOG =
      LogFactory.getLog(TestUpgradeFromHFileV1ToEncoding.class);

  private static final String TABLE = "UpgradeTable";
  private static final byte[] TABLE_BYTES = Bytes.toBytes(TABLE);

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Configuration conf = TEST_UTIL.getConfiguration();

  private static final int NUM_HFILE_V1_BATCHES = 10;
  private static final int NUM_HFILE_V2_BATCHES = 20;

  private static final int NUM_SLAVES = 3;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Use a small flush size to create more HFiles.
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024 * 1024);
    conf.setInt(HFile.FORMAT_VERSION_KEY, 1); // Use HFile v1 initially
    TEST_UTIL.startMiniCluster(NUM_SLAVES);
    LOG.debug("Started an HFile v1 cluster");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testUpgrade() throws Exception {
    int numBatches = 0;
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    HColumnDescriptor hcd = new HColumnDescriptor(CF);
    htd.addFamily(hcd);
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(htd);
    admin.close();
    for (int i = 0; i < NUM_HFILE_V1_BATCHES; ++i) {
      TestChangingEncoding.writeTestDataBatch(conf, TABLE, numBatches++);
    }
    TEST_UTIL.shutdownMiniHBaseCluster();

    conf.setInt(HFile.FORMAT_VERSION_KEY, 2);
    TEST_UTIL.startMiniHBaseCluster(1, NUM_SLAVES);
    LOG.debug("Started an HFile v2 cluster");
    admin = new HBaseAdmin(conf);
    htd = admin.getTableDescriptor(TABLE_BYTES);
    hcd = htd.getFamily(CF_BYTES);
    hcd.setDataBlockEncoding(DataBlockEncoding.PREFIX);
    admin.disableTable(TABLE);
    admin.modifyColumn(TABLE, hcd);
    admin.enableTable(TABLE);
    admin.close();
    for (int i = 0; i < NUM_HFILE_V2_BATCHES; ++i) {
      TestChangingEncoding.writeTestDataBatch(conf, TABLE, numBatches++);
    }

    LOG.debug("Verifying all 'batches', both HFile v1 and encoded HFile v2");
    verifyBatches(numBatches);

    LOG.debug("Doing a manual compaction");
    admin.compact(TABLE);
    Thread.sleep(TimeUnit.SECONDS.toMillis(10));

    LOG.debug("Verify all the data again");
    verifyBatches(numBatches);
  }

  private void verifyBatches(int numBatches) throws Exception {
    for (int i = 0; i < numBatches; ++i) {
      TestChangingEncoding.verifyTestDataBatch(conf, TABLE, i);
    }
  }

}
