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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A write/read/verify load test on a mini HBase cluster. Tests reading
 * and then writing.
 */
@Category({MiscTests.class, MediumTests.class})
@RunWith(Parameterized.class)
public class TestMiniClusterLoadSequential {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMiniClusterLoadSequential.class);

  private static final Logger LOG = LoggerFactory.getLogger(
      TestMiniClusterLoadSequential.class);

  protected static final TableName TABLE =
      TableName.valueOf("load_test_tbl");
  protected static final byte[] CF = Bytes.toBytes("load_test_cf");
  protected static final int NUM_THREADS = 8;
  protected static final int NUM_RS = 2;
  protected static final int TIMEOUT_MS = 180000;
  protected static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  protected final Configuration conf = TEST_UTIL.getConfiguration();
  protected final boolean isMultiPut;
  protected final DataBlockEncoding dataBlockEncoding;

  protected MultiThreadedWriter writerThreads;
  protected MultiThreadedReader readerThreads;
  protected int numKeys;

  protected Compression.Algorithm compression = Compression.Algorithm.NONE;

  public TestMiniClusterLoadSequential(boolean isMultiPut,
      DataBlockEncoding dataBlockEncoding) {
    this.isMultiPut = isMultiPut;
    this.dataBlockEncoding = dataBlockEncoding;
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024 * 1024);

    // We don't want any region reassignments by the load balancer during the test.
    conf.setFloat(HConstants.LOAD_BALANCER_SLOP_KEY, 10.0f);
  }

  @Parameters
  public static Collection<Object[]> parameters() {
    List<Object[]> parameters = new ArrayList<>();
    for (boolean multiPut : new boolean[]{false, true}) {
      for (DataBlockEncoding dataBlockEncoding : new DataBlockEncoding[] {
          DataBlockEncoding.NONE, DataBlockEncoding.PREFIX }) {
        parameters.add(new Object[]{multiPut, dataBlockEncoding});
      }
    }
    return parameters;
  }

  @Before
  public void setUp() throws Exception {
    LOG.debug("Test setup: isMultiPut=" + isMultiPut);
    TEST_UTIL.startMiniCluster(NUM_RS);
  }

  @After
  public void tearDown() throws Exception {
    LOG.debug("Test teardown: isMultiPut=" + isMultiPut);
    TEST_UTIL.shutdownMiniCluster();
  }

  protected MultiThreadedReader prepareReaderThreads(LoadTestDataGenerator dataGen,
      Configuration conf, TableName tableName, double verifyPercent) throws IOException {
    MultiThreadedReader reader = new MultiThreadedReader(dataGen, conf, tableName, verifyPercent);
    return reader;
  }

  protected MultiThreadedWriter prepareWriterThreads(LoadTestDataGenerator dataGen,
      Configuration conf, TableName tableName) throws IOException {
    MultiThreadedWriter writer = new MultiThreadedWriter(dataGen, conf, tableName);
    writer.setMultiPut(isMultiPut);
    return writer;
  }

  @Test
  public void loadTest() throws Exception {
    prepareForLoadTest();
    runLoadTestOnExistingTable();
  }

  protected void runLoadTestOnExistingTable() throws IOException {
    writerThreads.start(0, numKeys, NUM_THREADS);
    writerThreads.waitForFinish();
    assertEquals(0, writerThreads.getNumWriteFailures());

    readerThreads.start(0, numKeys, NUM_THREADS);
    readerThreads.waitForFinish();
    assertEquals(0, readerThreads.getNumReadFailures());
    assertEquals(0, readerThreads.getNumReadErrors());
    assertEquals(numKeys, readerThreads.getNumKeysVerified());
  }

  protected void createPreSplitLoadTestTable(HTableDescriptor htd, HColumnDescriptor hcd)
      throws IOException {
    HBaseTestingUtility.createPreSplitLoadTestTable(conf, htd, hcd);
    TEST_UTIL.waitUntilAllRegionsAssigned(htd.getTableName());
  }

  protected void prepareForLoadTest() throws IOException {
    LOG.info("Starting load test: dataBlockEncoding=" + dataBlockEncoding +
        ", isMultiPut=" + isMultiPut);
    numKeys = numKeys();
    Admin admin = TEST_UTIL.getAdmin();
    while (admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
                .getLiveServerMetrics().size() < NUM_RS) {
      LOG.info("Sleeping until " + NUM_RS + " RSs are online");
      Threads.sleepWithoutInterrupt(1000);
    }
    admin.close();

    HTableDescriptor htd = new HTableDescriptor(TABLE);
    HColumnDescriptor hcd = new HColumnDescriptor(CF)
      .setCompressionType(compression)
      .setDataBlockEncoding(dataBlockEncoding);
    createPreSplitLoadTestTable(htd, hcd);

    LoadTestDataGenerator dataGen = new MultiThreadedAction.DefaultDataGenerator(CF);
    writerThreads = prepareWriterThreads(dataGen, conf, TABLE);
    readerThreads = prepareReaderThreads(dataGen, conf, TABLE, 100);
  }

  protected int numKeys() {
    return 1000;
  }

  protected HColumnDescriptor getColumnDesc(Admin admin)
      throws TableNotFoundException, IOException {
    return admin.getTableDescriptor(TABLE).getFamily(CF);
  }

}
