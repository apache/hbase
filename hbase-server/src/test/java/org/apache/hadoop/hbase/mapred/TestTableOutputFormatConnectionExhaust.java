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
package org.apache.hadoop.hbase.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.junit.Assert.fail;

/**
 * Spark creates many instances of TableOutputFormat within a single process.  We need to make
 * sure we can have many instances and not leak connections.
 *
 * This test creates a few TableOutputFormats and shouldn't fail due to ZK connection exhaustion.
 */
@Category(MediumTests.class)
public class TestTableOutputFormatConnectionExhaust {

  private static final Log LOG =
      LogFactory.getLog(TestTableOutputFormatConnectionExhaust.class);

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  static final String TABLE = "TestTableOutputFormatConnectionExhaust";
  static final String FAMILY = "family";

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Default in ZookeeperMiniCluster is 1000, setting artificially low to trigger exhaustion.
    // need min of 7 to properly start the default mini HBase cluster
    UTIL.getConfiguration().setInt(HConstants.ZOOKEEPER_MAX_CLIENT_CNXNS, 10);
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws IOException {
    LOG.info("before");
    UTIL.ensureSomeRegionServersAvailable(1);
    LOG.info("before done");
  }

  /**
   * Open and close a TableOutputFormat.  The closing the RecordWriter should release HBase
   * Connection (ZK) resources, and will throw exception if they are exhausted.
   */
  static void openCloseTableOutputFormat(int iter)  throws IOException {
    LOG.info("Instantiating TableOutputFormat connection  " + iter);
    JobConf conf = new JobConf();
    conf.addResource(UTIL.getConfiguration());
    conf.set(TableOutputFormat.OUTPUT_TABLE, TABLE);
    TableMapReduceUtil.initTableMapJob(TABLE, FAMILY, TableMap.class,
        ImmutableBytesWritable.class, ImmutableBytesWritable.class, conf);
    TableOutputFormat tof = new TableOutputFormat();
    RecordWriter rw = tof.getRecordWriter(null, conf, TABLE, null);
    rw.close(null);
  }

  @Test
  public void testConnectionExhaustion() throws IOException {
    int MAX_INSTANCES = 5; // fails on iteration 3 if zk connections leak
    for (int i = 0; i < MAX_INSTANCES; i++) {
      final int iter = i;
      try {
        openCloseTableOutputFormat(iter);
      } catch (Exception e) {
        LOG.error("Exception encountered", e);
        fail("Failed on iteration " + i);
      }
    }
  }

}
