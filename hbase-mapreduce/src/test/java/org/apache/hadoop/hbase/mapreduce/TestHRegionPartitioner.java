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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MapReduceTests.class, MediumTests.class})
public class TestHRegionPartitioner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHRegionPartitioner.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Test HRegionPartitioner
   */
  @Test
  public void testHRegionPartitioner() throws Exception {

    byte[][] families = { Bytes.toBytes("familyA"), Bytes.toBytes("familyB") };

    UTIL.createTable(TableName.valueOf(name.getMethodName()), families, 1,
    Bytes.toBytes("aa"), Bytes.toBytes("cc"), 3);

    HRegionPartitioner<Long, Long> partitioner = new HRegionPartitioner<>();
    Configuration configuration = UTIL.getConfiguration();
    configuration.set(TableOutputFormat.OUTPUT_TABLE, name.getMethodName());
    partitioner.setConf(configuration);
    ImmutableBytesWritable writable = new ImmutableBytesWritable(Bytes.toBytes("bb"));

    assertEquals(1, partitioner.getPartition(writable, 10L, 3));
    assertEquals(0, partitioner.getPartition(writable, 10L, 1));
  }
}
