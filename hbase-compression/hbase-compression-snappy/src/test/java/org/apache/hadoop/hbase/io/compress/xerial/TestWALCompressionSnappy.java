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
package org.apache.hadoop.hbase.io.compress.xerial;

import static org.junit.Assume.assumeTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.wal.CompressionContext;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.wal.CompressedWALTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestWALCompressionSnappy extends CompressedWALTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALCompressionSnappy.class);

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    assumeTrue(SnappyCodec.isLoaded());
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(Compression.SNAPPY_CODEC_CLASS_KEY, SnappyCodec.class.getCanonicalName());
    Compression.Algorithm.SNAPPY.reload(conf);
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    conf.setBoolean(CompressionContext.ENABLE_WAL_VALUE_COMPRESSION, true);
    conf.set(CompressionContext.WAL_VALUE_COMPRESSION_TYPE, Compression.Algorithm.SNAPPY.getName());
    TEST_UTIL.startMiniDFSCluster(3);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName().replaceAll("[^a-zA-Z0-9]", "_"));
    doTest(tableName);
  }

}
