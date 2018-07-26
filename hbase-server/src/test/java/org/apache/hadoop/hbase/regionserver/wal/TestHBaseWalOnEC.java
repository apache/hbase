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

package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestHBaseWalOnEC {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseWalOnEC.class);

  private static final HBaseTestingUtility util = new HBaseTestingUtility();

  private static final String HFLUSH = "hflush";

  @BeforeClass
  public static void setup() throws Exception {
    try {
      MiniDFSCluster cluster = util.startMiniDFSCluster(3); // Need 3 DNs for RS-3-2 policy
      DistributedFileSystem fs = cluster.getFileSystem();

      Method enableAllECPolicies = DFSTestUtil.class.getMethod("enableAllECPolicies",
          DistributedFileSystem.class);
      enableAllECPolicies.invoke(null, fs);

      DFSClient client = fs.getClient();
      Method setErasureCodingPolicy = DFSClient.class.getMethod("setErasureCodingPolicy",
          String.class, String.class);
      setErasureCodingPolicy.invoke(client, "/", "RS-3-2-1024k"); // try a built-in policy

      try (FSDataOutputStream out = fs.create(new Path("/canary"))) {
        // If this comes back as having hflush then some test setup assumption is wrong.
        // Fail the test so that a developer has to look and triage
        assertFalse("Did not enable EC!", CommonFSUtils.hasCapability(out, HFLUSH));
      }
    } catch (NoSuchMethodException e) {
      // We're not testing anything interesting if EC is not available, so skip the rest of the test
      Assume.assumeNoException("Using an older version of hadoop; EC not available.", e);
    }

    util.getConfiguration().setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, true);
    util.startMiniCluster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testStreamCreate() throws IOException {
    try (FSDataOutputStream out = CommonFSUtils.createForWal(util.getDFSCluster().getFileSystem(),
        new Path("/testStreamCreate"), true)) {
      assertTrue(CommonFSUtils.hasCapability(out, HFLUSH));
    }
  }

  @Test
  public void testFlush() throws IOException {
    byte[] row = Bytes.toBytes("row");
    byte[] cf = Bytes.toBytes("cf");
    byte[] cq = Bytes.toBytes("cq");
    byte[] value = Bytes.toBytes("value");

    TableName name = TableName.valueOf(getClass().getSimpleName());

    Table t = util.createTable(name, cf);
    t.put(new Put(row).addColumn(cf, cq, value));

    util.getAdmin().flush(name);

    assertArrayEquals(value, t.get(new Get(row)).getValue(cf, cq));
  }
}

