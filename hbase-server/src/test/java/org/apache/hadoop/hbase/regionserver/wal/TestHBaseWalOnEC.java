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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

@Tag(RegionServerTests.TAG)
@Tag(LargeTests.TAG)
@HBaseParameterizedTestTemplate
public class TestHBaseWalOnEC {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    try {
      MiniDFSCluster cluster = UTIL.startMiniDFSCluster(3); // Need 3 DNs for RS-3-2 policy
      DistributedFileSystem fs = cluster.getFileSystem();

      Method enableAllECPolicies =
        DFSTestUtil.class.getMethod("enableAllECPolicies", DistributedFileSystem.class);
      enableAllECPolicies.invoke(null, fs);

      DFSClient client = fs.getClient();
      Method setErasureCodingPolicy =
        DFSClient.class.getMethod("setErasureCodingPolicy", String.class, String.class);
      setErasureCodingPolicy.invoke(client, "/", "RS-3-2-1024k"); // try a built-in policy

      try (FSDataOutputStream out = fs.create(new Path("/canary"))) {
        // If this comes back as having hflush then some test setup assumption is wrong.
        // Fail the test so that a developer has to look and triage
        assertFalse(out.hasCapability(StreamCapabilities.HFLUSH), "Did not enable EC!");
      }
    } catch (NoSuchMethodException e) {
      // We're not testing anything interesting if EC is not available, so skip the rest of the test
      Assumptions.assumeTrue(false, "Using an older version of hadoop; EC not available.");
    }

    UTIL.getConfiguration().setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, true);

  }

  private final String walProvider;

  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of("asyncfs"), Arguments.of("filesystem"));
  }

  public TestHBaseWalOnEC(String walProvider) {
    this.walProvider = walProvider;
  }

  @BeforeEach
  public void setUp() throws Exception {
    UTIL.getConfiguration().set(WALFactory.WAL_PROVIDER, walProvider);
    UTIL.startMiniCluster(3);
  }

  @AfterEach
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @TestTemplate
  public void testReadWrite() throws IOException {
    byte[] row = Bytes.toBytes("row");
    byte[] cf = Bytes.toBytes("cf");
    byte[] cq = Bytes.toBytes("cq");
    byte[] value = Bytes.toBytes("value");

    TableName name = TableName.valueOf(getClass().getSimpleName());

    Table t = UTIL.createTable(name, cf);
    t.put(new Put(row).addColumn(cf, cq, value));

    UTIL.getAdmin().flush(name);

    assertArrayEquals(value, t.get(new Get(row)).getValue(cf, cq));
  }
}
