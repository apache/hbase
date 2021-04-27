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
package org.apache.hadoop.hbase.replication.replicationserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.regionserver.WALFileLengthProvider;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MediumTests.class})
public class TestRemoteWALFileLengthProvider {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRemoteWALFileLengthProvider.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRemoteWALFileLengthProvider.class);

  @Rule
  public final TestName name = new TestName();

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final byte[] CF = Bytes.toBytes("C");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    Table table = UTIL.createTable(tableName, CF);
    UTIL.waitUntilAllRegionsAssigned(tableName);
    assertEquals(1, UTIL.getMiniHBaseCluster().getNumLiveRegionServers());

    // Find the RS which holds test table regions.
    HRegionServer rs =
      UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
        .map(JVMClusterUtil.RegionServerThread::getRegionServer)
        .filter(s -> !s.getRegions(tableName).isEmpty())
        .findFirst().get();
    assertNotNull(rs);

    // Put some data and request rolling log, make multiple wals.
    table.put(new Put(Bytes.toBytes("r1")).addColumn(CF, CF, Bytes.toBytes("v")));
    rs.getWalRoller().requestRollAll();
    table.put(new Put(Bytes.toBytes("r2")).addColumn(CF, CF, Bytes.toBytes("v")));
    UTIL.waitFor(60000, rs::walRollRequestFinished);

    WALFileLengthProvider rsLengthProvider =
      rs.getWalFactory().getWALProvider().getWALFileLengthProvider();
    WALFileLengthProvider remoteLengthProvider =
      new RemoteWALFileLengthProvider(UTIL.getAsyncConnection(), rs.getServerName());

    // Check that RegionServer and ReplicationServer can get same result whether the wal is being
    // written
    boolean foundWalIsBeingWritten = false;
    List<Path> wals = getRsWalsOnFs(rs);
    assertTrue(wals.size() > 1);
    for (Path wal : wals) {
      Path path = new Path(rs.getWALRootDir(), wal);
      OptionalLong rsWalLength = rsLengthProvider.getLogFileSizeIfBeingWritten(path);
      OptionalLong remoteLength = remoteLengthProvider.getLogFileSizeIfBeingWritten(path);
      assertEquals(rsWalLength.isPresent(), remoteLength.isPresent());
      if (rsWalLength.isPresent() && remoteLength.isPresent()) {
        foundWalIsBeingWritten = true;
        assertEquals(rsWalLength.getAsLong(), remoteLength.getAsLong());
      }
    }
    assertTrue(foundWalIsBeingWritten);
  }

  private List<Path> getRsWalsOnFs(HRegionServer rs) throws IOException {
    FileSystem fs = rs.getFileSystem();
    FileStatus[] fileStatuses = fs.listStatus(new Path(rs.getWALRootDir(),
      AbstractFSWALProvider.getWALDirectoryName(rs.getServerName().toString())));
    return Arrays.stream(fileStatuses).map(FileStatus::getPath).collect(Collectors.toList());
  }
}
