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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestSerialReplication {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSerialReplication.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static String PEER_ID = "1";

  private static byte[] CF = Bytes.toBytes("CF");

  private static byte[] CQ = Bytes.toBytes("CQ");

  private static FileSystem FS;

  private static Path LOG_DIR;

  private static WALProvider.Writer WRITER;

  public static final class LocalReplicationEndpoint extends BaseReplicationEndpoint {

    private static final UUID PEER_UUID = UUID.randomUUID();

    @Override
    public UUID getPeerUUID() {
      return PEER_UUID;
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      synchronized (WRITER) {
        try {
          for (Entry entry : replicateContext.getEntries()) {
            WRITER.append(entry);
          }
          WRITER.sync();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      return true;
    }

    @Override
    public void start() {
      startAsync();
    }

    @Override
    public void stop() {
      stopAsync();
    }

    @Override
    protected void doStart() {
      notifyStarted();
    }

    @Override
    protected void doStop() {
      notifyStopped();
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().setInt("replication.source.nb.capacity", 10);
    UTIL.startMiniCluster(3);
    // disable balancer
    UTIL.getAdmin().balancerSwitch(false, true);
    LOG_DIR = UTIL.getDataTestDirOnTestFS("replicated");
    FS = UTIL.getTestFileSystem();
    FS.mkdirs(LOG_DIR);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Rule
  public final TestName name = new TestName();

  private Path logPath;

  @Before
  public void setUp() throws IOException, StreamLacksCapabilityException {
    logPath = new Path(LOG_DIR, name.getMethodName());
    WRITER = WALFactory.createWALWriter(FS, logPath, UTIL.getConfiguration());
    // add in disable state, so later when enabling it all sources will start push together.
    UTIL.getAdmin().addReplicationPeer(PEER_ID,
      ReplicationPeerConfig.newBuilder().setClusterKey("127.0.0.1:2181:/hbase")
        .setReplicationEndpointImpl(LocalReplicationEndpoint.class.getName()).build(),
      false);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.getAdmin().removeReplicationPeer(PEER_ID);
    for (RegionServerThread t : UTIL.getMiniHBaseCluster().getLiveRegionServerThreads()) {
      t.getRegionServer().getWalRoller().requestRollAll();
    }
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
          .map(t -> t.getRegionServer()).allMatch(HRegionServer::walRollRequestFinished);
      }

      @Override
      public String explainFailure() throws Exception {
        return "Log roll has not finished yet";
      }
    });
    for (RegionServerThread t : UTIL.getMiniHBaseCluster().getLiveRegionServerThreads()) {
      t.getRegionServer().getWalRoller().requestRollAll();
    }
    if (WRITER != null) {
      WRITER.close();
      WRITER = null;
    }
  }

  private void moveRegion(RegionInfo region, HRegionServer rs) throws Exception {
    UTIL.getAdmin().move(region.getEncodedNameAsBytes(),
      Bytes.toBytes(rs.getServerName().getServerName()));
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return rs.getRegion(region.getEncodedName()) != null;
      }

      @Override
      public String explainFailure() throws Exception {
        return region + " is still not on " + rs;
      }
    });
  }

  private void enablePeerAndWaitUntilReplicationDone(int expectedEntries) throws Exception {
    UTIL.getAdmin().enableReplicationPeer(PEER_ID);
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        try (WAL.Reader reader = WALFactory.createReader(FS, logPath, UTIL.getConfiguration())) {
          int count = 0;
          while (reader.next() != null) {
            count++;
          }
          return count >= expectedEntries;
        } catch (IOException e) {
          return false;
        }
      }

      @Override
      public String explainFailure() throws Exception {
        return "Not enough entries replicated";
      }
    });
  }

  @Test
  public void testRegionMove() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.getAdmin().createTable(
      TableDescriptorBuilder.newBuilder(tableName).addColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(CF).setScope(HConstants.REPLICATION_SCOPE_SERIAL).build()).build());
    UTIL.waitTableAvailable(tableName);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    RegionInfo region = UTIL.getAdmin().getRegions(tableName).get(0);
    HRegionServer rs = UTIL.getOtherRegionServer(UTIL.getRSForFirstRegionInTable(tableName));
    moveRegion(region, rs);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 100; i < 200; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    enablePeerAndWaitUntilReplicationDone(200);
    try (WAL.Reader reader =
      WALFactory.createReader(UTIL.getTestFileSystem(), logPath, UTIL.getConfiguration())) {
      long seqId = -1L;
      int count = 0;
      for (Entry entry;;) {
        entry = reader.next();
        if (entry == null) {
          break;
        }
        assertTrue(
          "Sequence id go backwards from " + seqId + " to " + entry.getKey().getSequenceId(),
          entry.getKey().getSequenceId() >= seqId);
        count++;
      }
      assertEquals(200, count);
    }
  }

  @Test
  public void testRegionSplit() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.getAdmin().createTable(
      TableDescriptorBuilder.newBuilder(tableName).addColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(CF).setScope(HConstants.REPLICATION_SCOPE_SERIAL).build()).build());
    UTIL.waitTableAvailable(tableName);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    UTIL.flush(tableName);
    RegionInfo region = UTIL.getAdmin().getRegions(tableName).get(0);
    UTIL.getAdmin().splitRegionAsync(region.getEncodedNameAsBytes(), Bytes.toBytes(50)).get(30,
      TimeUnit.SECONDS);
    UTIL.waitUntilNoRegionsInTransition(30000);
    List<RegionInfo> regions = UTIL.getAdmin().getRegions(tableName);
    assertEquals(2, regions.size());
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    enablePeerAndWaitUntilReplicationDone(200);
    Map<String, Long> regionsToSeqId = new HashMap<>();
    regionsToSeqId.put(region.getEncodedName(), -1L);
    regions.stream().map(RegionInfo::getEncodedName).forEach(n -> regionsToSeqId.put(n, -1L));
    try (WAL.Reader reader =
      WALFactory.createReader(UTIL.getTestFileSystem(), logPath, UTIL.getConfiguration())) {
      int count = 0;
      for (Entry entry;;) {
        entry = reader.next();
        if (entry == null) {
          break;
        }
        String encodedName = Bytes.toString(entry.getKey().getEncodedRegionName());
        Long seqId = regionsToSeqId.get(encodedName);
        assertNotNull(
          "Unexcepted entry " + entry + ", expected regions " + region + ", or " + regions, seqId);
        assertTrue("Sequence id go backwards from " + seqId + " to " +
          entry.getKey().getSequenceId() + " for " + encodedName,
          entry.getKey().getSequenceId() >= seqId.longValue());
        if (count < 100) {
          assertEquals(encodedName + " is pushed before parent " + region.getEncodedName(),
            region.getEncodedName(), encodedName);
        } else {
          assertNotEquals(region.getEncodedName(), encodedName);
        }
        count++;
      }
      assertEquals(200, count);
    }
  }

  @Test
  public void testRegionMerge() throws Exception {
    byte[] splitKey = Bytes.toBytes(50);
    TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.getAdmin().createTable(
      TableDescriptorBuilder.newBuilder(tableName)
        .addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF)
          .setScope(HConstants.REPLICATION_SCOPE_SERIAL).build())
        .build(),
      new byte[][] { splitKey });
    UTIL.waitTableAvailable(tableName);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    List<RegionInfo> regions = UTIL.getAdmin().getRegions(tableName);
    UTIL.getAdmin()
      .mergeRegionsAsync(
        regions.stream().map(RegionInfo::getEncodedNameAsBytes).toArray(byte[][]::new), false)
      .get(30, TimeUnit.SECONDS);
    UTIL.waitUntilNoRegionsInTransition(30000);
    List<RegionInfo> regionsAfterMerge = UTIL.getAdmin().getRegions(tableName);
    assertEquals(1, regionsAfterMerge.size());
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    enablePeerAndWaitUntilReplicationDone(200);
    Map<String, Long> regionsToSeqId = new HashMap<>();
    RegionInfo region = regionsAfterMerge.get(0);
    regionsToSeqId.put(region.getEncodedName(), -1L);
    regions.stream().map(RegionInfo::getEncodedName).forEach(n -> regionsToSeqId.put(n, -1L));
    try (WAL.Reader reader =
      WALFactory.createReader(UTIL.getTestFileSystem(), logPath, UTIL.getConfiguration())) {
      int count = 0;
      for (Entry entry;;) {
        entry = reader.next();
        if (entry == null) {
          break;
        }
        String encodedName = Bytes.toString(entry.getKey().getEncodedRegionName());
        Long seqId = regionsToSeqId.get(encodedName);
        assertNotNull(
          "Unexcepted entry " + entry + ", expected regions " + region + ", or " + regions, seqId);
        assertTrue("Sequence id go backwards from " + seqId + " to " +
          entry.getKey().getSequenceId() + " for " + encodedName,
          entry.getKey().getSequenceId() >= seqId.longValue());
        if (count < 100) {
          assertNotEquals(
            encodedName + " is pushed before parents " +
              regions.stream().map(RegionInfo::getEncodedName).collect(Collectors.joining(" and ")),
            region.getEncodedName(), encodedName);
        } else {
          assertEquals(region.getEncodedName(), encodedName);
        }
        count++;
      }
      assertEquals(200, count);
    }
  }
}
