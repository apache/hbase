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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-20624.
 */
@Category({ ReplicationTests.class, MediumTests.class })
public class TestRaceWhenCreatingReplicationSource {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRaceWhenCreatingReplicationSource.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static String PEER_ID = "1";

  private static TableName TABLE_NAME = TableName.valueOf("race");

  private static byte[] CF = Bytes.toBytes("CF");

  private static byte[] CQ = Bytes.toBytes("CQ");

  private static FileSystem FS;

  private static Path LOG_PATH;

  private static WALProvider.Writer WRITER;

  private static volatile boolean NULL_UUID = true;

  public static final class LocalReplicationEndpoint extends BaseReplicationEndpoint {

    private static final UUID PEER_UUID = UTIL.getRandomUUID();

    @Override
    public UUID getPeerUUID() {
      if (NULL_UUID) {
        return null;
      } else {
        return PEER_UUID;
      }
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      synchronized (WRITER) {
        try {
          for (Entry entry : replicateContext.getEntries()) {
            WRITER.append(entry);
          }
          WRITER.sync(false);
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

    @Override
    public boolean canReplicateToSameCluster() {
      return true;
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().set(WALFactory.WAL_PROVIDER, "multiwal");
    // make sure that we will create a new group for the table
    UTIL.getConfiguration().setInt("hbase.wal.regiongrouping.numgroups", 8);
    UTIL.startMiniCluster(3);
    Path dir = UTIL.getDataTestDirOnTestFS();
    FS = UTIL.getTestFileSystem();
    LOG_PATH = new Path(dir, "replicated");
    WRITER = WALFactory.createWALWriter(FS, LOG_PATH, UTIL.getConfiguration());
    UTIL.getAdmin().addReplicationPeer(PEER_ID,
      ReplicationPeerConfig.newBuilder().setClusterKey("127.0.0.1:2181:/hbase")
        .setReplicationEndpointImpl(LocalReplicationEndpoint.class.getName()).build(),
      true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRace() throws Exception {
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        for (RegionServerThread t : UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
          ReplicationSource source =
            (ReplicationSource) ((Replication) t.getRegionServer().getReplicationSourceService())
              .getReplicationManager().getSource(PEER_ID);
          if (source == null || source.getReplicationEndpoint() == null) {
            return false;
          }
        }
        return true;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Replication source has not been initialized yet";
      }
    });
    UTIL.getAdmin().createTable(
      TableDescriptorBuilder.newBuilder(TABLE_NAME).setColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(CF).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build()).build());
    UTIL.waitTableAvailable(TABLE_NAME);
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      table.put(new Put(Bytes.toBytes(1)).addColumn(CF, CQ, Bytes.toBytes(1)));
    }
    NULL_UUID = false;
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        try (WAL.Reader reader = WALFactory.createReader(FS, LOG_PATH, UTIL.getConfiguration())) {
          return reader.next() != null;
        } catch (IOException e) {
          return false;
        }
      }

      @Override
      public String explainFailure() throws Exception {
        return "Replication has not catched up";
      }
    });
    try (WAL.Reader reader = WALFactory.createReader(FS, LOG_PATH, UTIL.getConfiguration())) {
      Cell cell = reader.next().getEdit().getCells().get(0);
      assertEquals(1, Bytes.toInt(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
      assertArrayEquals(CF, CellUtil.cloneFamily(cell));
      assertArrayEquals(CQ, CellUtil.cloneQualifier(cell));
      assertEquals(1,
        Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    }
  }
}
