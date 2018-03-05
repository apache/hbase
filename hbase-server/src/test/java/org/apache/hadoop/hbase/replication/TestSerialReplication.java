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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;
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
    UTIL.ensureSomeRegionServersAvailable(3);
    logPath = new Path(LOG_DIR, name.getMethodName());
    WRITER = WALFactory.createWALWriter(FS, logPath, UTIL.getConfiguration());
    // add in disable state, so later when enabling it all sources will start push together.
    UTIL.getAdmin().addReplicationPeer(PEER_ID,
      ReplicationPeerConfig.newBuilder().setClusterKey("127.0.0.1:2181:/hbase")
        .setReplicationEndpointImpl(LocalReplicationEndpoint.class.getName()).build(),
      false);
  }

  @After
  public void tearDown() throws IOException {
    UTIL.getAdmin().removeReplicationPeer(PEER_ID);
    if (WRITER != null) {
      WRITER.close();
      WRITER = null;
    }
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
    UTIL.getAdmin().move(region.getEncodedNameAsBytes(),
      Bytes.toBytes(rs.getServerName().getServerName()));
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return !rs.getRegions(tableName).isEmpty();
      }

      @Override
      public String explainFailure() throws Exception {
        return region + " is still not on " + rs;
      }
    });
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 100; i < 200; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    UTIL.getAdmin().enableReplicationPeer(PEER_ID);
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        try (WAL.Reader reader = WALFactory.createReader(FS, logPath, UTIL.getConfiguration())) {
          int count = 0;
          while (reader.next() != null) {
            count++;
          }
          return count >= 200;
        } catch (IOException e) {
          return false;
        }
      }

      @Override
      public String explainFailure() throws Exception {
        return "Not enough entries replicated";
      }
    });
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
}
