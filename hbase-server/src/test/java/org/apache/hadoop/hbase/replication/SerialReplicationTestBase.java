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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Base class for testing serial replication.
 */
public class SerialReplicationTestBase {

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  protected static String PEER_ID = "1";

  protected static byte[] CF = Bytes.toBytes("CF");

  protected static byte[] CQ = Bytes.toBytes("CQ");

  protected static FileSystem FS;

  protected static Path LOG_DIR;

  protected static WALProvider.Writer WRITER;

  @Rule
  public final TestName name = new TestName();

  protected Path logPath;

  public static final class LocalReplicationEndpoint extends BaseReplicationEndpoint {

    private static final UUID PEER_UUID = UTIL.getRandomUUID();

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
    UTIL.getConfiguration().setInt("replication.source.nb.capacity", 10);
    UTIL.getConfiguration().setLong("replication.sleep.before.failover", 1000);
    UTIL.getConfiguration().setLong("hbase.serial.replication.waiting.ms", 100);
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

  @After
  public void tearDown() throws Exception {
    Admin admin = UTIL.getAdmin();
    for (ReplicationPeerDescription pd : admin.listReplicationPeers()) {
      admin.removeReplicationPeer(pd.getPeerId());
    }
    rollAllWALs();
    if (WRITER != null) {
      WRITER.close();
      WRITER = null;
    }
  }

  protected static void moveRegion(RegionInfo region, HRegionServer rs) throws Exception {
    UTIL.getAdmin().move(region.getEncodedNameAsBytes(), rs.getServerName());
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

  protected static void rollAllWALs() throws Exception {
    for (RegionServerThread t : UTIL.getMiniHBaseCluster().getLiveRegionServerThreads()) {
      t.getRegionServer().getWalRoller().requestRollAll();
    }
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return UTIL.getMiniHBaseCluster()
            .getLiveRegionServerThreads()
            .stream()
            .map(RegionServerThread::getRegionServer)
            .allMatch(HRegionServer::walRollRequestFinished);
      }

      @Override
      public String explainFailure() throws Exception {
        return "Log roll has not finished yet";
      }
    });
  }

  protected final void setupWALWriter() throws IOException {
    logPath = new Path(LOG_DIR, name.getMethodName());
    WRITER = WALFactory.createWALWriter(FS, logPath, UTIL.getConfiguration());
  }

  protected final void waitUntilReplicationDone(int expectedEntries) throws Exception {
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

  protected final void enablePeerAndWaitUntilReplicationDone(int expectedEntries) throws Exception {
    UTIL.getAdmin().enableReplicationPeer(PEER_ID);
    waitUntilReplicationDone(expectedEntries);
  }

  protected final void addPeer(boolean enabled) throws IOException {
    UTIL.getAdmin().addReplicationPeer(PEER_ID,
      ReplicationPeerConfig.newBuilder().setClusterKey("127.0.0.1:2181:/hbase")
        .setReplicationEndpointImpl(LocalReplicationEndpoint.class.getName()).setSerial(true)
        .build(),
      enabled);
  }

  protected final void checkOrder(int expectedEntries) throws IOException {
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
        seqId = entry.getKey().getSequenceId();
        count++;
      }
      assertEquals(expectedEntries, count);
    }
  }

  protected final TableName createTable() throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.getAdmin().createTable(
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(CF).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build()).build());
    UTIL.waitTableAvailable(tableName);
    return tableName;
  }
}
