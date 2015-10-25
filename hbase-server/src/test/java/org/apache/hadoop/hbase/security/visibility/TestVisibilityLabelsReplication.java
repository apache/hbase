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
package org.apache.hadoop.hbase.security.visibility;

import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_NAME;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagRewriteCell;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.codec.KeyValueCodecWithTags;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.visibility.VisibilityController.VisibilityReplication;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ SecurityTests.class, MediumTests.class })
public class TestVisibilityLabelsReplication {
  private static final Log LOG = LogFactory.getLog(TestVisibilityLabelsReplication.class);
  protected static final int NON_VIS_TAG_TYPE = 100;
  protected static final String TEMP = "temp";
  protected static Configuration conf;
  protected static Configuration conf1;
  protected static TableName TABLE_NAME = TableName.valueOf("TABLE_NAME");
  protected static ReplicationAdmin replicationAdmin;
  public static final String TOPSECRET = "topsecret";
  public static final String PUBLIC = "public";
  public static final String PRIVATE = "private";
  public static final String CONFIDENTIAL = "confidential";
  public static final String COPYRIGHT = "\u00A9ABC";
  public static final String ACCENT = "\u0941";
  public static final String SECRET = "secret";
  public static final String UNICODE_VIS_TAG = COPYRIGHT + "\"" + ACCENT + "\\" + SECRET + "\""
      + "\u0027&\\";
  public static HBaseTestingUtility TEST_UTIL;
  public static HBaseTestingUtility TEST_UTIL1;
  public static final byte[] row1 = Bytes.toBytes("row1");
  public static final byte[] row2 = Bytes.toBytes("row2");
  public static final byte[] row3 = Bytes.toBytes("row3");
  public static final byte[] row4 = Bytes.toBytes("row4");
  public final static byte[] fam = Bytes.toBytes("info");
  public final static byte[] qual = Bytes.toBytes("qual");
  public final static byte[] value = Bytes.toBytes("value");
  protected static ZooKeeperWatcher zkw1;
  protected static ZooKeeperWatcher zkw2;
  protected static int expected[] = { 4, 6, 4, 0, 3 };
  private static final String NON_VISIBILITY = "non-visibility";
  protected static String[] expectedVisString = {
      "(\"secret\"&\"topsecret\"&\"public\")|(\"topsecret\"&\"confidential\")",
      "(\"public\"&\"private\")|(\"topsecret\"&\"private\")|"
          + "(\"confidential\"&\"public\")|(\"topsecret\"&\"confidential\")",
      "(!\"topsecret\"&\"secret\")|(!\"topsecret\"&\"confidential\")",
      "(\"secret\"&\"" + COPYRIGHT + "\\\"" + ACCENT + "\\\\" + SECRET + "\\\"" + "\u0027&\\\\"
          + "\")" };

  @Rule
  public final TestName TEST_NAME = new TestName();
  public static User SUPERUSER, USER1;

  @Before
  public void setup() throws Exception {
    // setup configuration
    conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false);
    conf.setBoolean("hbase.online.schema.update.enable", true);
    conf.setInt("hfile.format.version", 3);
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf.setInt("replication.source.size.capacity", 10240);
    conf.setLong("replication.source.sleepforretries", 100);
    conf.setInt("hbase.regionserver.maxlogs", 10);
    conf.setLong("hbase.master.logcleaner.ttl", 10);
    conf.setInt("zookeeper.recovery.retry", 1);
    conf.setInt("zookeeper.recovery.retry.intervalmill", 10);
    conf.setBoolean("dfs.support.append", true);
    conf.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf.setInt("replication.stats.thread.period.seconds", 5);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    setVisibilityLabelServiceImpl(conf);
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
    conf.setStrings(HConstants.REPLICATION_CODEC_CONF_KEY, KeyValueCodecWithTags.class.getName());
    VisibilityTestUtil.enableVisiblityLabels(conf);
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
        VisibilityReplication.class.getName());
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        SimpleCP.class.getName());
    // Have to reset conf1 in case zk cluster location different
    // than default
    conf.setClass(VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS, SimpleScanLabelGenerator.class,
        ScanLabelGenerator.class);
    conf.set("hbase.superuser", User.getCurrent().getShortName());
    SUPERUSER = User.createUserForTesting(conf, User.getCurrent().getShortName(),
        new String[] { "supergroup" });
    // User.createUserForTesting(conf, User.getCurrent().getShortName(), new
    // String[] { "supergroup" });
    USER1 = User.createUserForTesting(conf, "user1", new String[] {});
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = TEST_UTIL.getZkCluster();
    zkw1 = new ZooKeeperWatcher(conf, "cluster1", null, true);
    replicationAdmin = new ReplicationAdmin(conf);

    // Base conf2 on conf1 so it gets the right zk cluster.
    conf1 = HBaseConfiguration.create(conf);
    conf1.setInt("hfile.format.version", 3);
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    conf1.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf1.setBoolean("dfs.support.append", true);
    conf1.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
    conf1.setStrings(HConstants.REPLICATION_CODEC_CONF_KEY, KeyValueCodecWithTags.class.getName());
    conf1.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        TestCoprocessorForTagsAtSink.class.getName());
    // setVisibilityLabelServiceImpl(conf1);
    USER1 = User.createUserForTesting(conf1, "user1", new String[] {});
    TEST_UTIL1 = new HBaseTestingUtility(conf1);
    TEST_UTIL1.setZkCluster(miniZK);
    zkw2 = new ZooKeeperWatcher(conf1, "cluster2", null, true);
    replicationAdmin.addPeer("2", TEST_UTIL1.getClusterKey());

    TEST_UTIL.startMiniCluster(1);
    // Wait for the labels table to become available
    TEST_UTIL.waitTableEnabled(LABELS_TABLE_NAME.getName(), 50000);
    TEST_UTIL1.startMiniCluster(1);
    HBaseAdmin hBaseAdmin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor table = new HTableDescriptor(TABLE_NAME);
    HColumnDescriptor desc = new HColumnDescriptor(fam);
    desc.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(desc);
    try {
      hBaseAdmin.createTable(table);
    } finally {
      if (hBaseAdmin != null) {
        hBaseAdmin.close();
      }
    }
    HBaseAdmin hBaseAdmin1 = TEST_UTIL1.getHBaseAdmin();
    try {
      hBaseAdmin1.createTable(table);
    } finally {
      if (hBaseAdmin1 != null) {
        hBaseAdmin1.close();
      }
    }
    addLabels();
    setAuths(conf);
    setAuths(conf1);
  }

  protected static void setVisibilityLabelServiceImpl(Configuration conf) {
    conf.setClass(VisibilityLabelServiceManager.VISIBILITY_LABEL_SERVICE_CLASS,
        DefaultVisibilityLabelServiceImpl.class, VisibilityLabelService.class);
  }

  @Test
  public void testVisibilityReplication() throws Exception {
    int retry = 0;
    try (Table table = writeData(TABLE_NAME, "(" + SECRET + "&" + PUBLIC + ")" + "|(" + CONFIDENTIAL
            + ")&(" + TOPSECRET + ")", "(" + PRIVATE + "|" + CONFIDENTIAL + ")&(" + PUBLIC + "|"
            + TOPSECRET + ")", "(" + SECRET + "|" + CONFIDENTIAL + ")" + "&" + "!" + TOPSECRET,
        CellVisibility.quote(UNICODE_VIS_TAG) + "&" + SECRET);) {
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL, PRIVATE, TOPSECRET,
          UNICODE_VIS_TAG));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(4);

      assertTrue(next.length == 4);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row1, 0, row1.length));
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
      cellScanner = next[2].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row3, 0, row3.length));
      cellScanner = next[3].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row4, 0, row4.length));
      try (Table table2 = TEST_UTIL1.getConnection().getTable(TABLE_NAME);) {
        s = new Scan();
        // Ensure both rows are replicated
        scanner = table2.getScanner(s);
        next = scanner.next(4);
        while (next.length == 0 && retry <= 10) {
          scanner = table2.getScanner(s);
          next = scanner.next(4);
          Thread.sleep(2000);
          retry++;
        }
        assertTrue(next.length == 4);
        verifyGet(row1, expectedVisString[0], expected[0], false, TOPSECRET, CONFIDENTIAL);
        TestCoprocessorForTagsAtSink.tags.clear();
        verifyGet(row2, expectedVisString[1], expected[1], false, CONFIDENTIAL, PUBLIC);
        TestCoprocessorForTagsAtSink.tags.clear();
        verifyGet(row3, expectedVisString[2], expected[2], false, PRIVATE, SECRET);
        verifyGet(row3, "", expected[3], true, TOPSECRET, SECRET);
        verifyGet(row4, expectedVisString[3], expected[4], false, UNICODE_VIS_TAG, SECRET);
      }
    }
  }

  protected static void doAssert(byte[] row, String visTag) throws Exception {
    if (VisibilityReplicationEndPointForTest.lastEntries == null) {
      return; // first call
    }
    Assert.assertEquals(1, VisibilityReplicationEndPointForTest.lastEntries.size());
    List<Cell> cells = VisibilityReplicationEndPointForTest.lastEntries.get(0).getEdit().getCells();
    Assert.assertEquals(4, cells.size());
    boolean tagFound = false;
    for (Cell cell : cells) {
      if ((Bytes.equals(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), row, 0,
          row.length))) {
        List<Tag> tags = Tag
            .asList(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
        for (Tag tag : tags) {
          if (tag.getType() == TagType.STRING_VIS_TAG_TYPE) {
            assertEquals(visTag, Bytes.toString(tag.getValue()));
            tagFound = true;
            break;
          }
        }
      }
    }
    assertTrue(tagFound);
  }

  protected void verifyGet(final byte[] row, final String visString, final int expected,
      final boolean nullExpected, final String... auths) throws IOException,
      InterruptedException {
    PrivilegedExceptionAction<Void> scanAction = new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf1);
             Table table2 = connection.getTable(TABLE_NAME)) {
          CellScanner cellScanner;
          Cell current;
          Get get = new Get(row);
          get.setAuthorizations(new Authorizations(auths));
          Result result = table2.get(get);
          cellScanner = result.cellScanner();
          boolean advance = cellScanner.advance();
          if (nullExpected) {
            assertTrue(!advance);
            return null;
          }
          current = cellScanner.current();
          assertArrayEquals(CellUtil.cloneRow(current), row);
          for (Tag tag : TestCoprocessorForTagsAtSink.tags) {
            LOG.info("The tag type is " + tag.getType());
          }
          assertEquals(expected, TestCoprocessorForTagsAtSink.tags.size());
          Tag tag = TestCoprocessorForTagsAtSink.tags.get(1);
          if (tag.getType() != NON_VIS_TAG_TYPE) {
            assertEquals(TagType.VISIBILITY_EXP_SERIALIZATION_FORMAT_TAG_TYPE, tag.getType());
          }
          tag = TestCoprocessorForTagsAtSink.tags.get(0);
          boolean foundNonVisTag = false;
          for (Tag t : TestCoprocessorForTagsAtSink.tags) {
            if (t.getType() == NON_VIS_TAG_TYPE) {
              assertEquals(TEMP, Bytes.toString(t.getValue()));
              foundNonVisTag = true;
              break;
            }
          }
          doAssert(row, visString);
          assertTrue(foundNonVisTag);
          return null;
        }
      }
    };
    USER1.runAs(scanAction);
  }

  public static void addLabels() throws Exception {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        String[] labels = { SECRET, TOPSECRET, CONFIDENTIAL, PUBLIC, PRIVATE, UNICODE_VIS_TAG };
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          VisibilityClient.addLabels(conn, labels);
        } catch (Throwable t) {
          throw new IOException(t);
        }
        return null;
      }
    };
    SUPERUSER.runAs(action);
  }

  public static void setAuths(final Configuration conf) throws Exception {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          return VisibilityClient.setAuths(conn, new String[] { SECRET,
            CONFIDENTIAL, PRIVATE, TOPSECRET, UNICODE_VIS_TAG }, "user1");
        } catch (Throwable e) {
          throw new Exception(e);
        }
      }
    };
    VisibilityLabelsResponse response = SUPERUSER.runAs(action);
  }

  static Table writeData(TableName tableName, String... labelExps) throws Exception {
    Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME);
    int i = 1;
    List<Put> puts = new ArrayList<Put>();
    for (String labelExp : labelExps) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value);
      put.setCellVisibility(new CellVisibility(labelExp));
      put.setAttribute(NON_VISIBILITY, Bytes.toBytes(TEMP));
      puts.add(put);
      i++;
    }
    table.put(puts);
    return table;
  }
  // A simple BaseRegionbserver impl that allows to add a non-visibility tag from the
  // attributes of the Put mutation.  The existing cells in the put mutation is overwritten
  // with a new cell that has the visibility tags and the non visibility tag
  public static class SimpleCP extends BaseRegionObserver {
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put m, WALEdit edit,
        Durability durability) throws IOException {
      byte[] attribute = m.getAttribute(NON_VISIBILITY);
      byte[] cf = null;
      List<Cell> updatedCells = new ArrayList<Cell>();
      if (attribute != null) {
        for (List<? extends Cell> edits : m.getFamilyCellMap().values()) {
          for (Cell cell : edits) {
            KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
            if (cf == null) {
              cf = CellUtil.cloneFamily(kv);
            }
            Tag tag = new Tag((byte) NON_VIS_TAG_TYPE, attribute);
            List<Tag> tagList = new ArrayList<Tag>();
            tagList.add(tag);
            tagList.addAll(kv.getTags());
            byte[] fromList = Tag.fromList(tagList);
            TagRewriteCell newcell = new TagRewriteCell(kv, fromList);
            ((List<Cell>) updatedCells).add(newcell);
          }
        }
        m.getFamilyCellMap().remove(cf);
        // Update the family map
        m.getFamilyCellMap().put(cf, updatedCells);
      }
    }
  }

  public static class TestCoprocessorForTagsAtSink extends BaseRegionObserver {
    public static List<Tag> tags = null;

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
        List<Cell> results) throws IOException {
      if (results.size() > 0) {
        // Check tag presence in the 1st cell in 1st Result
        if (!results.isEmpty()) {
          Cell cell = results.get(0);
          tags = Tag.asList(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
        }
      }
    }
  }

  /**
   * An extn of VisibilityReplicationEndpoint to verify the tags that are replicated
   */
  public static class VisibilityReplicationEndPointForTest extends VisibilityReplicationEndpoint {
    static AtomicInteger replicateCount = new AtomicInteger();
    static volatile List<Entry> lastEntries = null;

    public VisibilityReplicationEndPointForTest(ReplicationEndpoint endpoint,
        VisibilityLabelService visibilityLabelsService) {
      super(endpoint, visibilityLabelsService);
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      boolean ret = super.replicate(replicateContext);
      lastEntries = replicateContext.getEntries();
      replicateCount.incrementAndGet();
      return ret;
    }
  }
}
