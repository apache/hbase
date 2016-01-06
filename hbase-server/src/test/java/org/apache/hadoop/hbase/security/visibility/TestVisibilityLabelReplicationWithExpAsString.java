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
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.codec.KeyValueCodecWithTags;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.visibility.VisibilityController.VisibilityReplication;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Before;
import org.junit.experimental.categories.Category;

@Category({ SecurityTests.class, MediumTests.class })
public class TestVisibilityLabelReplicationWithExpAsString extends TestVisibilityLabelsReplication {
  private static final Log LOG = LogFactory
      .getLog(TestVisibilityLabelReplicationWithExpAsString.class);

  @Override
  @Before
  public void setup() throws Exception {
    expected[0] = 4;
    expected[1] = 6;
    expected[2] = 4;
    expected[3] = 0;
    expected[3] = 3;
    expectedVisString[0] = "(\"public\"&\"secret\"&\"topsecret\")|(\"confidential\"&\"topsecret\")";
    expectedVisString[1] = "(\"private\"&\"public\")|(\"private\"&\"topsecret\")|"
        + "(\"confidential\"&\"public\")|(\"confidential\"&\"topsecret\")";
    expectedVisString[2] = "(!\"topsecret\"&\"secret\")|(!\"topsecret\"&\"confidential\")";
    expectedVisString[3] = "(\"secret\"&\"" + COPYRIGHT + "\\\"" + ACCENT + "\\\\" + SECRET
        + "\\\"" + "\u0027&\\\\" + "\")";
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
    setVisibilityLabelServiceImpl(conf, ExpAsStringVisibilityLabelServiceImpl.class);
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
    conf.set("hbase.superuser", "admin");
    conf.set("hbase.superuser", User.getCurrent().getShortName());
    SUPERUSER = User.createUserForTesting(conf, User.getCurrent().getShortName(),
        new String[] { "supergroup" });
    User.createUserForTesting(conf,
        User.getCurrent().getShortName(), new String[] { "supergroup" });
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
    setVisibilityLabelServiceImpl(conf1, ExpAsStringVisibilityLabelServiceImpl.class);
    TEST_UTIL1 = new HBaseTestingUtility(conf1);
    TEST_UTIL1.setZkCluster(miniZK);
    zkw2 = new ZooKeeperWatcher(conf1, "cluster2", null, true);
    replicationAdmin.addPeer("2", TEST_UTIL1.getClusterKey());

    TEST_UTIL.startMiniCluster(1);
    // Wait for the labels table to become available
    TEST_UTIL.waitTableEnabled(LABELS_TABLE_NAME.getName(), 50000);
    TEST_UTIL1.startMiniCluster(1);
    HTableDescriptor table = new HTableDescriptor(TABLE_NAME);
    HColumnDescriptor desc = new HColumnDescriptor(fam);
    desc.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(desc);
    try (Admin hBaseAdmin = TEST_UTIL.getHBaseAdmin()) {
      hBaseAdmin.createTable(table);
    }
    try (Admin hBaseAdmin1 = TEST_UTIL1.getHBaseAdmin()){
      hBaseAdmin1.createTable(table);
    }
    addLabels();
    setAuths(conf);
    setAuths(conf1);
  }

  protected static void setVisibilityLabelServiceImpl(Configuration conf, Class clazz) {
    conf.setClass(VisibilityLabelServiceManager.VISIBILITY_LABEL_SERVICE_CLASS,
        clazz, VisibilityLabelService.class);
  }

  @Override
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
          assertEquals(expected, TestCoprocessorForTagsAtSink.tags.size());
          boolean foundNonVisTag = false;
          for(Tag t : TestCoprocessorForTagsAtSink.tags) {
            if(t.getType() == NON_VIS_TAG_TYPE) {
              assertEquals(TEMP, Bytes.toString(TagUtil.cloneValue(t)));
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
}
