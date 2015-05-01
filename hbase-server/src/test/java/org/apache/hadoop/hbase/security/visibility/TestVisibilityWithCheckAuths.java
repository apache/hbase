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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(MediumTests.class)
/**
 * Test visibility by setting 'hbase.security.visibility.mutations.checkauths' to true
 */
public class TestVisibilityWithCheckAuths {
  private static final String TOPSECRET = "TOPSECRET";
  private static final String PUBLIC = "PUBLIC";
  public static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] row1 = Bytes.toBytes("row1");
  private final static byte[] fam = Bytes.toBytes("info");
  private final static byte[] qual = Bytes.toBytes("qual");
  private final static byte[] value = Bytes.toBytes("value");
  public static Configuration conf;

  @Rule
  public final TestName TEST_NAME = new TestName();
  public static User SUPERUSER;
  public static User USER;
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false);
    VisibilityTestUtil.enableVisiblityLabels(conf);
    conf.setBoolean(VisibilityConstants.CHECK_AUTHS_FOR_MUTATION, true);
    conf.setClass(VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS, SimpleScanLabelGenerator.class,
        ScanLabelGenerator.class);
    conf.set("hbase.superuser", "admin");
    TEST_UTIL.startMiniCluster(2);
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    USER = User.createUserForTesting(conf, "user", new String[]{});
    // Wait for the labels table to become available
    TEST_UTIL.waitTableEnabled(LABELS_TABLE_NAME.getName(), 50000);
    addLabels();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public static void addLabels() throws Exception {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      @Override
      public VisibilityLabelsResponse run() throws Exception {
        String[] labels = { TOPSECRET };
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

  @Test
  public void testVerifyAccessDeniedForInvalidUserAuths() throws Exception {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      @Override
      public VisibilityLabelsResponse run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          return VisibilityClient.setAuths(conn, new String[] { TOPSECRET },
              USER.getShortName());
        } catch (Throwable e) {
        }
        return null;
      }
    };
    SUPERUSER.runAs(action);
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Admin hBaseAdmin = TEST_UTIL.getHBaseAdmin();
    HColumnDescriptor colDesc = new HColumnDescriptor(fam);
    colDesc.setMaxVersions(5);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(colDesc);
    hBaseAdmin.createTable(desc);
    try {
      TEST_UTIL.getHBaseAdmin().flush(tableName);
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Put p = new Put(row1);
            p.setCellVisibility(new CellVisibility(PUBLIC + "&" + TOPSECRET));
            p.add(fam, qual, 125l, value);
            table.put(p);
            Assert.fail("Testcase should fail with AccesDeniedException");
          } catch (Throwable t) {
            assertTrue(t.getMessage().contains("AccessDeniedException"));
          }
          return null;
        }
      };
      USER.runAs(actiona);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Test
  public void testLabelsWithAppend() throws Throwable {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      @Override
      public VisibilityLabelsResponse run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          return VisibilityClient.setAuths(conn, new String[] { TOPSECRET },
              USER.getShortName());
        } catch (Throwable e) {
        }
        return null;
      }
    };
    SUPERUSER.runAs(action);
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = TEST_UTIL.createTable(tableName, fam)) {
      final byte[] row1 = Bytes.toBytes("row1");
      final byte[] val = Bytes.toBytes("a");
      PrivilegedExceptionAction<Void> actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Put put = new Put(row1);
            put.add(fam, qual, HConstants.LATEST_TIMESTAMP, val);
            put.setCellVisibility(new CellVisibility(TOPSECRET));
            table.put(put);
          }
          return null;
        }
      };
      USER.runAs(actiona);
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Append append = new Append(row1);
            append.add(fam, qual, Bytes.toBytes("b"));
            table.append(append);
          }
          return null;
        }
      };
      USER.runAs(actiona);
      actiona = new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try (Connection connection = ConnectionFactory.createConnection(conf);
               Table table = connection.getTable(tableName)) {
            Append append = new Append(row1);
            append.add(fam, qual, Bytes.toBytes("c"));
            append.setCellVisibility(new CellVisibility(PUBLIC));
            table.append(append);
            Assert.fail("Testcase should fail with AccesDeniedException");
          } catch (Throwable t) {
            assertTrue(t.getMessage().contains("AccessDeniedException"));
          }
          return null;
        }
      };
      USER.runAs(actiona);
    }
  }
}
