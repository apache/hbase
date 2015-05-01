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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.google.protobuf.ByteString;

@Category({SecurityTests.class, MediumTests.class})
public class TestVisibilityLabelsWithACL {

  private static final String PRIVATE = "private";
  private static final String CONFIDENTIAL = "confidential";
  private static final String SECRET = "secret";
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] row1 = Bytes.toBytes("row1");
  private final static byte[] fam = Bytes.toBytes("info");
  private final static byte[] qual = Bytes.toBytes("qual");
  private final static byte[] value = Bytes.toBytes("value");
  private static Configuration conf;

  @Rule
  public final TestName TEST_NAME = new TestName();
  private static User SUPERUSER;
  private static User NORMAL_USER1;
  private static User NORMAL_USER2;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);
    conf.set("hbase.coprocessor.master.classes", AccessController.class.getName() + ","
        + VisibilityController.class.getName());
    conf.set("hbase.coprocessor.region.classes", AccessController.class.getName() + ","
        + VisibilityController.class.getName());
    TEST_UTIL.startMiniCluster(2);

    TEST_UTIL.waitTableEnabled(AccessControlLists.ACL_TABLE_NAME.getName(), 50000);
    // Wait for the labels table to become available
    TEST_UTIL.waitTableEnabled(LABELS_TABLE_NAME.getName(), 50000);
    addLabels();

    // Create users for testing
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    NORMAL_USER1 = User.createUserForTesting(conf, "user1", new String[] {});
    NORMAL_USER2 = User.createUserForTesting(conf, "user2", new String[] {});
    // Grant users EXEC privilege on the labels table. For the purposes of this
    // test, we want to insure that access is denied even with the ability to access
    // the endpoint.
    SecureTestUtil.grantOnTable(TEST_UTIL, NORMAL_USER1.getShortName(), LABELS_TABLE_NAME,
      null, null, Permission.Action.EXEC);
    SecureTestUtil.grantOnTable(TEST_UTIL, NORMAL_USER2.getShortName(), LABELS_TABLE_NAME,
      null, null, Permission.Action.EXEC);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testScanForUserWithFewerLabelAuthsThanLabelsInScanAuthorizations() throws Throwable {
    String[] auths = { SECRET };
    String user = "user2";
    VisibilityClient.setAuths(TEST_UTIL.getConnection(), auths, user);
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    final Table table = createTableAndWriteDataWithLabels(tableName, SECRET + "&" + CONFIDENTIAL
        + "&!" + PRIVATE, SECRET + "&!" + PRIVATE);
    SecureTestUtil.grantOnTable(TEST_UTIL, NORMAL_USER2.getShortName(), tableName,
      null, null, Permission.Action.READ);
    PrivilegedExceptionAction<Void> scanAction = new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        Scan s = new Scan();
        s.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL));
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table t = connection.getTable(table.getName())) {
          ResultScanner scanner = t.getScanner(s);
          Result result = scanner.next();
          assertTrue(!result.isEmpty());
          assertTrue(Bytes.equals(Bytes.toBytes("row2"), result.getRow()));
          result = scanner.next();
          assertNull(result);
        }
        return null;
      }
    };
    NORMAL_USER2.runAs(scanAction);
  }

  @Test
  public void testScanForSuperUserWithFewerLabelAuths() throws Throwable {
    String[] auths = { SECRET };
    String user = "admin";
    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      VisibilityClient.setAuths(conn, auths, user);
    }
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    final Table table = createTableAndWriteDataWithLabels(tableName, SECRET + "&" + CONFIDENTIAL
        + "&!" + PRIVATE, SECRET + "&!" + PRIVATE);
    PrivilegedExceptionAction<Void> scanAction = new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        Scan s = new Scan();
        s.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL));
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table t = connection.getTable(table.getName())) {
          ResultScanner scanner = t.getScanner(s);
          Result[] result = scanner.next(5);
          assertTrue(result.length == 2);
        }
        return null;
      }
    };
    SUPERUSER.runAs(scanAction);
  }

  @Test
  public void testGetForSuperUserWithFewerLabelAuths() throws Throwable {
    String[] auths = { SECRET };
    String user = "admin";
    VisibilityClient.setAuths(TEST_UTIL.getConnection(), auths, user);
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    final Table table = createTableAndWriteDataWithLabels(tableName, SECRET + "&" + CONFIDENTIAL
        + "&!" + PRIVATE, SECRET + "&!" + PRIVATE);
    PrivilegedExceptionAction<Void> scanAction = new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        Get g = new Get(row1);
        g.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL));
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table t = connection.getTable(table.getName())) {
          Result result = t.get(g);
          assertTrue(!result.isEmpty());
        }
        return null;
      }
    };
    SUPERUSER.runAs(scanAction);
  }

  @Test
  public void testVisibilityLabelsForUserWithNoAuths() throws Throwable {
    String user = "admin";
    String[] auths = { SECRET };
    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      VisibilityClient.clearAuths(conn, auths, user); // Removing all auths if any.
      VisibilityClient.setAuths(conn, auths, "user1");
    }
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    final Table table = createTableAndWriteDataWithLabels(tableName, SECRET);
    SecureTestUtil.grantOnTable(TEST_UTIL, NORMAL_USER1.getShortName(), tableName,
      null, null, Permission.Action.READ);
    SecureTestUtil.grantOnTable(TEST_UTIL, NORMAL_USER2.getShortName(), tableName,
      null, null, Permission.Action.READ);
    PrivilegedExceptionAction<Void> getAction = new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        Get g = new Get(row1);
        g.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL));
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table t = connection.getTable(table.getName())) {
          Result result = t.get(g);
          assertTrue(result.isEmpty());
        }
        return null;
      }
    };
    NORMAL_USER2.runAs(getAction);
  }

  @Test
  public void testLabelsTableOpsWithDifferentUsers() throws Throwable {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action = 
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          return VisibilityClient.addLabels(conn, new String[] { "l1", "l2" });
        } catch (Throwable e) {
        }
        return null;
      }
    };
    VisibilityLabelsResponse response = NORMAL_USER1.runAs(action);
    assertEquals("org.apache.hadoop.hbase.security.AccessDeniedException", response
        .getResult(0).getException().getName());
    assertEquals("org.apache.hadoop.hbase.security.AccessDeniedException", response
        .getResult(1).getException().getName());

    action = new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          return VisibilityClient.setAuths(conn, new String[] { CONFIDENTIAL, PRIVATE }, "user1");
        } catch (Throwable e) {
        }
        return null;
      }
    };
    response = NORMAL_USER1.runAs(action);
    assertEquals("org.apache.hadoop.hbase.security.AccessDeniedException", response
        .getResult(0).getException().getName());
    assertEquals("org.apache.hadoop.hbase.security.AccessDeniedException", response
        .getResult(1).getException().getName());

    action = new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          return VisibilityClient.setAuths(conn, new String[] { CONFIDENTIAL, PRIVATE }, "user1");
        } catch (Throwable e) {
        }
        return null;
      }
    };
    response = SUPERUSER.runAs(action);
    assertTrue(response.getResult(0).getException().getValue().isEmpty());
    assertTrue(response.getResult(1).getException().getValue().isEmpty());

    action = new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          return VisibilityClient.clearAuths(conn, new String[] {
              CONFIDENTIAL, PRIVATE }, "user1");
        } catch (Throwable e) {
        }
        return null;
      }
    };
    response = NORMAL_USER1.runAs(action);
    assertEquals("org.apache.hadoop.hbase.security.AccessDeniedException", response.getResult(0)
        .getException().getName());
    assertEquals("org.apache.hadoop.hbase.security.AccessDeniedException", response.getResult(1)
        .getException().getName());

    response = VisibilityClient.clearAuths(TEST_UTIL.getConnection(), new String[] { CONFIDENTIAL,
      PRIVATE }, "user1");
    assertTrue(response.getResult(0).getException().getValue().isEmpty());
    assertTrue(response.getResult(1).getException().getValue().isEmpty());

    VisibilityClient.setAuths(TEST_UTIL.getConnection(), new String[] { CONFIDENTIAL, PRIVATE },
      "user3");
    PrivilegedExceptionAction<GetAuthsResponse> action1 = 
        new PrivilegedExceptionAction<GetAuthsResponse>() {
      public GetAuthsResponse run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          return VisibilityClient.getAuths(conn, "user3");
        } catch (Throwable e) {
        }
        return null;
      }
    };
    GetAuthsResponse authsResponse = NORMAL_USER1.runAs(action1);
    assertNull(authsResponse);
    authsResponse = SUPERUSER.runAs(action1);
    List<String> authsList = new ArrayList<String>();
    for (ByteString authBS : authsResponse.getAuthList()) {
      authsList.add(Bytes.toString(authBS.toByteArray()));
    }
    assertEquals(2, authsList.size());
    assertTrue(authsList.contains(CONFIDENTIAL));
    assertTrue(authsList.contains(PRIVATE));
  }

  private static Table createTableAndWriteDataWithLabels(TableName tableName, String... labelExps)
      throws Exception {
    Table table = null;
    try {
      table = TEST_UTIL.createTable(tableName, fam);
      int i = 1;
      List<Put> puts = new ArrayList<Put>();
      for (String labelExp : labelExps) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.add(fam, qual, HConstants.LATEST_TIMESTAMP, value);
        put.setCellVisibility(new CellVisibility(labelExp));
        puts.add(put);
        i++;
      }
      table.put(puts);
    } finally {
      if (table != null) {
        table.close();
      }
    }
    return table;
  }

  private static void addLabels() throws IOException {
    String[] labels = { SECRET, CONFIDENTIAL, PRIVATE };
    try {
      VisibilityClient.addLabels(TEST_UTIL.getConnection(), labels);
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }
}
