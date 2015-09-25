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
import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.google.protobuf.ByteString;

@Category(LargeTests.class)
public class TestWithDisabledAuthorization {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  
  private static final String CONFIDENTIAL = "confidential";
  private static final String SECRET = "secret";
  private static final String PRIVATE = "private";
  private static final byte[] TEST_FAMILY = Bytes.toBytes("test");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("q");
  private static final byte[] ZERO = Bytes.toBytes(0L);


  @Rule 
  public final TestName TEST_NAME = new TestName();

  private static User SUPERUSER;
  private static User USER_RW;
  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    // Up the handlers; this test needs more than usual.
    conf.setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);
    // Set up superuser
    SecureTestUtil.configureSuperuser(conf);

    // Install the VisibilityController as a system processor
    VisibilityTestUtil.enableVisiblityLabels(conf);

    // Now, DISABLE active authorization
    conf.setBoolean(User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY, false);

    TEST_UTIL.startMiniCluster();

    // Wait for the labels table to become available
    TEST_UTIL.waitUntilAllRegionsAssigned(LABELS_TABLE_NAME);

    // create a set of test users
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    USER_RW = User.createUserForTesting(conf, "rwuser", new String[0]);

    // Define test labels
    SUPERUSER.runAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          VisibilityClient.addLabels(conn,
            new String[] { SECRET, CONFIDENTIAL, PRIVATE });
          VisibilityClient.setAuths(conn,
            new String[] { SECRET, CONFIDENTIAL },
            USER_RW.getShortName());
        } catch (Throwable t) {
          fail("Should not have failed");          
        }
        return null;
      }
    });
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test (timeout=180000)
  public void testManageUserAuths() throws Throwable {
    // Even though authorization is disabled, we should be able to manage user auths

    SUPERUSER.runAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          VisibilityClient.setAuths(conn,
            new String[] { SECRET, CONFIDENTIAL },
            USER_RW.getShortName());
        } catch (Throwable t) {
          fail("Should not have failed");          
        }
        return null;
      }
    });

    PrivilegedExceptionAction<List<String>> getAuths =
      new PrivilegedExceptionAction<List<String>>() {
        public List<String> run() throws Exception {
          GetAuthsResponse authsResponse = null;
          try (Connection conn = ConnectionFactory.createConnection(conf)) {
            authsResponse = VisibilityClient.getAuths(conn,
              USER_RW.getShortName());
          } catch (Throwable t) {
            fail("Should not have failed");
          }
          List<String> authsList = new ArrayList<String>();
          for (ByteString authBS : authsResponse.getAuthList()) {
            authsList.add(Bytes.toString(authBS.toByteArray()));
          }
          return authsList;
        }
      };

    List<String> authsList = SUPERUSER.runAs(getAuths);
    assertEquals(2, authsList.size());
    assertTrue(authsList.contains(SECRET));
    assertTrue(authsList.contains(CONFIDENTIAL));

    SUPERUSER.runAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          VisibilityClient.clearAuths(conn,
            new String[] { SECRET },
            USER_RW.getShortName());
        } catch (Throwable t) {
          fail("Should not have failed");          
        }
        return null;
      }
    });

    authsList = SUPERUSER.runAs(getAuths);
    assertEquals(1, authsList.size());
    assertTrue(authsList.contains(CONFIDENTIAL));

    SUPERUSER.runAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          VisibilityClient.clearAuths(conn,
            new String[] { CONFIDENTIAL },
            USER_RW.getShortName());
        } catch (Throwable t) {
          fail("Should not have failed");          
        }
        return null;
      }
    });

    authsList = SUPERUSER.runAs(getAuths);
    assertEquals(0, authsList.size());
  }

  @Test (timeout=180000)
  public void testPassiveVisibility() throws Exception {
    // No values should be filtered regardless of authorization if we are passive
    try (Table t = createTableAndWriteDataWithLabels(
      TableName.valueOf(TEST_NAME.getMethodName()),
        SECRET,
        PRIVATE,
        SECRET + "|" + CONFIDENTIAL,
        PRIVATE + "|" + CONFIDENTIAL)) {
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations());
      try (ResultScanner scanner = t.getScanner(s)) {
        Result[] next = scanner.next(10);
        assertEquals(next.length, 4);
      }
      s = new Scan();
      s.setAuthorizations(new Authorizations(SECRET));
      try (ResultScanner scanner = t.getScanner(s)) {
        Result[] next = scanner.next(10);
        assertEquals(next.length, 4);
      }
      s = new Scan();
      s.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL));
      try (ResultScanner scanner = t.getScanner(s)) {
        Result[] next = scanner.next(10);
        assertEquals(next.length, 4);
      }
      s = new Scan();
      s.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL, PRIVATE));
      try (ResultScanner scanner = t.getScanner(s)) {
        Result[] next = scanner.next(10);
        assertEquals(next.length, 4);
      }
    }
  }

  static Table createTableAndWriteDataWithLabels(TableName tableName, String... labelExps)
      throws Exception {
    List<Put> puts = new ArrayList<Put>();
    for (int i = 0; i < labelExps.length; i++) {
      Put put = new Put(Bytes.toBytes("row" + (i+1)));
      put.addColumn(TEST_FAMILY, TEST_QUALIFIER, HConstants.LATEST_TIMESTAMP, ZERO);
      put.setCellVisibility(new CellVisibility(labelExps[i]));
      puts.add(put);
    }
    Table table = TEST_UTIL.createTable(tableName, TEST_FAMILY);
    table.put(puts);
    return table;
  }
}
