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
package org.apache.hadoop.hbase.security.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
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
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({SecurityTests.class, MediumTests.class})
public class TestAccessControlFilter extends SecureTestUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAccessControlFilter.class);

  @Rule public TestName name = new TestName();
  private static HBaseTestingUtility TEST_UTIL;

  private static User READER;
  private static User LIMITED;
  private static User DENIED;

  private static TableName TABLE;
  private static byte[] FAMILY = Bytes.toBytes("f1");
  private static byte[] PRIVATE_COL = Bytes.toBytes("private");
  private static byte[] PUBLIC_COL = Bytes.toBytes("public");

  @Before
  public void setup () {
    TABLE = TableName.valueOf(name.getMethodName());
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    // Up the handlers; this test needs more than usual.
    conf.setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);
    enableSecurity(conf);
    verifyConfiguration(conf);

    // We expect 0.98 scanning semantics
    conf.setBoolean(AccessControlConstants.CF_ATTRIBUTE_EARLY_OUT, false);

    TEST_UTIL.startMiniCluster();
    TEST_UTIL.waitTableEnabled(PermissionStorage.ACL_TABLE_NAME.getName(), 50000);

    READER = User.createUserForTesting(conf, "reader", new String[0]);
    LIMITED = User.createUserForTesting(conf, "limited", new String[0]);
    DENIED = User.createUserForTesting(conf, "denied", new String[0]);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testQualifierAccess() throws Exception {
    final Table table = createTable(TEST_UTIL, TABLE, new byte[][] { FAMILY });
    try {
      doQualifierAccess(table);
    } finally {
      table.close();
    }
  }

  private void doQualifierAccess(final Table table) throws Exception {
    // set permissions
    SecureTestUtil.grantOnTable(TEST_UTIL, READER.getShortName(), TABLE, null, null,
      Permission.Action.READ);
    SecureTestUtil.grantOnTable(TEST_UTIL, LIMITED.getShortName(), TABLE, FAMILY, PUBLIC_COL,
      Permission.Action.READ);

    // put some test data
    List<Put> puts = new ArrayList<>(100);
    for (int i=0; i<100; i++) {
      Put p = new Put(Bytes.toBytes(i));
      p.addColumn(FAMILY, PRIVATE_COL, Bytes.toBytes("secret " + i));
      p.addColumn(FAMILY, PUBLIC_COL, Bytes.toBytes("info " + i));
      puts.add(p);
    }
    table.put(puts);

    // test read
    READER.runAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
        // force a new RS connection
        conf.set("testkey", TEST_UTIL.getRandomUUID().toString());
        Connection connection = ConnectionFactory.createConnection(conf);
        Table t = connection.getTable(TABLE);
        try {
          ResultScanner rs = t.getScanner(new Scan());
          int rowcnt = 0;
          for (Result r : rs) {
            rowcnt++;
            int rownum = Bytes.toInt(r.getRow());
            assertTrue(r.containsColumn(FAMILY, PRIVATE_COL));
            assertEquals("secret "+rownum, Bytes.toString(r.getValue(FAMILY, PRIVATE_COL)));
            assertTrue(r.containsColumn(FAMILY, PUBLIC_COL));
            assertEquals("info "+rownum, Bytes.toString(r.getValue(FAMILY, PUBLIC_COL)));
          }
          assertEquals("Expected 100 rows returned", 100, rowcnt);
          return null;
        } finally {
          t.close();
          connection.close();
        }
      }
    });

    // test read with qualifier filter
    LIMITED.runAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
        // force a new RS connection
        conf.set("testkey", TEST_UTIL.getRandomUUID().toString());
        Connection connection = ConnectionFactory.createConnection(conf);
        Table t = connection.getTable(TABLE);
        try {
          ResultScanner rs = t.getScanner(new Scan());
          int rowcnt = 0;
          for (Result r : rs) {
            rowcnt++;
            int rownum = Bytes.toInt(r.getRow());
            assertFalse(r.containsColumn(FAMILY, PRIVATE_COL));
            assertTrue(r.containsColumn(FAMILY, PUBLIC_COL));
            assertEquals("info " + rownum, Bytes.toString(r.getValue(FAMILY, PUBLIC_COL)));
          }
          assertEquals("Expected 100 rows returned", 100, rowcnt);
          return null;
        } finally {
          t.close();
          connection.close();
        }
      }
    });

    // test as user with no permission
    DENIED.runAs(new PrivilegedExceptionAction<Object>(){
      @Override
      public Object run() throws Exception {
        Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
        // force a new RS connection
        conf.set("testkey", TEST_UTIL.getRandomUUID().toString());
        Connection connection = ConnectionFactory.createConnection(conf);
        Table t = connection.getTable(TABLE);
        try {
          ResultScanner rs = t.getScanner(new Scan());
          int rowcnt = 0;
          for (Result r : rs) {
            rowcnt++;
            int rownum = Bytes.toInt(r.getRow());
            assertFalse(r.containsColumn(FAMILY, PRIVATE_COL));
            assertTrue(r.containsColumn(FAMILY, PUBLIC_COL));
            assertEquals("info " + rownum, Bytes.toString(r.getValue(FAMILY, PUBLIC_COL)));
          }
          assertEquals("Expected 0 rows returned", 0, rowcnt);
          return null;
        } finally {
          t.close();
          connection.close();
        }
      }
    });
  }
}
