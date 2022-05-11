/*
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ SecurityTests.class, MediumTests.class })
public class TestEnforcingScanLabelGenerator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestEnforcingScanLabelGenerator.class);

  public static final String CONFIDENTIAL = "confidential";
  private static final String SECRET = "secret";
  public static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] ROW_1 = Bytes.toBytes("row1");
  private final static byte[] CF = Bytes.toBytes("f");
  private final static byte[] Q1 = Bytes.toBytes("q1");
  private final static byte[] Q2 = Bytes.toBytes("q2");
  private final static byte[] Q3 = Bytes.toBytes("q3");
  private final static byte[] value = Bytes.toBytes("value");
  public static Configuration conf;

  @Rule
  public final TestName TEST_NAME = new TestName();
  public static User SUPERUSER;
  public static User TESTUSER;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    VisibilityTestUtil.enableVisiblityLabels(conf);
    String classes = DefinedSetFilterScanLabelGenerator.class.getCanonicalName() + " , "
      + EnforcingScanLabelGenerator.class.getCanonicalName();
    conf.setStrings(VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS, classes);
    conf.set("hbase.superuser", "admin");
    TEST_UTIL.startMiniCluster(1);
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    TESTUSER = User.createUserForTesting(conf, "test", new String[] {});

    // Wait for the labels table to become available
    TEST_UTIL.waitTableEnabled(LABELS_TABLE_NAME.getName(), 50000);

    // Set up for the test
    SUPERUSER.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          VisibilityClient.addLabels(conn, new String[] { SECRET, CONFIDENTIAL });
          VisibilityClient.setAuths(conn, new String[] { CONFIDENTIAL, }, TESTUSER.getShortName());
        } catch (Throwable t) {
          throw new IOException(t);
        }
        return null;
      }
    });
  }

  @Test
  public void testEnforcingScanLabelGenerator() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());

    SUPERUSER.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf);
          Table table = TEST_UTIL.createTable(tableName, CF)) {
          Put put = new Put(ROW_1);
          put.addColumn(CF, Q1, HConstants.LATEST_TIMESTAMP, value);
          put.setCellVisibility(new CellVisibility(SECRET));
          table.put(put);
          put = new Put(ROW_1);
          put.addColumn(CF, Q2, HConstants.LATEST_TIMESTAMP, value);
          put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
          table.put(put);
          put = new Put(ROW_1);
          put.addColumn(CF, Q3, HConstants.LATEST_TIMESTAMP, value);
          table.put(put);
          return null;
        }
      }
    });

    // Test that super user can see all the cells.
    SUPERUSER.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf);
          Table table = connection.getTable(tableName)) {
          // Test that super user can see all the cells.
          Get get = new Get(ROW_1);
          Result result = table.get(get);
          assertTrue("Missing authorization", result.containsColumn(CF, Q1));
          assertTrue("Missing authorization", result.containsColumn(CF, Q2));
          assertTrue("Missing authorization", result.containsColumn(CF, Q3));
          return null;
        }
      }
    });

    TESTUSER.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf);
          Table table = connection.getTable(tableName)) {
          // Test that we enforce the defined set
          Get get = new Get(ROW_1);
          get.setAuthorizations(new Authorizations(new String[] { SECRET, CONFIDENTIAL }));
          Result result = table.get(get);
          assertFalse("Inappropriate authorization", result.containsColumn(CF, Q1));
          assertTrue("Missing authorization", result.containsColumn(CF, Q2));
          assertTrue("Inappropriate filtering", result.containsColumn(CF, Q3));
          // Test that we also enforce the defined set for the user if no auths are provided
          get = new Get(ROW_1);
          result = table.get(get);
          assertFalse("Inappropriate authorization", result.containsColumn(CF, Q1));
          assertTrue("Missing authorization", result.containsColumn(CF, Q2));
          assertTrue("Inappropriate filtering", result.containsColumn(CF, Q3));
          return null;
        }
      }
    });

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
}
