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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

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
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({SecurityTests.class, MediumTests.class})
public class TestVisibilityLabelsWithSLGStack {

  public static final String CONFIDENTIAL = "confidential";
  private static final String SECRET = "secret";
  public static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] ROW_1 = Bytes.toBytes("row1");
  private final static byte[] CF = Bytes.toBytes("f");
  private final static byte[] Q1 = Bytes.toBytes("q1");
  private final static byte[] Q2 = Bytes.toBytes("q2");
  private final static byte[] value = Bytes.toBytes("value");
  public static Configuration conf;

  @Rule
  public final TestName TEST_NAME = new TestName();
  public static User SUPERUSER;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    VisibilityTestUtil.enableVisiblityLabels(conf);
    String classes = SimpleScanLabelGenerator.class.getCanonicalName() + " , "
        + LabelFilteringScanLabelGenerator.class.getCanonicalName();
    conf.setStrings(VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS, classes);
    conf.set("hbase.superuser", "admin");
    TEST_UTIL.startMiniCluster(1);
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });

    // Wait for the labels table to become available
    TEST_UTIL.waitTableEnabled(LABELS_TABLE_NAME.getName(), 50000);
    addLabels();
  }

  @Test
  public void testWithSAGStack() throws Exception {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = TEST_UTIL.createTable(tableName, CF)) {
      Put put = new Put(ROW_1);
      put.addColumn(CF, Q1, HConstants.LATEST_TIMESTAMP, value);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);
      put = new Put(ROW_1);
      put.addColumn(CF, Q2, HConstants.LATEST_TIMESTAMP, value);
      put.setCellVisibility(new CellVisibility(CONFIDENTIAL));
      table.put(put);

      LabelFilteringScanLabelGenerator.labelToFilter = CONFIDENTIAL;
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL));
      ResultScanner scanner = table.getScanner(s);
      Result next = scanner.next();
      assertNotNull(next.getColumnLatestCell(CF, Q1));
      assertNull(next.getColumnLatestCell(CF, Q2));
    }
  }

  private static void addLabels() throws Exception {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action = 
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        String[] labels = { SECRET, CONFIDENTIAL };
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

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
}
