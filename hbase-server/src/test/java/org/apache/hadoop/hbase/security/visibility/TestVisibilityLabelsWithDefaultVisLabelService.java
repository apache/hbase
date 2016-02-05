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
import static org.apache.hadoop.hbase.security.visibility.VisibilityUtils.SYSTEM_LABEL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.ListLabelsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ByteString;

@Category({SecurityTests.class, MediumTests.class})
public class TestVisibilityLabelsWithDefaultVisLabelService extends TestVisibilityLabels {
  private static final Log LOG = LogFactory.getLog(
    TestVisibilityLabelsWithDefaultVisLabelService.class);

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false);
    conf.setBoolean("hbase.online.schema.update.enable", true);
    VisibilityTestUtil.enableVisiblityLabels(conf);
    conf.setClass(VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS, SimpleScanLabelGenerator.class,
        ScanLabelGenerator.class);
    conf.set("hbase.superuser", "admin");
    TEST_UTIL.startMiniCluster(2);
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    USER1 = User.createUserForTesting(conf, "user1", new String[] {});

    // Wait for the labels table to become available
    TEST_UTIL.waitTableEnabled(LABELS_TABLE_NAME.getName(), 50000);
    addLabels();
  }

  @Test
  public void testAddLabels() throws Throwable {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        String[] labels = { "L1", SECRET, "L2", "invalid~", "L3" };
        VisibilityLabelsResponse response = null;
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          response = VisibilityClient.addLabels(conn, labels);
        } catch (Throwable e) {
          fail("Should not have thrown exception");
        }
        List<RegionActionResult> resultList = response.getResultList();
        assertEquals(5, resultList.size());
        assertTrue(resultList.get(0).getException().getValue().isEmpty());
        assertEquals("org.apache.hadoop.hbase.DoNotRetryIOException", resultList.get(1)
            .getException().getName());
        assertTrue(Bytes.toString(resultList.get(1).getException().getValue().toByteArray())
            .contains(
                "org.apache.hadoop.hbase.security.visibility.LabelAlreadyExistsException: "
                    + "Label 'secret' already exists"));
        assertTrue(resultList.get(2).getException().getValue().isEmpty());
        assertTrue(resultList.get(3).getException().getValue().isEmpty());
        assertTrue(resultList.get(4).getException().getValue().isEmpty());
        return null;
      }
    };
    SUPERUSER.runAs(action);
  }

  @Test(timeout = 60 * 1000)
  public void testAddVisibilityLabelsOnRSRestart() throws Exception {
    List<RegionServerThread> regionServerThreads = TEST_UTIL.getHBaseCluster()
        .getRegionServerThreads();
    for (RegionServerThread rsThread : regionServerThreads) {
      rsThread.getRegionServer().abort("Aborting ");
    }
    // Start one new RS
    RegionServerThread rs = TEST_UTIL.getHBaseCluster().startRegionServer();
    waitForLabelsRegionAvailability(rs.getRegionServer());
    final AtomicBoolean vcInitialized = new AtomicBoolean(true);
    do {
      PrivilegedExceptionAction<VisibilityLabelsResponse> action =
          new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
        public VisibilityLabelsResponse run() throws Exception {
          String[] labels = { SECRET, CONFIDENTIAL, PRIVATE, "ABC", "XYZ" };
          try (Connection conn = ConnectionFactory.createConnection(conf)) {
            VisibilityLabelsResponse resp = VisibilityClient.addLabels(conn, labels);
            List<RegionActionResult> results = resp.getResultList();
            if (results.get(0).hasException()) {
              NameBytesPair pair = results.get(0).getException();
              Throwable t = ProtobufUtil.toException(pair);
              LOG.debug("Got exception writing labels", t);
              if (t instanceof VisibilityControllerNotReadyException) {
                vcInitialized.set(false);
                LOG.warn("VisibilityController was not yet initialized");
                Threads.sleep(10);
              } else {
                vcInitialized.set(true);
              }
            } else LOG.debug("new labels added: " + resp);
          } catch (Throwable t) {
            throw new IOException(t);
          }
          return null;
        }
      };
      SUPERUSER.runAs(action);
    } while (!vcInitialized.get());
    // Scan the visibility label
    Scan s = new Scan();
    s.setAuthorizations(new Authorizations(VisibilityUtils.SYSTEM_LABEL));

    int i = 0;
    try (Table ht = TEST_UTIL.getConnection().getTable(LABELS_TABLE_NAME);
         ResultScanner scanner = ht.getScanner(s)) {
      while (true) {
        Result next = scanner.next();
        if (next == null) {
          break;
        }
        i++;
      }
    }
    // One label is the "system" label.
    Assert.assertEquals("The count should be 13", 13, i);
  }

  @Test
  public void testListLabels() throws Throwable {
    PrivilegedExceptionAction<ListLabelsResponse> action =
        new PrivilegedExceptionAction<ListLabelsResponse>() {
      public ListLabelsResponse run() throws Exception {
        ListLabelsResponse response = null;
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          response = VisibilityClient.listLabels(conn, null);
        } catch (Throwable e) {
          fail("Should not have thrown exception");
        }
        // The addLabels() in setup added:
        // { SECRET, TOPSECRET, CONFIDENTIAL, PUBLIC, PRIVATE, COPYRIGHT, ACCENT,
        //  UNICODE_VIS_TAG, UC1, UC2 };
        // The previous tests added 2 more labels: ABC, XYZ
        // The 'system' label is excluded.
        List<ByteString> labels = response.getLabelList();
        assertEquals(12, labels.size());
        assertTrue(labels.contains(ByteString.copyFrom(SECRET.getBytes())));
        assertTrue(labels.contains(ByteString.copyFrom(TOPSECRET.getBytes())));
        assertTrue(labels.contains(ByteString.copyFrom(CONFIDENTIAL.getBytes())));
        assertTrue(labels.contains(ByteString.copyFrom("ABC".getBytes())));
        assertTrue(labels.contains(ByteString.copyFrom("XYZ".getBytes())));
        assertFalse(labels.contains(ByteString.copyFrom(SYSTEM_LABEL.getBytes())));
        return null;
      }
    };
    SUPERUSER.runAs(action);
  }

  @Test
  public void testListLabelsWithRegEx() throws Throwable {
    PrivilegedExceptionAction<ListLabelsResponse> action =
        new PrivilegedExceptionAction<ListLabelsResponse>() {
      public ListLabelsResponse run() throws Exception {
        ListLabelsResponse response = null;
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          response = VisibilityClient.listLabels(conn, ".*secret");
        } catch (Throwable e) {
          fail("Should not have thrown exception");
        }
        // Only return the labels that end with 'secret'
        List<ByteString> labels = response.getLabelList();
        assertEquals(2, labels.size());
        assertTrue(labels.contains(ByteString.copyFrom(SECRET.getBytes())));
        assertTrue(labels.contains(ByteString.copyFrom(TOPSECRET.getBytes())));
        return null;
      }
    };
    SUPERUSER.runAs(action);
  }

  @Test(timeout = 60 * 1000)
  public void testVisibilityLabelsOnWALReplay() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try (Table table = createTableAndWriteDataWithLabels(tableName,
        "(" + SECRET + "|" + CONFIDENTIAL + ")", PRIVATE);) {
      List<RegionServerThread> regionServerThreads = TEST_UTIL.getHBaseCluster()
          .getRegionServerThreads();
      for (RegionServerThread rsThread : regionServerThreads) {
        rsThread.getRegionServer().abort("Aborting ");
      }
      // Start one new RS
      RegionServerThread rs = TEST_UTIL.getHBaseCluster().startRegionServer();
      waitForLabelsRegionAvailability(rs.getRegionServer());
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations(SECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 1);
    }
  }
}
