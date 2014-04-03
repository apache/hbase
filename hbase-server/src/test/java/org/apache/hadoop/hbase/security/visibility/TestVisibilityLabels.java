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

import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_FAMILY;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_NAME;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABEL_QUALIFIER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.google.protobuf.ByteString;

/**
 * Test class that tests the visibility labels
 */
@Category(MediumTests.class)
public class TestVisibilityLabels {

  private static final String TOPSECRET = "topsecret";
  private static final String PUBLIC = "public";
  private static final String PRIVATE = "private";
  private static final String CONFIDENTIAL = "confidential";
  private static final String SECRET = "secret";
  public static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] row1 = Bytes.toBytes("row1");
  private static final byte[] row2 = Bytes.toBytes("row2");
  private static final byte[] row3 = Bytes.toBytes("row3");
  private static final byte[] row4 = Bytes.toBytes("row4");
  private final static byte[] fam = Bytes.toBytes("info");
  private final static byte[] qual = Bytes.toBytes("qual");
  private final static byte[] value = Bytes.toBytes("value");
  public static Configuration conf;

  private volatile boolean killedRS = false;
  @Rule 
  public final TestName TEST_NAME = new TestName();
  public static User SUPERUSER;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false);
    conf.setBoolean("hbase.online.schema.update.enable", true);
    conf.setInt("hfile.format.version", 3);
    conf.set("hbase.coprocessor.master.classes", VisibilityController.class.getName());
    conf.set("hbase.coprocessor.region.classes", VisibilityController.class.getName());
    conf.setClass(VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS, SimpleScanLabelGenerator.class,
        ScanLabelGenerator.class);
    conf.set("hbase.superuser", "admin");
    TEST_UTIL.startMiniCluster(2);
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });

    // Wait for the labels table to become available
    TEST_UTIL.waitTableEnabled(LABELS_TABLE_NAME.getName(), 50000);
    addLabels();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    killedRS = false;
  }

  @Test
  public void testSimpleVisibilityLabels() throws Exception {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    HTable table = createTableAndWriteDataWithLabels(tableName, SECRET + "|" + CONFIDENTIAL,
        PRIVATE + "|" + CONFIDENTIAL);
    try {
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL, PRIVATE));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);

      assertTrue(next.length == 2);
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
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testVisibilityLabelsWithComplexLabels() throws Exception {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    HTable table = createTableAndWriteDataWithLabels(tableName, "(" + SECRET + "|" + CONFIDENTIAL
        + ")" + "&" + "!" + TOPSECRET, "(" + PRIVATE + "&" + CONFIDENTIAL + "&" + SECRET + ")", "("
        + PRIVATE + "&" + CONFIDENTIAL + "&" + SECRET + ")", "(" + PRIVATE + "&" + CONFIDENTIAL
        + "&" + SECRET + ")");
    try {
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations(TOPSECRET, CONFIDENTIAL, PRIVATE, PUBLIC, SECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(4);
      assertEquals(3, next.length);
      CellScanner cellScanner = next[0].cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row2, 0, row2.length));
      cellScanner = next[1].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row3, 0, row3.length));
      cellScanner = next[2].cellScanner();
      cellScanner.advance();
      current = cellScanner.current();
      assertTrue(Bytes.equals(current.getRowArray(), current.getRowOffset(),
          current.getRowLength(), row4, 0, row4.length));
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testVisibilityLabelsThatDoesNotPassTheCriteria() throws Exception {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    HTable table = createTableAndWriteDataWithLabels(tableName, "(" + SECRET + "|" + CONFIDENTIAL
        + ")", PRIVATE);
    try {
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations(PUBLIC));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 0);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testVisibilityLabelsInPutsThatDoesNotMatchAnyDefinedLabels() throws Exception {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    try {
      createTableAndWriteDataWithLabels(tableName, "SAMPLE_LABEL", "TEST");
      fail("Should have failed with failed sanity check exception");
    } catch (Exception e) {
    }
  }

  @Test
  public void testVisibilityLabelsInScanThatDoesNotMatchAnyDefinedLabels() throws Exception {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    HTable table = createTableAndWriteDataWithLabels(tableName, "(" + SECRET + "|" + CONFIDENTIAL
        + ")", PRIVATE);
    try {
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations("SAMPLE"));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 0);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testVisibilityLabelsWithGet() throws Exception {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    HTable table = createTableAndWriteDataWithLabels(tableName, SECRET + "&" + CONFIDENTIAL + "&!"
        + PRIVATE, SECRET + "&" + CONFIDENTIAL + "&" + PRIVATE);
    try {
      Get get = new Get(row1);
      get.setAuthorizations(new Authorizations(SECRET, CONFIDENTIAL));
      Result result = table.get(get);
      assertTrue(!result.isEmpty());
      Cell cell = result.getColumnLatestCell(fam, qual);
      assertTrue(Bytes.equals(value, 0, value.length, cell.getValueArray(), cell.getValueOffset(),
          cell.getValueLength()));
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testVisibilityLabelsOnKillingOfRSContainingLabelsTable() throws Exception {
    List<RegionServerThread> regionServerThreads = TEST_UTIL.getHBaseCluster()
        .getRegionServerThreads();
    int liveRS = 0;
    for (RegionServerThread rsThreads : regionServerThreads) {
      if (!rsThreads.getRegionServer().isAborted()) {
        liveRS++;
      }
    }
    if (liveRS == 1) {
      TEST_UTIL.getHBaseCluster().startRegionServer();
    }
    Thread t1 = new Thread() {
      public void run() {
        List<RegionServerThread> regionServerThreads = TEST_UTIL.getHBaseCluster()
            .getRegionServerThreads();
        for (RegionServerThread rsThread : regionServerThreads) {
          List<HRegion> onlineRegions = rsThread.getRegionServer().getOnlineRegions(
              LABELS_TABLE_NAME);
          if (onlineRegions.size() > 0) {
            rsThread.getRegionServer().abort("Aborting ");
            killedRS = true;
            break;
          }
        }
      }

    };
    t1.start();
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    Thread t = new Thread() {
      public void run() {
        try {
          while (!killedRS) {
            Thread.sleep(1);
          }
          createTableAndWriteDataWithLabels(tableName, "(" + SECRET + "|" + CONFIDENTIAL + ")",
              PRIVATE);
        } catch (Exception e) {
        }
      }
    };
    t.start();
    regionServerThreads = TEST_UTIL.getHBaseCluster().getRegionServerThreads();
    while (!killedRS) {
      Thread.sleep(10);
    }
    regionServerThreads = TEST_UTIL.getHBaseCluster().getRegionServerThreads();
    for (RegionServerThread rsThread : regionServerThreads) {
      while (true) {
        if (!rsThread.getRegionServer().isAborted()) {
          List<HRegion> onlineRegions = rsThread.getRegionServer().getOnlineRegions(
              LABELS_TABLE_NAME);
          if (onlineRegions.size() > 0) {
            break;
          } else {
            Thread.sleep(10);
          }
        } else {
          break;
        }
      }
    }
    TEST_UTIL.waitTableEnabled(LABELS_TABLE_NAME.getName(), 50000);
    t.join();
    HTable table = null;
    try {
      table = new HTable(TEST_UTIL.getConfiguration(), tableName);
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations(SECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 1);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test(timeout = 60 * 1000)
  public void testVisibilityLabelsOnRSRestart() throws Exception {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    HTable table = createTableAndWriteDataWithLabels(tableName, "(" + SECRET + "|" + CONFIDENTIAL
        + ")", PRIVATE);
    List<RegionServerThread> regionServerThreads = TEST_UTIL.getHBaseCluster()
        .getRegionServerThreads();
    for (RegionServerThread rsThread : regionServerThreads) {
      rsThread.getRegionServer().abort("Aborting ");
    }
    // Start one new RS
    RegionServerThread rs = TEST_UTIL.getHBaseCluster().startRegionServer();
    waitForLabelsRegionAvailability(rs.getRegionServer());
    try {
      Scan s = new Scan();
      s.setAuthorizations(new Authorizations(SECRET));
      ResultScanner scanner = table.getScanner(s);
      Result[] next = scanner.next(3);
      assertTrue(next.length == 1);
    } finally {
      if (table != null) {
        table.close();
      }
    }
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
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        String[] labels = { SECRET, CONFIDENTIAL, PRIVATE, "ABC", "XYZ" };
        try {
          VisibilityClient.addLabels(conf, labels);
        } catch (Throwable t) {
          throw new IOException(t);
        }
        return null;
      }
    };
    SUPERUSER.runAs(action);
    // Scan the visibility label
    Scan s = new Scan();
    s.setAuthorizations(new Authorizations(VisibilityUtils.SYSTEM_LABEL));
    HTable ht = new HTable(conf, LABELS_TABLE_NAME.getName());
    int i = 0;
    try {
      ResultScanner scanner = ht.getScanner(s);
      while (true) {
        Result next = scanner.next();
        if (next == null) {
          break;
        }
        i++;
      }
    } finally {
      if (ht != null) {
        ht.close();
      }
    }
    // One label is the "system" label.
    Assert.assertEquals("The count should be 8", 8, i);
  }

  private void waitForLabelsRegionAvailability(HRegionServer regionServer) {
    while (!regionServer.isOnline()) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
      }
    }
    while (regionServer.getOnlineRegions(LABELS_TABLE_NAME).isEmpty()) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
      }
    }
    HRegion labelsTableRegion = regionServer.getOnlineRegions(LABELS_TABLE_NAME).get(0);
    while (labelsTableRegion.isRecovering()) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
      }
    }
  }

  @Test
  public void testVisibilityLabelsInGetThatDoesNotMatchAnyDefinedLabels() throws Exception {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    HTable table = createTableAndWriteDataWithLabels(tableName, "(" + SECRET + "|" + CONFIDENTIAL
        + ")", PRIVATE);
    try {
      Get get = new Get(row1);
      get.setAuthorizations(new Authorizations("SAMPLE"));
      Result result = table.get(get);
      assertTrue(result.isEmpty());
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testAddLabels() throws Throwable {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action = 
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        String[] labels = { "L1", SECRET, "L2", "invalid~", "L3" };
        VisibilityLabelsResponse response = null;
        try {
          response = VisibilityClient.addLabels(conf, labels);
        } catch (Throwable e) {
          fail("Should not have thrown exception");
        }
        List<RegionActionResult> resultList = response.getResultList();
        assertEquals(5, resultList.size());
        assertTrue(resultList.get(0).getException().getValue().isEmpty());
        assertEquals("org.apache.hadoop.hbase.security.visibility.LabelAlreadyExistsException",
            resultList.get(1).getException().getName());
        assertTrue(resultList.get(2).getException().getValue().isEmpty());
        assertEquals("org.apache.hadoop.hbase.security.visibility.InvalidLabelException",
            resultList.get(3).getException().getName());
        assertTrue(resultList.get(4).getException().getValue().isEmpty());
        return null;
      }
    };
    SUPERUSER.runAs(action);
  }

  @Test
  public void testSetAndGetUserAuths() throws Throwable {
    final String user = "user1";
    PrivilegedExceptionAction<Void> action = new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        String[] auths = { SECRET, CONFIDENTIAL };
        try {
          VisibilityClient.setAuths(conf, auths, user);
        } catch (Throwable e) {
        }
        return null;
      }
    };
    SUPERUSER.runAs(action);
    HTable ht = null;
    try {
      ht = new HTable(conf, LABELS_TABLE_NAME);
      Scan scan = new Scan();
      scan.setAuthorizations(new Authorizations(VisibilityUtils.SYSTEM_LABEL));
      ResultScanner scanner = ht.getScanner(scan);
      Result result = null;
      while ((result = scanner.next()) != null) {
        Cell label = result.getColumnLatestCell(LABELS_TABLE_FAMILY, LABEL_QUALIFIER);
        Cell userAuth = result.getColumnLatestCell(LABELS_TABLE_FAMILY, user.getBytes());
        if (Bytes.equals(SECRET.getBytes(), 0, SECRET.getBytes().length, label.getValueArray(),
            label.getValueOffset(), label.getValueLength())
            || Bytes.equals(CONFIDENTIAL.getBytes(), 0, CONFIDENTIAL.getBytes().length,
                label.getValueArray(), label.getValueOffset(), label.getValueLength())) {
          assertNotNull(userAuth);
        } else {
          assertNull(userAuth);
        }
      }
    } finally {
      if (ht != null) {
        ht.close();
      }
    }

    action = new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        GetAuthsResponse authsResponse = null;
        try {
          authsResponse = VisibilityClient.getAuths(conf, user);
        } catch (Throwable e) {
          fail("Should not have failed");
        }
        List<String> authsList = new ArrayList<String>();
        for (ByteString authBS : authsResponse.getAuthList()) {
          authsList.add(Bytes.toString(authBS.toByteArray()));
        }
        assertEquals(2, authsList.size());
        assertTrue(authsList.contains(SECRET));
        assertTrue(authsList.contains(CONFIDENTIAL));
        return null;
      }
    };
    SUPERUSER.runAs(action);

    // Try doing setAuths once again and there should not be any duplicates
    action = new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        String[] auths1 = { SECRET, CONFIDENTIAL };
        GetAuthsResponse authsResponse = null;
        try {
          VisibilityClient.setAuths(conf, auths1, user);
          try {
            authsResponse = VisibilityClient.getAuths(conf, user);
          } catch (Throwable e) {
            fail("Should not have failed");
          }
        } catch (Throwable e) {
        }
        List<String> authsList = new ArrayList<String>();
        for (ByteString authBS : authsResponse.getAuthList()) {
          authsList.add(Bytes.toString(authBS.toByteArray()));
        }
        assertEquals(2, authsList.size());
        assertTrue(authsList.contains(SECRET));
        assertTrue(authsList.contains(CONFIDENTIAL));
        return null;
      }
    };
    SUPERUSER.runAs(action);
  }

  @Test
  public void testClearUserAuths() throws Throwable {
    PrivilegedExceptionAction<Void> action = new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        String[] auths = { SECRET, CONFIDENTIAL, PRIVATE };
        String user = "testUser";
        try {
          VisibilityClient.setAuths(conf, auths, user);
        } catch (Throwable e) {
          fail("Should not have failed");
        }
        // Removing the auths for SECRET and CONFIDENTIAL for the user.
        // Passing a non existing auth also.
        auths = new String[] { SECRET, PUBLIC, CONFIDENTIAL };
        VisibilityLabelsResponse response = null;
        try {
          response = VisibilityClient.clearAuths(conf, auths, user);
        } catch (Throwable e) {
          fail("Should not have failed");
        }
        List<RegionActionResult> resultList = response.getResultList();
        assertEquals(3, resultList.size());
        assertTrue(resultList.get(0).getException().getValue().isEmpty());
        assertEquals("org.apache.hadoop.hbase.security.visibility.InvalidLabelException",
            resultList.get(1).getException().getName());
        assertTrue(resultList.get(2).getException().getValue().isEmpty());
        HTable ht = null;
        try {
          ht = new HTable(conf, LABELS_TABLE_NAME);
          ResultScanner scanner = ht.getScanner(new Scan());
          Result result = null;
          while ((result = scanner.next()) != null) {
            Cell label = result.getColumnLatestCell(LABELS_TABLE_FAMILY, LABEL_QUALIFIER);
            Cell userAuth = result.getColumnLatestCell(LABELS_TABLE_FAMILY, user.getBytes());
            if (Bytes.equals(PRIVATE.getBytes(), 0, PRIVATE.getBytes().length,
                label.getValueArray(), label.getValueOffset(), label.getValueLength())) {
              assertNotNull(userAuth);
            } else {
              assertNull(userAuth);
            }
          }
        } finally {
          if (ht != null) {
            ht.close();
          }
        }

        GetAuthsResponse authsResponse = null;
        try {
          authsResponse = VisibilityClient.getAuths(conf, user);
        } catch (Throwable e) {
          fail("Should not have failed");
        }
        List<String> authsList = new ArrayList<String>();
        for (ByteString authBS : authsResponse.getAuthList()) {
          authsList.add(Bytes.toString(authBS.toByteArray()));
        }
        assertEquals(1, authsList.size());
        assertTrue(authsList.contains(PRIVATE));
        return null;
      }
    };
    SUPERUSER.runAs(action);
  }

  @Test
  public void testLabelsWithCheckAndPut() throws Throwable {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    HTable table = null;
    try {
      table = TEST_UTIL.createTable(tableName, fam);
      byte[] row1 = Bytes.toBytes("row1");
      Put put = new Put(row1);
      put.add(fam, qual, HConstants.LATEST_TIMESTAMP, value);
      put.setCellVisibility(new CellVisibility(SECRET + " & " + CONFIDENTIAL));
      table.checkAndPut(row1, fam, qual, null, put);
      byte[] row2 = Bytes.toBytes("row2");
      put = new Put(row2);
      put.add(fam, qual, HConstants.LATEST_TIMESTAMP, value);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.checkAndPut(row2, fam, qual, null, put);
      
      Scan scan = new Scan();
      scan.setAuthorizations(new Authorizations(SECRET));
      ResultScanner scanner = table.getScanner(scan);
      Result result = scanner.next();
      assertTrue(!result.isEmpty());
      assertTrue(Bytes.equals(row2, result.getRow()));
      result = scanner.next();
      assertNull(result);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testLabelsWithIncrement() throws Throwable {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    HTable table = null;
    try {
      table = TEST_UTIL.createTable(tableName, fam);
      byte[] row1 = Bytes.toBytes("row1");
      byte[] val = Bytes.toBytes(1L);
      Put put = new Put(row1);
      put.add(fam, qual, HConstants.LATEST_TIMESTAMP, val);
      put.setCellVisibility(new CellVisibility(SECRET + " & " + CONFIDENTIAL));
      table.put(put);
      Get get = new Get(row1);
      get.setAuthorizations(new Authorizations(SECRET));
      Result result = table.get(get);
      assertTrue(result.isEmpty());
      table.incrementColumnValue(row1, fam, qual, 2L);
      result = table.get(get);
      assertTrue(result.isEmpty());
      Increment increment = new Increment(row1);
      increment.addColumn(fam, qual, 2L);
      increment.setCellVisibility(new CellVisibility(SECRET));
      table.increment(increment);
      result = table.get(get);
      assertTrue(!result.isEmpty());
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testLabelsWithAppend() throws Throwable {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    HTable table = null;
    try {
      table = TEST_UTIL.createTable(tableName, fam);
      byte[] row1 = Bytes.toBytes("row1");
      byte[] val = Bytes.toBytes("a");
      Put put = new Put(row1);
      put.add(fam, qual, HConstants.LATEST_TIMESTAMP, val);
      put.setCellVisibility(new CellVisibility(SECRET + " & " + CONFIDENTIAL));
      table.put(put);
      Get get = new Get(row1);
      get.setAuthorizations(new Authorizations(SECRET));
      Result result = table.get(get);
      assertTrue(result.isEmpty());
      Append append = new Append(row1);
      append.add(fam, qual, Bytes.toBytes("b"));
      table.append(append);
      result = table.get(get);
      assertTrue(result.isEmpty());
      append = new Append(row1);
      append.add(fam, qual, Bytes.toBytes("c"));
      append.setCellVisibility(new CellVisibility(SECRET));
      table.append(append);
      result = table.get(get);
      assertTrue(!result.isEmpty());
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testUserShouldNotDoDDLOpOnLabelsTable() throws Exception {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    try {
      admin.disableTable(LABELS_TABLE_NAME);
      fail("Lables table should not get disabled by user.");
    } catch (Exception e) {
    }
    try {
      admin.deleteTable(LABELS_TABLE_NAME);
      fail("Lables table should not get disabled by user.");
    } catch (Exception e) {
    }
    try {
      HColumnDescriptor hcd = new HColumnDescriptor("testFamily");
      admin.addColumn(LABELS_TABLE_NAME, hcd);
      fail("Lables table should not get altered by user.");
    } catch (Exception e) {
    }
    try {
      admin.deleteColumn(LABELS_TABLE_NAME, VisibilityConstants.LABELS_TABLE_FAMILY);
      fail("Lables table should not get altered by user.");
    } catch (Exception e) {
    }
    try {
      HColumnDescriptor hcd = new HColumnDescriptor(VisibilityConstants.LABELS_TABLE_FAMILY);
      hcd.setBloomFilterType(BloomType.ROWCOL);
      admin.modifyColumn(LABELS_TABLE_NAME, hcd);
      fail("Lables table should not get altered by user.");
    } catch (Exception e) {
    }
    try {
      HTableDescriptor htd = new HTableDescriptor(LABELS_TABLE_NAME);
      htd.addFamily(new HColumnDescriptor("f1"));
      htd.addFamily(new HColumnDescriptor("f2"));
      admin.modifyTable(LABELS_TABLE_NAME, htd);
      fail("Lables table should not get altered by user.");
    } catch (Exception e) {
    }
  }

  @Test
  public void testMultipleVersions() throws Exception {
    final byte[] r1 = Bytes.toBytes("row1");
    final byte[] r2 = Bytes.toBytes("row2");
    final byte[] v1 = Bytes.toBytes("100");
    final byte[] v2 = Bytes.toBytes("101");
    final byte[] fam2 = Bytes.toBytes("info2");
    final byte[] qual2 = Bytes.toBytes("qual2");
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor col = new HColumnDescriptor(fam);// Default max versions is 1.
    desc.addFamily(col);
    col = new HColumnDescriptor(fam2);
    col.setMaxVersions(5);
    desc.addFamily(col);
    TEST_UTIL.getHBaseAdmin().createTable(desc);
    HTable table = null;
    try {
      table = new HTable(TEST_UTIL.getConfiguration(), tableName);
      Put put = new Put(r1);
      put.add(fam, qual, 3l, v1);
      put.add(fam, qual2, 3l, v1);
      put.add(fam2, qual, 3l, v1);
      put.add(fam2, qual2, 3l, v1);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);
      put = new Put(r1);
      put.add(fam, qual, 4l, v2);
      put.add(fam, qual2, 4l, v2);
      put.add(fam2, qual, 4l, v2);
      put.add(fam2, qual2, 4l, v2);
      put.setCellVisibility(new CellVisibility(PRIVATE));
      table.put(put);

      put = new Put(r2);
      put.add(fam, qual, 3l, v1);
      put.add(fam, qual2, 3l, v1);
      put.add(fam2, qual, 3l, v1);
      put.add(fam2, qual2, 3l, v1);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);
      put = new Put(r2);
      put.add(fam, qual, 4l, v2);
      put.add(fam, qual2, 4l, v2);
      put.add(fam2, qual, 4l, v2);
      put.add(fam2, qual2, 4l, v2);
      put.setCellVisibility(new CellVisibility(SECRET));
      table.put(put);

      // TEST_UTIL.getHBaseAdmin().flush(tableName.getNameAsString());
      Scan s = new Scan();
      s.setMaxVersions(1);
      s.setAuthorizations(new Authorizations(SECRET));
      ResultScanner scanner = table.getScanner(s);
      Result result = scanner.next();
      assertTrue(Bytes.equals(r1, result.getRow()));
      // for cf 'fam' max versions in HCD is 1. So the old version cells, which are having matching
      // CellVisibility with Authorizations, should not get considered in the label evaluation at
      // all.
      assertNull(result.getColumnLatestCell(fam, qual));
      assertNull(result.getColumnLatestCell(fam, qual2));
      // for cf 'fam2' max versions in HCD is > 1. So we can consider the old version cells, which
      // are having matching CellVisibility with Authorizations, in the label evaluation. It can
      // just skip those recent versions for which visibility is not there as per the new version's
      // CellVisibility. The old versions which are having visibility can be send back
      Cell cell = result.getColumnLatestCell(fam2, qual);
      assertNotNull(cell);
      assertTrue(Bytes.equals(v1, 0, v1.length, cell.getValueArray(), cell.getValueOffset(),
          cell.getValueLength()));
      cell = result.getColumnLatestCell(fam2, qual2);
      assertNotNull(cell);
      assertTrue(Bytes.equals(v1, 0, v1.length, cell.getValueArray(), cell.getValueOffset(),
          cell.getValueLength()));

      result = scanner.next();
      assertTrue(Bytes.equals(r2, result.getRow()));
      cell = result.getColumnLatestCell(fam, qual);
      assertNotNull(cell);
      assertTrue(Bytes.equals(v2, 0, v2.length, cell.getValueArray(), cell.getValueOffset(),
          cell.getValueLength()));
      cell = result.getColumnLatestCell(fam, qual2);
      assertNotNull(cell);
      assertTrue(Bytes.equals(v2, 0, v2.length, cell.getValueArray(), cell.getValueOffset(),
          cell.getValueLength()));
      cell = result.getColumnLatestCell(fam2, qual);
      assertNotNull(cell);
      assertTrue(Bytes.equals(v2, 0, v2.length, cell.getValueArray(), cell.getValueOffset(),
          cell.getValueLength()));
      cell = result.getColumnLatestCell(fam2, qual2);
      assertNotNull(cell);
      assertTrue(Bytes.equals(v2, 0, v2.length, cell.getValueArray(), cell.getValueOffset(),
          cell.getValueLength()));
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  private static HTable createTableAndWriteDataWithLabels(TableName tableName, String... labelExps)
      throws Exception {
    HTable table = null;
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

  public static void addLabels() throws Exception {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        String[] labels = { SECRET, TOPSECRET, CONFIDENTIAL, PUBLIC, PRIVATE };
        try {
          VisibilityClient.addLabels(conf, labels);
        } catch (Throwable t) {
          throw new IOException(t);
        }
        return null;
      }
    };
    SUPERUSER.runAs(action);
  }
}
