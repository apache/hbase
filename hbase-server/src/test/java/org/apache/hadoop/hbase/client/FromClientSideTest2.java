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
package org.apache.hadoop.hbase.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

public class FromClientSideTest2 extends FromClientSideTestBase {

  protected FromClientSideTest2(Class<? extends ConnectionRegistry> registryImpl,
    int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  private static final Logger LOG = LoggerFactory.getLogger(FromClientSideTest2.class);

  private static int WAITTABLE_MILLIS;
  private static byte[] ANOTHERROW;
  private static byte[] COL_QUAL;
  private static byte[] VAL_BYTES;
  private static byte[] ROW_BYTES;

  protected static void startCluster(Class<?>... cps) throws Exception {
    WAITTABLE_MILLIS = 10000;
    SLAVES = 3;
    ANOTHERROW = Bytes.toBytes("anotherrow");
    COL_QUAL = Bytes.toBytes("f1");
    VAL_BYTES = Bytes.toBytes("v1");
    ROW_BYTES = Bytes.toBytes("r1");
    initialize(cps);
  }

  @TestTemplate
  public void testHTableBatchWithEmptyPut() throws IOException, InterruptedException {
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY });
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      List<Put> actions = new ArrayList<>();
      Object[] results = new Object[2];
      // create an empty Put
      Put put1 = new Put(ROW);
      actions.add(put1);

      Put put2 = new Put(ANOTHERROW);
      put2.addColumn(FAMILY, QUALIFIER, VALUE);
      actions.add(put2);

      table.batch(actions, results);
      fail("Empty Put should have failed the batch call");
    } catch (IllegalArgumentException iae) {
    }
  }

  // Test Table.batch with large amount of mutations against the same key.
  // It used to trigger read lock's "Maximum lock count exceeded" Error.
  @TestTemplate
  public void testHTableWithLargeBatch() throws IOException, InterruptedException {
    int sixtyFourK = 64 * 1024;
    List actions = new ArrayList();
    Object[] results = new Object[(sixtyFourK + 1) * 2];

    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY });
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      for (int i = 0; i < sixtyFourK + 1; i++) {
        Put put1 = new Put(ROW);
        put1.addColumn(FAMILY, QUALIFIER, VALUE);
        actions.add(put1);

        Put put2 = new Put(ANOTHERROW);
        put2.addColumn(FAMILY, QUALIFIER, VALUE);
        actions.add(put2);
      }

      table.batch(actions, results);
    }
  }

  @TestTemplate
  public void testBatchWithRowMutation() throws Exception {
    LOG.info("Starting testBatchWithRowMutation");
    byte[][] QUALIFIERS = new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b") };
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY });
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {

      RowMutations arm = RowMutations
        .of(Collections.singletonList(new Put(ROW).addColumn(FAMILY, QUALIFIERS[0], VALUE)));
      Object[] batchResult = new Object[1];
      table.batch(Arrays.asList(arm), batchResult);

      Get g = new Get(ROW);
      Result r = table.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[0])));

      arm = RowMutations.of(Arrays.asList(new Put(ROW).addColumn(FAMILY, QUALIFIERS[1], VALUE),
        new Delete(ROW).addColumns(FAMILY, QUALIFIERS[0])));
      table.batch(Arrays.asList(arm), batchResult);
      r = table.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[1])));
      assertNull(r.getValue(FAMILY, QUALIFIERS[0]));

      // Test that we get the correct remote exception for RowMutations from batch()
      RetriesExhaustedException e = assertThrows(RetriesExhaustedException.class, () -> {
        RowMutations m = RowMutations.of(Collections.singletonList(
          new Put(ROW).addColumn(new byte[] { 'b', 'o', 'g', 'u', 's' }, QUALIFIERS[0], VALUE)));
        table.batch(Arrays.asList(m), batchResult);
      }, "Expected RetriesExhaustedWithDetailsException with NoSuchColumnFamilyException");
      assertThat(e.getMessage(), containsString("NoSuchColumnFamilyException"));
    }

  }

  @TestTemplate
  public void testBatchWithCheckAndMutate() throws Exception {
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY });
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      byte[] row1 = Bytes.toBytes("row1");
      byte[] row2 = Bytes.toBytes("row2");
      byte[] row3 = Bytes.toBytes("row3");
      byte[] row4 = Bytes.toBytes("row4");
      byte[] row5 = Bytes.toBytes("row5");
      byte[] row6 = Bytes.toBytes("row6");
      byte[] row7 = Bytes.toBytes("row7");

      table
        .put(Arrays.asList(new Put(row1).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a")),
          new Put(row2).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")),
          new Put(row3).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")),
          new Put(row4).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")),
          new Put(row5).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e")),
          new Put(row6).addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes(10L)),
          new Put(row7).addColumn(FAMILY, Bytes.toBytes("G"), Bytes.toBytes("g"))));

      CheckAndMutate checkAndMutate1 =
        CheckAndMutate.newBuilder(row1).ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
          .build(new RowMutations(row1)
            .add((Mutation) new Put(row1).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("g")))
            .add((Mutation) new Delete(row1).addColumns(FAMILY, Bytes.toBytes("A")))
            .add(new Increment(row1).addColumn(FAMILY, Bytes.toBytes("C"), 3L))
            .add(new Append(row1).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d"))));
      Get get = new Get(row2).addColumn(FAMILY, Bytes.toBytes("B"));
      RowMutations mutations = new RowMutations(row3)
        .add((Mutation) new Delete(row3).addColumns(FAMILY, Bytes.toBytes("C")))
        .add((Mutation) new Put(row3).addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("f")))
        .add(new Increment(row3).addColumn(FAMILY, Bytes.toBytes("A"), 5L))
        .add(new Append(row3).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")));
      CheckAndMutate checkAndMutate2 =
        CheckAndMutate.newBuilder(row4).ifEquals(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("a"))
          .build(new Put(row4).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("h")));
      Put put = new Put(row5).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("f"));
      CheckAndMutate checkAndMutate3 =
        CheckAndMutate.newBuilder(row6).ifEquals(FAMILY, Bytes.toBytes("F"), Bytes.toBytes(10L))
          .build(new Increment(row6).addColumn(FAMILY, Bytes.toBytes("F"), 1));
      CheckAndMutate checkAndMutate4 =
        CheckAndMutate.newBuilder(row7).ifEquals(FAMILY, Bytes.toBytes("G"), Bytes.toBytes("g"))
          .build(new Append(row7).addColumn(FAMILY, Bytes.toBytes("G"), Bytes.toBytes("g")));

      List<Row> actions = Arrays.asList(checkAndMutate1, get, mutations, checkAndMutate2, put,
        checkAndMutate3, checkAndMutate4);
      Object[] results = new Object[actions.size()];
      table.batch(actions, results);

      CheckAndMutateResult checkAndMutateResult = (CheckAndMutateResult) results[0];
      assertTrue(checkAndMutateResult.isSuccess());
      assertEquals(3L,
        Bytes.toLong(checkAndMutateResult.getResult().getValue(FAMILY, Bytes.toBytes("C"))));
      assertEquals("d",
        Bytes.toString(checkAndMutateResult.getResult().getValue(FAMILY, Bytes.toBytes("D"))));

      assertEquals("b", Bytes.toString(((Result) results[1]).getValue(FAMILY, Bytes.toBytes("B"))));

      Result result = (Result) results[2];
      assertTrue(result.getExists());
      assertEquals(5L, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("A"))));
      assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

      checkAndMutateResult = (CheckAndMutateResult) results[3];
      assertFalse(checkAndMutateResult.isSuccess());
      assertNull(checkAndMutateResult.getResult());

      assertTrue(((Result) results[4]).isEmpty());

      checkAndMutateResult = (CheckAndMutateResult) results[5];
      assertTrue(checkAndMutateResult.isSuccess());
      assertEquals(11,
        Bytes.toLong(checkAndMutateResult.getResult().getValue(FAMILY, Bytes.toBytes("F"))));

      checkAndMutateResult = (CheckAndMutateResult) results[6];
      assertTrue(checkAndMutateResult.isSuccess());
      assertEquals("gg",
        Bytes.toString(checkAndMutateResult.getResult().getValue(FAMILY, Bytes.toBytes("G"))));

      result = table.get(new Get(row1));
      assertEquals("g", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));
      assertNull(result.getValue(FAMILY, Bytes.toBytes("A")));
      assertEquals(3L, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("C"))));
      assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

      result = table.get(new Get(row3));
      assertNull(result.getValue(FAMILY, Bytes.toBytes("C")));
      assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("F"))));
      assertNull(Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))));
      assertEquals(5L, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("A"))));
      assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

      result = table.get(new Get(row4));
      assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

      result = table.get(new Get(row5));
      assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("E"))));

      result = table.get(new Get(row6));
      assertEquals(11, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("F"))));

      result = table.get(new Get(row7));
      assertEquals("gg", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("G"))));
    }
  }

  @TestTemplate
  public void testHTableExistsMethodSingleRegionSingleGet()
    throws IOException, InterruptedException {
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY });
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      // Test with a single region table.
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);

      Get get = new Get(ROW);

      boolean exist = table.exists(get);
      assertFalse(exist);

      table.put(put);

      exist = table.exists(get);
      assertTrue(exist);
    }
  }

  @TestTemplate
  public void testHTableExistsMethodSingleRegionMultipleGets()
    throws IOException, InterruptedException {
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY });
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      List<Get> gets = new ArrayList<>();
      gets.add(new Get(ROW));
      gets.add(new Get(ANOTHERROW));

      boolean[] results = table.exists(gets);
      assertTrue(results[0]);
      assertFalse(results[1]);
    }
  }

  @TestTemplate
  public void testHTableExistsBeforeGet() throws IOException, InterruptedException {
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY });
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      Get get = new Get(ROW);

      boolean exist = table.exists(get);
      assertEquals(true, exist);

      Result result = table.get(get);
      assertEquals(false, result.isEmpty());
      assertTrue(Bytes.equals(VALUE, result.getValue(FAMILY, QUALIFIER)));
    }
  }

  @TestTemplate
  public void testHTableExistsAllBeforeGet() throws IOException, InterruptedException {
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY });
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      final byte[] ROW2 = Bytes.add(ROW, Bytes.toBytes("2"));
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);
      put = new Put(ROW2);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      Get get = new Get(ROW);
      Get get2 = new Get(ROW2);
      ArrayList<Get> getList = new ArrayList(2);
      getList.add(get);
      getList.add(get2);

      boolean[] exists = table.existsAll(getList);
      assertEquals(true, exists[0]);
      assertEquals(true, exists[1]);

      Result[] result = table.get(getList);
      assertEquals(false, result[0].isEmpty());
      assertTrue(Bytes.equals(VALUE, result[0].getValue(FAMILY, QUALIFIER)));
      assertEquals(false, result[1].isEmpty());
      assertTrue(Bytes.equals(VALUE, result[1].getValue(FAMILY, QUALIFIER)));
    }
  }

  @TestTemplate
  public void testGetEmptyRow() throws Exception {
    // Create a table and put in 1 row
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY });
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Put put = new Put(ROW_BYTES);
      put.addColumn(FAMILY, COL_QUAL, VAL_BYTES);
      table.put(put);

      // Try getting the row with an empty row key
      Result res = null;
      try {
        res = table.get(new Get(new byte[0]));
        fail();
      } catch (IllegalArgumentException e) {
        // Expected.
      }
      assertTrue(res == null);
      res = table.get(new Get(Bytes.toBytes("r1-not-exist")));
      assertTrue(res.isEmpty() == true);
      res = table.get(new Get(ROW_BYTES));
      assertTrue(Arrays.equals(res.getValue(FAMILY, COL_QUAL), VAL_BYTES));
    }
  }

  @TestTemplate
  public void testConnectionDefaultUsesCodec() throws Exception {
    ClusterConnection con = (ClusterConnection) TEST_UTIL.getConnection();
    assertTrue(con.hasCellBlockSupport());
  }

  private void randomCFPuts(Table table, byte[] row, byte[] family, int nPuts) throws Exception {
    Put put = new Put(row);
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < nPuts; i++) {
      byte[] qualifier = Bytes.toBytes(rand.nextInt());
      byte[] value = Bytes.toBytes(rand.nextInt());
      put.addColumn(family, qualifier, value);
    }
    table.put(put);
  }

  private void performMultiplePutAndFlush(HBaseAdmin admin, Table table, byte[] row, byte[] family,
    int nFlushes, int nPuts) throws Exception {
    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(table.getName())) {
      // connection needed for poll-wait
      HRegionLocation loc = locator.getRegionLocation(row, true);
      AdminProtos.AdminService.BlockingInterface server =
        ((ClusterConnection) admin.getConnection()).getAdmin(loc.getServerName());
      byte[] regName = loc.getRegionInfo().getRegionName();

      for (int i = 0; i < nFlushes; i++) {
        randomCFPuts(table, row, family, nPuts);
        List<String> sf = ProtobufUtil.getStoreFiles(server, regName, FAMILY);
        int sfCount = sf.size();

        admin.flush(table.getName());
      }
    }
  }

  private int getStoreFileCount(Admin admin, ServerName serverName, RegionInfo region)
    throws IOException {
    for (RegionMetrics metrics : admin.getRegionMetrics(serverName, region.getTable())) {
      if (Bytes.equals(region.getRegionName(), metrics.getRegionName())) {
        return metrics.getStoreFileCount();
      }
    }
    return 0;
  }

  // override the config settings at the CF level and ensure priority
  @TestTemplate
  public void testAdvancedConfigOverride() throws Exception {
    /*
     * Overall idea: (1) create 3 store files and issue a compaction. config's compaction.min == 3,
     * so should work. (2) Increase the compaction.min toggle in the HTD to 5 and modify table. If
     * we use the HTD value instead of the default config value, adding 3 files and issuing a
     * compaction SHOULD NOT work (3) Decrease the compaction.min toggle in the HCD to 2 and modify
     * table. The CF schema should override the Table schema and now cause a minor compaction.
     */
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compaction.min", 3);
    TEST_UTIL.createTable(tableName, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName);
      Admin admin = conn.getAdmin()) {
      ClusterConnection connection = (ClusterConnection) conn;
      // Create 3 store files.
      byte[] row = Bytes.toBytes(ThreadLocalRandom.current().nextInt());
      performMultiplePutAndFlush((HBaseAdmin) admin, table, row, FAMILY, 3, 100);

      try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
        // Verify we have multiple store files.
        HRegionLocation loc = locator.getRegionLocation(row, true);
        byte[] regionName = loc.getRegionInfo().getRegionName();
        AdminProtos.AdminService.BlockingInterface server =
          connection.getAdmin(loc.getServerName());
        assertTrue(ProtobufUtil.getStoreFiles(server, regionName, FAMILY).size() > 1);

        // Issue a compaction request
        admin.compact(tableName);

        // poll wait for the compactions to happen
        for (int i = 0; i < 10 * 1000 / 40; ++i) {
          // The number of store files after compaction should be lesser.
          loc = locator.getRegionLocation(row, true);
          if (!loc.getRegionInfo().isOffline()) {
            regionName = loc.getRegionInfo().getRegionName();
            server = connection.getAdmin(loc.getServerName());
            if (ProtobufUtil.getStoreFiles(server, regionName, FAMILY).size() <= 1) {
              break;
            }
          }
          Thread.sleep(40);
        }
        // verify the compactions took place and that we didn't just time out
        assertTrue(ProtobufUtil.getStoreFiles(server, regionName, FAMILY).size() <= 1);

        // change the compaction.min config option for this table to 5
        LOG.info("hbase.hstore.compaction.min should now be 5");
        HTableDescriptor htd = new HTableDescriptor(table.getTableDescriptor());
        htd.setValue("hbase.hstore.compaction.min", String.valueOf(5));
        admin.modifyTable(tableName, htd);
        Pair<Integer, Integer> st = admin.getAlterStatus(tableName);
        while (null != st && st.getFirst() > 0) {
          LOG.debug(st.getFirst() + " regions left to update");
          Thread.sleep(40);
          st = admin.getAlterStatus(tableName);
        }
        LOG.info("alter status finished");

        // Create 3 more store files.
        performMultiplePutAndFlush((HBaseAdmin) admin, table, row, FAMILY, 3, 10);

        // Issue a compaction request
        admin.compact(tableName);

        // This time, the compaction request should not happen
        Thread.sleep(10 * 1000);
        loc = locator.getRegionLocation(row, true);
        regionName = loc.getRegionInfo().getRegionName();
        server = connection.getAdmin(loc.getServerName());
        int sfCount = ProtobufUtil.getStoreFiles(server, regionName, FAMILY).size();
        assertTrue(sfCount > 1);

        // change an individual CF's config option to 2 & online schema update
        LOG.info("hbase.hstore.compaction.min should now be 2");
        HColumnDescriptor hcd = new HColumnDescriptor(htd.getFamily(FAMILY));
        hcd.setValue("hbase.hstore.compaction.min", String.valueOf(2));
        htd.modifyFamily(hcd);
        admin.modifyTable(tableName, htd);
        st = admin.getAlterStatus(tableName);
        while (null != st && st.getFirst() > 0) {
          LOG.debug(st.getFirst() + " regions left to update");
          Thread.sleep(40);
          st = admin.getAlterStatus(tableName);
        }
        LOG.info("alter status finished");

        // Issue a compaction request
        admin.compact(tableName);

        // poll wait for the compactions to happen
        for (int i = 0; i < 10 * 1000 / 40; ++i) {
          loc = locator.getRegionLocation(row, true);
          regionName = loc.getRegionInfo().getRegionName();
          try {
            server = connection.getAdmin(loc.getServerName());
            if (ProtobufUtil.getStoreFiles(server, regionName, FAMILY).size() < sfCount) {
              break;
            }
          } catch (Exception e) {
            LOG.debug("Waiting for region to come online: " + Bytes.toString(regionName));
          }
          Thread.sleep(40);
        }

        // verify the compaction took place and that we didn't just time out
        assertTrue(ProtobufUtil.getStoreFiles(server, regionName, FAMILY).size() < sfCount);

        // Finally, ensure that we can remove a custom config value after we made it
        LOG.info("Removing CF config value");
        LOG.info("hbase.hstore.compaction.min should now be 5");
        hcd = new HColumnDescriptor(htd.getFamily(FAMILY));
        hcd.setValue("hbase.hstore.compaction.min", null);
        htd.modifyFamily(hcd);
        admin.modifyTable(tableName, htd);
        st = admin.getAlterStatus(tableName);
        while (null != st && st.getFirst() > 0) {
          LOG.debug(st.getFirst() + " regions left to update");
          Thread.sleep(40);
          st = admin.getAlterStatus(tableName);
        }
        LOG.info("alter status finished");
        assertNull(
          table.getTableDescriptor().getFamily(FAMILY).getValue("hbase.hstore.compaction.min"));
      }
    }
  }
}
